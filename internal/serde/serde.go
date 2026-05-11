// Copyright 2025 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package serde provides a shared [Serde] client.
package serde

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"buf.build/gen/go/bufbuild/registry/connectrpc/gosimple/buf/registry/module/v1/modulev1connect"
	modulev1 "buf.build/gen/go/bufbuild/registry/protocolbuffers/go/buf/registry/module/v1"
	"connectrpc.com/connect"
	serde "github.com/bufbuild/bsr-kafka-serde-go"
	"github.com/sethvargo/go-retry"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

const negativeCacheExpiry = 1 * time.Minute

// New creates a new [Serde].
func New(host string, options ...serde.Option) *Serde {
	cfg := serde.Config{
		HTTPClient: http.DefaultClient,
	}
	for _, opt := range options {
		opt.ApplyToSerde(&cfg)
	}
	var clientOptions []connect.ClientOption
	if cfg.Token != "" {
		clientOptions = append(clientOptions, connect.WithInterceptors(
			newUnaryBearerTokenInterceptor(cfg.Token),
		))
	}
	baseURL := "https://" + host
	return &Serde{
		genSDKDeps:           collectGenSDKDeps(buildDeps()),
		cache:                map[cacheKey]cacheValue{},
		sfGroup:              &singleflight.Group{},
		typeCommits:          map[reflect.Type]string{},
		resolvedCommits:      map[string]string{},
		skipCommitResolution: cfg.SkipCommitResolution,
		commitResolver:       cfg.CommitResolver,
		fileDescriptorSetClient: modulev1connect.NewFileDescriptorSetServiceClient(
			cfg.HTTPClient,
			baseURL,
			clientOptions...,
		),
		commitClient: modulev1connect.NewCommitServiceClient(
			cfg.HTTPClient,
			baseURL,
			clientOptions...,
		),
		commitSFGroup: &singleflight.Group{},
	}
}

// Serde looks up FileDescriptorSets from a Buf Schema Registry, and caches lookups for
// [Serde.Deserialize] to use.
type Serde struct {
	// genSDKDeps may be overridden in tests within this package.
	genSDKDeps []genSDKDep

	mu    sync.RWMutex
	cache map[cacheKey]cacheValue

	sfGroup                 *singleflight.Group
	fileDescriptorSetClient modulev1connect.FileDescriptorSetServiceClient

	// typeCommitsMutex guards typeCommits.
	typeCommitsMutex sync.RWMutex
	// typeCommits caches the resolved commit ID per message type. A stored empty string means the
	// type is not from a BSR-generated SDK module (or has no pseudo-version short commit), and no
	// BSR call should be made. This avoids repeated reflection and dep-scanning on every Serialize.
	typeCommits map[reflect.Type]string

	resolvedCommitsMutex sync.RWMutex
	resolvedCommits      map[string]string // module path → full commit ID

	commitClient         modulev1connect.CommitServiceClient
	commitSFGroup        *singleflight.Group
	skipCommitResolution bool
	commitResolver       serde.CommitResolver
}

// Deserialize deserializes the value into a [proto.Message], based on the commit and messageFQN.
func (s *Serde) Deserialize(ctx context.Context, value []byte, commit, messageFQN string, now func() time.Time) (proto.Message, error) {
	// Validate that we have what we need.
	if commit == "" {
		return nil, fmt.Errorf("could not find %q header in record", serde.BufRegistryValueSchemaCommit)
	}
	if messageFQN == "" {
		return nil, fmt.Errorf("could not find %q header in record", serde.BufRegistryValueSchemaMessage)
	}
	messageFullName := protoreflect.FullName(messageFQN)
	if !messageFullName.IsValid() {
		return nil, fmt.Errorf("message name %q is not a valid protoreflect.FullName", messageFQN)
	}
	var messageDescriptor protoreflect.MessageDescriptor
	key := cacheKey{
		commit:     commit,
		messageFQN: messageFQN,
	}
	s.mu.RLock()
	cachedValue, ok := s.cache[key]
	s.mu.RUnlock()
	if ok && cachedValue.messageDescriptor != nil {
		messageDescriptor = cachedValue.messageDescriptor
	} else if ok {
		// stored an error; check to see if it should be expired.
		if now().After(cachedValue.created.Add(negativeCacheExpiry)) {
			// expire the cache entry and continue
			s.mu.Lock()
			delete(s.cache, key)
			s.mu.Unlock()
		} else {
			// return the error
			return nil, cachedValue.err
		}
	}
	if messageDescriptor == nil { //nolint: nestif // No need to restructure this.
		response, err, _ := s.sfGroup.Do(key.String(), func() (any, error) {
			return retry.DoValue(
				ctx,
				retry.WithJitter(100*time.Millisecond,
					retry.WithMaxDuration(
						30*time.Second,
						retry.NewExponential(time.Second),
					),
				),
				func(ctx context.Context) (*modulev1.GetFileDescriptorSetResponse, error) {
					response, err := s.fileDescriptorSetClient.GetFileDescriptorSet(ctx, &modulev1.GetFileDescriptorSetRequest{
						ResourceRef: &modulev1.ResourceRef{
							Value: &modulev1.ResourceRef_Id{
								Id: commit,
							},
						},
						IncludeTypes: []string{messageFQN},
					})
					if err != nil {
						switch code := connect.CodeOf(err); code { //nolint: exhaustive // We're covering the positive cases here; everything else is negative.
						case connect.CodeUnavailable, connect.CodeResourceExhausted, connect.CodeInternal:
							return nil, retry.RetryableError(err)
						default:
							return nil, err
						}
					}
					return response, nil
				},
			)
		})
		if err != nil {
			s.mu.Lock()
			s.cache[key] = cacheValue{
				err:     err,
				created: time.Now(),
			}
			s.mu.Unlock()
			return nil, err
		}
		getFileDescriptorSetResponse, ok := response.(*modulev1.GetFileDescriptorSetResponse)
		if !ok {
			return nil, fmt.Errorf("singleflight should have returned GetFileDescriptorSetResponse but got %T", response)
		}
		files, err := protodesc.NewFiles(getFileDescriptorSetResponse.GetFileDescriptorSet())
		if err != nil {
			return nil, fmt.Errorf("creating new protodesc files: %w", err)
		}
		descriptor, err := files.FindDescriptorByName(messageFullName)
		if err != nil {
			return nil, fmt.Errorf("finding descriptor by name %q: %w", messageFullName, err)
		}
		messageDescriptor, ok = descriptor.(protoreflect.MessageDescriptor)
		if !ok {
			return nil, fmt.Errorf("descriptor %T is not a message descriptor", descriptor)
		}
		s.mu.Lock()
		s.cache[key] = cacheValue{
			messageDescriptor: messageDescriptor,
		}
		s.mu.Unlock()
	}
	var msg proto.Message
	messageType, err := protoregistry.GlobalTypes.FindMessageByName(messageFullName)
	if err != nil {
		if !errors.Is(err, protoregistry.NotFound) {
			return nil, fmt.Errorf("looking up message %q by name in global types registry: %w", messageFullName, err)
		}
		// err is [protoregistry.NotFound]; use [dynamicpb.NewMessage] instead.
		msg = dynamicpb.NewMessage(messageDescriptor)
	} else {
		msg = messageType.New().Interface()
	}
	if err := proto.Unmarshal(value, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// GenSDKCommitFromMessage returns the full BSR commit ID for the given [proto.Message],
// either by calling a configured [serde.CommitResolver] or by resolving the short commit
// from the build info pseudo-version against the BSR API. Returns an empty string and no
// error if no commit can be determined for the message.
func (s *Serde) GenSDKCommitFromMessage(ctx context.Context, src proto.Message) (string, error) {
	if s.skipCommitResolution {
		return "", nil
	}
	msgType := reflect.TypeOf(src)
	if msgType.Kind() == reflect.Pointer {
		msgType = msgType.Elem()
	}
	s.typeCommitsMutex.RLock()
	cached, ok := s.typeCommits[msgType]
	s.typeCommitsMutex.RUnlock()
	if ok {
		return cached, nil
	}
	dep, hasDep := findGenSDKDep(msgType.PkgPath(), s.genSDKDeps)
	var moduleFullName string
	if hasDep {
		moduleFullName = dep.moduleFullName
	}
	if s.commitResolver != nil {
		// Errors are not cached so a transient resolver failure can be retried by the next call.
		commit, err := s.commitResolver(ctx, src, moduleFullName)
		if err != nil {
			return "", err
		}
		s.typeCommitsMutex.Lock()
		s.typeCommits[msgType] = commit
		s.typeCommitsMutex.Unlock()
		return commit, nil
	}
	if !hasDep {
		s.typeCommitsMutex.Lock()
		s.typeCommits[msgType] = ""
		s.typeCommitsMutex.Unlock()
		return "", nil
	}
	commit, err := s.resolveCommit(ctx, dep)
	if err != nil {
		return "", err
	}
	s.typeCommitsMutex.Lock()
	s.typeCommits[msgType] = commit
	s.typeCommitsMutex.Unlock()
	return commit, nil
}

func buildDeps() []*debug.Module {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return nil
	}
	return info.Deps
}

type cacheKey struct {
	commit     string
	messageFQN string
}

type cacheValue struct {
	messageDescriptor protoreflect.MessageDescriptor

	// any error that occurred during the fetching of the messageDescriptor.
	err error
	// when this cache value was created, for expiry of negative results.
	created time.Time
}

func (ck cacheKey) String() string {
	return ck.commit + "::" + ck.messageFQN
}

// newUnaryBearerTokenInterceptor returns a [connect.Interceptor] that adds an Authorization: Bearer
// header with the given token to unary requests. It does not add a header if token is empty.
func newUnaryBearerTokenInterceptor(token string) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
			if token != "" {
				request.Header().Set("Authorization", "Bearer "+token)
			}
			return next(ctx, request)
		}
	})
}

// resolveCommit resolves dep's short commit to the full BSR commit ID via ListCommits.
// Errors are not cached so a transient BSR failure can be retried by the next call.
func (s *Serde) resolveCommit(ctx context.Context, dep genSDKDep) (string, error) {
	s.resolvedCommitsMutex.RLock()
	commit, ok := s.resolvedCommits[dep.moduleFullName]
	s.resolvedCommitsMutex.RUnlock()
	if ok {
		return commit, nil
	}
	result, err, _ := s.commitSFGroup.Do(dep.moduleFullName, func() (any, error) {
		s.resolvedCommitsMutex.RLock()
		commit, ok := s.resolvedCommits[dep.moduleFullName]
		s.resolvedCommitsMutex.RUnlock()
		if ok {
			return commit, nil
		}
		response, err := retry.DoValue(
			ctx,
			retry.WithJitter(100*time.Millisecond,
				retry.WithMaxDuration(
					30*time.Second,
					retry.NewExponential(time.Second),
				),
			),
			func(ctx context.Context) (*modulev1.ListCommitsResponse, error) {
				response, err := s.commitClient.ListCommits(ctx, &modulev1.ListCommitsRequest{
					IdQuery: dep.shortCommit,
					ResourceRef: &modulev1.ResourceRef{
						Value: &modulev1.ResourceRef_Name_{
							Name: &modulev1.ResourceRef_Name{
								Owner:  dep.owner,
								Module: dep.module,
							},
						},
					},
				})
				if err != nil {
					switch connect.CodeOf(err) { //nolint: exhaustive // We're covering the positive cases here; everything else is negative.
					case connect.CodeUnavailable, connect.CodeResourceExhausted, connect.CodeInternal:
						return nil, retry.RetryableError(err)
					default:
						return nil, err
					}
				}
				return response, nil
			},
		)
		if err != nil {
			return "", fmt.Errorf("listing commits for %s with id query %q: %w", dep.moduleFullName, dep.shortCommit, err)
		}
		commits := response.GetCommits()
		var fullCommit string
		switch len(commits) {
		case 0:
			return "", fmt.Errorf("no commits found for %s with id query %q", dep.moduleFullName, dep.shortCommit)
		case 1:
			fullCommit = commits[0].GetId()
		default:
			ts, ok := parsePseudoVersionTimestamp(dep.version)
			if !ok {
				return "", fmt.Errorf("could not parse timestamp from pseudo-version %q", dep.version)
			}
			for _, commit := range commits {
				if commit.GetCreateTime().AsTime().Equal(ts) {
					fullCommit = commit.GetId()
					break
				}
			}
			if fullCommit == "" {
				return "", fmt.Errorf("no commit found for %s with id query %q matching timestamp from %q", dep.moduleFullName, dep.shortCommit, dep.version)
			}
		}
		s.resolvedCommitsMutex.Lock()
		s.resolvedCommits[dep.moduleFullName] = fullCommit
		s.resolvedCommitsMutex.Unlock()
		return fullCommit, nil
	})
	if err != nil {
		return "", err
	}
	fullCommit, _ := result.(string)
	return fullCommit, nil
}

// genSDKDep is a BSR-generated Go SDK module dependency with fields pre-extracted at construction
// so the hot path does no parsing.
type genSDKDep struct {
	pkgPathPrefix  string
	version        string
	moduleFullName string
	owner          string
	module         string
	shortCommit    string
}

func collectGenSDKDeps(modules []*debug.Module) []genSDKDep {
	var deps []genSDKDep
	for _, module := range modules {
		registry, owner, name, ok := parseGenSDKModulePath(module.Path)
		if !ok {
			continue
		}
		shortCommit := extractCommitFromPseudoVersion(module.Version)
		if shortCommit == "" {
			continue
		}
		deps = append(deps, genSDKDep{
			pkgPathPrefix:  module.Path,
			version:        module.Version,
			moduleFullName: registry + "/" + owner + "/" + name,
			owner:          owner,
			module:         name,
			shortCommit:    shortCommit,
		})
	}
	return deps
}

// findGenSDKDep returns the dep whose pkgPathPrefix is the longest prefix of pkgPath.
func findGenSDKDep(pkgPath string, deps []genSDKDep) (genSDKDep, bool) {
	var (
		best  genSDKDep
		found bool
	)
	for _, dep := range deps {
		if strings.HasPrefix(pkgPath, dep.pkgPathPrefix) {
			if !found || len(dep.pkgPathPrefix) > len(best.pkgPathPrefix) {
				best = dep
				found = true
			}
		}
	}
	return best, found
}

// parseGenSDKModulePath parses the registry, owner, and module name from a BSR-generated
// Go SDK module path. The path format is:
// {registry}/gen/go/{owner}/{module}/{plugin-owner}/{plugin-name}.
func parseGenSDKModulePath(modulePath string) (registry, owner, module string, ok bool) {
	parts := strings.SplitN(modulePath, "/", 7)
	if len(parts) < 5 || parts[1] != "gen" || parts[2] != "go" {
		return "", "", "", false
	}
	return parts[0], parts[3], parts[4], true
}

// parsePseudoVersionTimestamp parses the timestamp from a Go module pseudo-version.
// Pseudo-versions have the format: vX.Y.Z-YYYYMMDDHHMMSS-COMMIT.N.
func parsePseudoVersionTimestamp(version string) (time.Time, bool) {
	parts := strings.SplitN(version, "-", 3)
	if len(parts) != 3 {
		return time.Time{}, false
	}
	t, err := time.Parse("20060102150405", parts[1])
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// extractCommitFromPseudoVersion extracts the commit hash from a Go module pseudo-version.
// Generated SDK pseudo-versions have the format: vX.Y.Z-YYYYMMDDHHMMSS-COMMIT.N.
func extractCommitFromPseudoVersion(version string) string {
	parts := strings.SplitN(version, "-", 3)
	if len(parts) != 3 {
		return ""
	}
	// The third part is "COMMIT.N" - strip the trailing ".N".
	commitAndPatch := parts[2]
	if idx := strings.LastIndex(commitAndPatch, "."); idx != -1 {
		return commitAndPatch[:idx]
	}
	return commitAndPatch
}
