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

// buildDeps is the list of module dependencies from the build info, read once.
var buildDeps = sync.OnceValue(func() []*debug.Module { //nolint:gochecknoglobals // read-only after first call via sync.OnceValue
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return nil
	}
	return info.Deps
})

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
		cache:           map[cacheKey]cacheValue{},
		sfGroup:         &singleflight.Group{},
		resolvedCommits: map[string]string{},
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
	mu    sync.RWMutex
	cache map[cacheKey]cacheValue

	sfGroup                 *singleflight.Group
	fileDescriptorSetClient modulev1connect.FileDescriptorSetServiceClient

	resolvedCommitsMutex sync.RWMutex
	resolvedCommits      map[string]string // module path → full commit ID

	commitClient  modulev1connect.CommitServiceClient
	commitSFGroup *singleflight.Group
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

// GenSDKCommitFromMessage returns the full BSR commit ID of the generated SDK module containing
// the given [proto.Message] type. It extracts the short commit from the module's pseudo-version in
// the build info, then resolves it to the full commit ID via the BSR API.
// Returns an empty string and no error if the type is not from a BSR-generated SDK module.
func (s *Serde) GenSDKCommitFromMessage(ctx context.Context, src proto.Message) (string, error) {
	msgType := reflect.TypeOf(src)
	if msgType.Kind() == reflect.Pointer {
		msgType = msgType.Elem()
	}
	dep := findDepForPkgPath(msgType.PkgPath(), buildDeps())
	if dep == nil {
		return "", nil
	}
	shortCommit := extractCommitFromPseudoVersion(dep.Version)
	if shortCommit == "" {
		return "", nil
	}
	return s.resolveCommit(ctx, dep.Path, dep.Version, shortCommit)
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

// resolveCommit resolves a short commit ID extracted from a module pseudo-version to the full BSR
// commit ID, using [modulev1connect.CommitServiceClient.ListCommits] with an ID query. Results are
// cached by module path; concurrent calls for the same module path are deduplicated.
func (s *Serde) resolveCommit(ctx context.Context, depPath, depVersion, shortCommit string) (string, error) {
	s.resolvedCommitsMutex.RLock()
	commit, ok := s.resolvedCommits[depPath]
	s.resolvedCommitsMutex.RUnlock()
	if ok {
		return commit, nil
	}
	result, err, _ := s.commitSFGroup.Do(depPath, func() (any, error) {
		s.resolvedCommitsMutex.RLock()
		commit, ok := s.resolvedCommits[depPath]
		s.resolvedCommitsMutex.RUnlock()
		if ok {
			return commit, nil
		}
		owner, module, ok := parseGenSDKModulePath(depPath)
		if !ok {
			return "", fmt.Errorf("could not parse BSR module path from %q", depPath)
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
					IdQuery: shortCommit,
					ResourceRef: &modulev1.ResourceRef{
						Value: &modulev1.ResourceRef_Name_{
							Name: &modulev1.ResourceRef_Name{
								Owner:  owner,
								Module: module,
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
			return "", fmt.Errorf("listing commits for %s/%s with id query %q: %w", owner, module, shortCommit, err)
		}
		commits := response.GetCommits()
		var fullCommit string
		switch len(commits) {
		case 0:
			return "", fmt.Errorf("no commits found for %s/%s with id query %q", owner, module, shortCommit)
		case 1:
			fullCommit = commits[0].GetId()
		default:
			ts, ok := parsePseudoVersionTimestamp(depVersion)
			if !ok {
				return "", fmt.Errorf("could not parse timestamp from pseudo-version %q", depVersion)
			}
			for _, commit := range commits {
				if commit.GetCreateTime().AsTime().Equal(ts) {
					fullCommit = commit.GetId()
					break
				}
			}
			if fullCommit == "" {
				return "", fmt.Errorf("no commit found for %s/%s with id query %q matching timestamp from %q", owner, module, shortCommit, depVersion)
			}
		}
		s.resolvedCommitsMutex.Lock()
		s.resolvedCommits[depPath] = fullCommit
		s.resolvedCommitsMutex.Unlock()
		return fullCommit, nil
	})
	if err != nil {
		return "", err
	}
	fullCommit, _ := result.(string)
	return fullCommit, nil
}

// findDepForPkgPath finds the module dependency with the longest path prefix matching pkgPath.
// Separated from [GenSDKCommitFromMessage] for testability.
func findDepForPkgPath(pkgPath string, deps []*debug.Module) *debug.Module {
	var bestDep *debug.Module
	for _, dep := range deps {
		if strings.HasPrefix(pkgPath, dep.Path) {
			if bestDep == nil || len(dep.Path) > len(bestDep.Path) {
				bestDep = dep
			}
		}
	}
	return bestDep
}

// parseGenSDKModulePath parses the owner and module name from a BSR-generated Go SDK module path.
// The path format is: {host}/gen/go/{owner}/{module}/{sdk-type}/{language}.
func parseGenSDKModulePath(modulePath string) (owner, module string, ok bool) {
	parts := strings.SplitN(modulePath, "/", 7)
	if len(parts) < 5 || parts[1] != "gen" || parts[2] != "go" {
		return "", "", false
	}
	return parts[3], parts[4], true
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
