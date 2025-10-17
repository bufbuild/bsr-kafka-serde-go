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

// Headers that Bufstream will stamp on records when validation is enabled.
const (
	BufRegistryValueSchemaMessage = "buf.registry.value.schema.message"
	BufRegistryValueSchemaCommit  = "buf.registry.value.schema.commit"
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
	return &Serde{
		cache:   map[cacheKey]cacheValue{},
		sfGroup: &singleflight.Group{},
		fileDescriptorSetClient: modulev1connect.NewFileDescriptorSetServiceClient(
			cfg.HTTPClient,
			"https://"+host,
			clientOptions...,
		),
	}
}

// Serde looks up FileDescriptorSets from a Buf Schema Registry, and caches lookups for
// [Serde.Deserialize] to use.
type Serde struct {
	mu    sync.RWMutex
	cache map[cacheKey]cacheValue

	sfGroup                 *singleflight.Group
	fileDescriptorSetClient modulev1connect.FileDescriptorSetServiceClient
}

// Deserialize deserializes the value into a [proto.Message], based on the commit and messageFQN.
func (s *Serde) Deserialize(ctx context.Context, value []byte, commit, messageFQN string, now func() time.Time) (proto.Message, error) {
	// Validate that we have what we need.
	if commit == "" {
		return nil, fmt.Errorf("could not find %q header in record", BufRegistryValueSchemaCommit)
	}
	if messageFQN == "" {
		return nil, fmt.Errorf("could not find %q header in record", BufRegistryValueSchemaMessage)
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
