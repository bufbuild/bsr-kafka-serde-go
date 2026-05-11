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

package serde

import (
	"context"
	"maps"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
)

// Option modifies a Serde instance.
type Option interface {
	ApplyToSerde(cfg *Config)
}

// CommitResolver returns the BSR commit ID for src. moduleFullName has the form
// "registry/owner/name" (e.g. "buf.build/opentelemetry/opentelemetry") when src is
// from a BSR-generated SDK module, and is empty otherwise. Returning "" with a nil
// error skips the [BufRegistryValueSchemaCommit] header.
//
// Resolvers must be safe for concurrent use. Results are cached per Go message type
// for the lifetime of the Serde instance.
type CommitResolver func(ctx context.Context, src proto.Message, moduleFullName string) (string, error)

type Config struct {
	HTTPClient           connect.HTTPClient
	Token                string
	SkipCommitResolution bool
	CommitResolver       CommitResolver
}

// WithHTTPClient allows swapping out the http client used for interacting with the BSR.
// By default, [http.DefaultClient] will be used.
func WithHTTPClient(httpClient connect.HTTPClient) Option {
	return &httpClientOption{
		httpClient: httpClient,
	}
}

// WithToken allows configuring an authentication token for the BSR.
func WithToken(token string) Option {
	return &tokenOption{
		token: token,
	}
}

// WithoutCommitResolution disables automatic BSR commit resolution during serialization.
// When set, Serialize will not look up the BSR commit for the serialized message and
// will not set the [BufRegistryValueSchemaCommit] header. This is useful when the
// commit header is set by another component (e.g. Bufstream) rather than the producer.
func WithoutCommitResolution() Option {
	return &withoutCommitResolutionOption{}
}

// WithCommitResolver replaces the default BSR commit resolution with the given
// [CommitResolver], avoiding the runtime BSR API call. [WithoutCommitResolution]
// takes precedence: if both are set, resolver is not called.
func WithCommitResolver(resolver CommitResolver) Option {
	return &commitResolverOption{
		resolver: resolver,
	}
}

// WithStaticModuleCommits returns commit IDs from a static map keyed by module full name
// (e.g. "buf.build/opentelemetry/opentelemetry"). Messages whose module full name is not
// in the map will not have the [BufRegistryValueSchemaCommit] header set.
func WithStaticModuleCommits(commits map[string]string) Option {
	cloned := maps.Clone(commits)
	return WithCommitResolver(func(_ context.Context, _ proto.Message, moduleFullName string) (string, error) {
		return cloned[moduleFullName], nil
	})
}

type httpClientOption struct {
	httpClient connect.HTTPClient
}

func (o *httpClientOption) ApplyToSerde(cfg *Config) {
	cfg.HTTPClient = o.httpClient
}

type tokenOption struct {
	token string
}

func (o *tokenOption) ApplyToSerde(cfg *Config) {
	cfg.Token = o.token
}

type withoutCommitResolutionOption struct{}

func (o *withoutCommitResolutionOption) ApplyToSerde(cfg *Config) {
	cfg.SkipCommitResolution = true
}

type commitResolverOption struct {
	resolver CommitResolver
}

func (o *commitResolverOption) ApplyToSerde(cfg *Config) {
	cfg.CommitResolver = o.resolver
}
