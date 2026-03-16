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
	"connectrpc.com/connect"
)

// Option modifies a Serde instance.
type Option interface {
	ApplyToSerde(cfg *Config)
}

type Config struct {
	HTTPClient           connect.HTTPClient
	Token                string
	SkipCommitResolution bool
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
