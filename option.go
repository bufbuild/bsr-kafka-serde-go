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
	HTTPClient connect.HTTPClient
	Token      string
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
