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

package main

import (
	"testing"

	demov1 "demo.buf.dev/gen/go/bufbuild/bufstream-demo/protocolbuffers/go/bufstream/demo/v1"
	serde "github.com/bufbuild/bsr-kafka-serde-go"
	"github.com/bufbuild/bsr-kafka-serde-go/confluent"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfluentSerializeSDKCommitHeader(t *testing.T) {
	t.Parallel()
	// demov1.EmailUpdated is from a BSR-generated SDK module; the full commit ID should be set.
	// This test runs in a package main binary, so buildDeps is populated.
	confluentSerde := confluent.New("demo.buf.dev")
	msg, err := confluentSerde.Serialize(t.Context(), &demov1.EmailUpdated{})
	require.NoError(t, err)
	assert.NotEmpty(t, findHeader(msg.Headers, serde.BufRegistryValueSchemaCommit))
}

func TestConfluentWithoutCommitResolution(t *testing.T) {
	t.Parallel()
	// WithoutCommitResolution should suppress the commit header even for BSR-generated SDK types.
	confluentSerde := confluent.New("demo.buf.dev", serde.WithoutCommitResolution())
	msg, err := confluentSerde.Serialize(t.Context(), &demov1.EmailUpdated{})
	require.NoError(t, err)
	assert.Empty(t, findHeader(msg.Headers, serde.BufRegistryValueSchemaCommit))
	assert.NotEmpty(t, findHeader(msg.Headers, serde.BufRegistryValueSchemaMessage))
}

func findHeader(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
