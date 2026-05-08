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

	logsv1 "buf.build/gen/go/opentelemetry/opentelemetry/protocolbuffers/go/opentelemetry/proto/logs/v1"
	serde "github.com/bufbuild/bsr-kafka-serde-go"
	"github.com/bufbuild/bsr-kafka-serde-go/segmentio"
	kafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSegmentioSerializeSDKCommitHeader(t *testing.T) {
	t.Parallel()
	// logsv1.LogRecord is from a BSR-generated SDK module; the full commit ID should be set.
	// This test runs in a package main binary, so buildDeps is populated.
	segmentioSerde := segmentio.New("buf.build")
	msg, err := segmentioSerde.Serialize(t.Context(), &logsv1.LogRecord{})
	require.NoError(t, err)
	assert.NotEmpty(t, findHeader(msg.Headers, serde.BufRegistryValueSchemaCommit))
}

func TestSegmentioWithoutCommitResolution(t *testing.T) {
	t.Parallel()
	// WithoutCommitResolution should suppress the commit header even for BSR-generated SDK types.
	segmentioSerde := segmentio.New("buf.build", serde.WithoutCommitResolution())
	msg, err := segmentioSerde.Serialize(t.Context(), &logsv1.LogRecord{})
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
