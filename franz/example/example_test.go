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
	"github.com/bufbuild/bsr-kafka-serde-go/franz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFranzSerializeSDKCommitHeader(t *testing.T) {
	t.Parallel()
	// logsv1.LogRecord is from a BSR-generated SDK module; the full commit ID should be set.
	// This test runs in a package main binary, so buildDeps is populated.
	franzSerde := franz.New("buf.build")
	record, err := franzSerde.Serialize(t.Context(), &logsv1.LogRecord{})
	require.NoError(t, err)
	assert.NotEmpty(t, findHeader(record.Headers, serde.BufRegistryValueSchemaCommit))
}

func TestFranzWithoutCommitResolution(t *testing.T) {
	t.Parallel()
	// WithoutCommitResolution should suppress the commit header even for BSR-generated SDK types.
	franzSerde := franz.New("buf.build", serde.WithoutCommitResolution())
	record, err := franzSerde.Serialize(t.Context(), &logsv1.LogRecord{})
	require.NoError(t, err)
	assert.Empty(t, findHeader(record.Headers, serde.BufRegistryValueSchemaCommit))
	assert.NotEmpty(t, findHeader(record.Headers, serde.BufRegistryValueSchemaMessage))
}

func findHeader(headers []kgo.RecordHeader, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
