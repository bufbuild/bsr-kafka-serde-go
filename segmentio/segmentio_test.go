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

package segmentio_test

import (
	"crypto/rand"
	"net/url"
	"testing"

	modulev1 "buf.build/gen/go/bufbuild/registry/protocolbuffers/go/buf/registry/module/v1"
	serde "github.com/bufbuild/bsr-kafka-serde-go"
	"github.com/bufbuild/bsr-kafka-serde-go/internal/serdetest"
	"github.com/bufbuild/bsr-kafka-serde-go/segmentio"
	"github.com/google/go-cmp/cmp"
	kafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSegmentio(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	commitID := rand.Text()
	handler := &serdetest.FDSHandler{CommitID: commitID}
	server := serdetest.NewServer(t, handler)
	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	segmentioSerde := segmentio.New(
		serverURL.Host,
		serde.WithHTTPClient(server.Client()),
	)
	commit := &modulev1.Commit{
		Id: commitID,
	}
	message, err := segmentioSerde.Serialize(ctx, commit)
	require.NoError(t, err)
	// Serialize stamps the message header.
	assert.Equal(t, string(commit.ProtoReflect().Descriptor().FullName()), findHeader(message.Headers, serde.BufRegistryValueSchemaMessage))

	// Bufstream, internally, will stamp the commit header.
	message.Headers = append(message.Headers,
		kafka.Header{
			Key:   serde.BufRegistryValueSchemaCommit,
			Value: []byte(commitID),
		},
	)

	newCommit := &modulev1.Commit{}
	require.NoError(t, segmentioSerde.DeserializeTo(message, newCommit))

	assert.Empty(t, cmp.Diff(commit, newCommit, protocmp.Transform()))

	// If we _didn't_ know the type, we can hit the FDS service and deserialize that way.
	dynamicMessage, err := segmentioSerde.Deserialize(ctx, message)
	require.NoError(t, err)

	assert.Empty(t, cmp.Diff(commit, dynamicMessage, protocmp.Transform()))
}

func TestSegmentioSerializeSDKCommitHeader(t *testing.T) {
	t.Parallel()
	// timestamppb.Timestamp is from google.golang.org/protobuf, which uses regular semver with
	// no BSR commit embedded. Serialize will not make any BSR calls and must not add the SDK
	// commit header, so no server is needed.
	segmentioSerde := segmentio.New("test.example.com")
	// The positive case (gen SDK types producing a non-empty commit) is exercised by
	// TestSegmentioSerializeSDKCommitHeader in segmentio/example.
	ts := &timestamppb.Timestamp{}
	msg, err := segmentioSerde.Serialize(t.Context(), ts)
	require.NoError(t, err)
	assert.Empty(t, findHeader(msg.Headers, serde.BufRegistryValueSchemaCommit))
	assert.Equal(t, string(ts.ProtoReflect().Descriptor().FullName()), findHeader(msg.Headers, serde.BufRegistryValueSchemaMessage))
}

func findHeader(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
