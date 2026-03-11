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

package confluent_test

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"buf.build/gen/go/bufbuild/registry/connectrpc/gosimple/buf/registry/module/v1/modulev1connect"
	modulev1 "buf.build/gen/go/bufbuild/registry/protocolbuffers/go/buf/registry/module/v1"
	extensionv1beta1 "buf.build/gen/go/bufbuild/registry/protocolbuffers/go/buf/registry/priv/extension/v1beta1"
	serde "github.com/bufbuild/bsr-kafka-serde-go"
	"github.com/bufbuild/bsr-kafka-serde-go/confluent"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestConfluent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	commitID := rand.Text()
	handler := &fdsHandler{commitID: commitID}
	server := newServer(t, handler)
	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	confluentSerde := confluent.New(
		serverURL.Host,
		serde.WithHTTPClient(server.Client()),
	)
	commit := &modulev1.Commit{
		Id: commitID,
	}
	message, err := confluentSerde.Serialize(ctx, commit)
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
	require.NoError(t, confluentSerde.DeserializeTo(message, newCommit))

	assert.Empty(t, cmp.Diff(commit, newCommit, protocmp.Transform()))

	// If we _didn't_ know the type, we can hit the FDS service and deserialize that way.
	dynamicMessage, err := confluentSerde.Deserialize(ctx, message)
	require.NoError(t, err)

	assert.Empty(t, cmp.Diff(commit, dynamicMessage, protocmp.Transform()))
}

func TestConfluentSerializeSDKCommitHeader(t *testing.T) {
	t.Parallel()
	// timestamppb.Timestamp is from google.golang.org/protobuf, which uses regular semver with
	// no BSR commit embedded. Serialize will not make any BSR calls and must not add the SDK
	// commit header, so no server is needed.
	confluentSerde := confluent.New("test.example.com")
	// The positive case (gen SDK types producing a non-empty commit) is exercised by
	// TestConfluentSerializeSDKCommitHeader in confluent/example.
	ts := &timestamppb.Timestamp{}
	msg, err := confluentSerde.Serialize(t.Context(), ts)
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

type fdsHandler struct {
	modulev1connect.UnimplementedCommitServiceHandler

	commitID string
}

func (h *fdsHandler) GetFileDescriptorSet(_ context.Context, _ *modulev1.GetFileDescriptorSetRequest) (*modulev1.GetFileDescriptorSetResponse, error) {
	return &modulev1.GetFileDescriptorSetResponse{
		FileDescriptorSet: &descriptorpb.FileDescriptorSet{
			File: []*descriptorpb.FileDescriptorProto{
				protodesc.ToFileDescriptorProto(modulev1.File_buf_registry_module_v1_commit_proto),
				protodesc.ToFileDescriptorProto(modulev1.File_buf_registry_module_v1_digest_proto),
				protodesc.ToFileDescriptorProto(extensionv1beta1.File_buf_registry_priv_extension_v1beta1_extension_proto),
				protodesc.ToFileDescriptorProto(validate.File_buf_validate_validate_proto),
				protodesc.ToFileDescriptorProto(descriptorpb.File_google_protobuf_descriptor_proto),
				protodesc.ToFileDescriptorProto(durationpb.File_google_protobuf_duration_proto),
				protodesc.ToFileDescriptorProto(timestamppb.File_google_protobuf_timestamp_proto),
				protodesc.ToFileDescriptorProto(fieldmaskpb.File_google_protobuf_field_mask_proto),
			},
		},
		Commit: &modulev1.Commit{
			Id: h.commitID,
		},
	}, nil
}

func (h *fdsHandler) ListCommits(_ context.Context, _ *modulev1.ListCommitsRequest) (*modulev1.ListCommitsResponse, error) {
	return &modulev1.ListCommitsResponse{
		Commits: []*modulev1.Commit{{Id: h.commitID}},
	}, nil
}

func newServer(t *testing.T, svc *fdsHandler) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(modulev1connect.NewFileDescriptorSetServiceHandler(svc))
	mux.Handle(modulev1connect.NewCommitServiceHandler(svc))
	return httptest.NewTLSServer(mux)
}
