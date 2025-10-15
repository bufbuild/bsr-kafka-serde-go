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
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"buf.build/gen/go/bufbuild/registry/connectrpc/gosimple/buf/registry/module/v1/modulev1connect"
	modulev1 "buf.build/gen/go/bufbuild/registry/protocolbuffers/go/buf/registry/module/v1"
	extensionv1beta1 "buf.build/gen/go/bufbuild/registry/protocolbuffers/go/buf/registry/priv/extension/v1beta1"
	"connectrpc.com/connect"
	serde "github.com/bufbuild/bsr-kafka-serde-go"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSerde(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	commitID := rand.Text()
	handler := &fdsHandler{commitID: commitID}
	server := newServer(t, handler)
	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	serde := New(
		serverURL.Host,
		serde.WithHTTPClient(server.Client()),
	)
	commit := &modulev1.Commit{
		Id: commitID,
	}
	value, err := proto.Marshal(commit)
	require.NoError(t, err)

	dynamicMessage, err := serde.Deserialize(ctx, value, "abc", "buf.registry.module.v1.Commit", time.Now)
	require.NoError(t, err)

	assert.Empty(t, cmp.Diff(commit, dynamicMessage, protocmp.Transform()))

	_, err = serde.Deserialize(ctx, value, "miss", "buf.registry.module.v1.Commit", time.Now)
	require.Error(t, err)
	require.Equal(t, int32(1), handler.misses.Load())

	// Try again; should be cached and not hit handler.
	_, err = serde.Deserialize(ctx, value, "miss", "buf.registry.module.v1.Commit", time.Now)
	require.Error(t, err)
	require.Equal(t, int32(1), handler.misses.Load())

	// Negative cache should expire after 1 minute.
	_, err = serde.Deserialize(ctx, value, "miss", "buf.registry.module.v1.Commit", func() time.Time { return time.Now().Add(2 * time.Minute) })
	require.Error(t, err)
	require.Equal(t, int32(2), handler.misses.Load())
}

type fdsHandler struct {
	commitID string

	hits   atomic.Int32
	misses atomic.Int32
}

func (h *fdsHandler) GetFileDescriptorSet(_ context.Context, req *modulev1.GetFileDescriptorSetRequest) (*modulev1.GetFileDescriptorSetResponse, error) {
	if req.ResourceRef.GetId() == "abc" {
		h.hits.Add(1)
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
				},
			},
			Commit: &modulev1.Commit{
				Id: h.commitID,
			},
		}, nil
	}
	h.misses.Add(1)
	return nil, connect.NewError(connect.CodeNotFound, errors.New("not found"))
}

func newServer(t *testing.T, svc modulev1connect.FileDescriptorSetServiceHandler) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(modulev1connect.NewFileDescriptorSetServiceHandler(svc))
	return httptest.NewTLSServer(mux)
}
