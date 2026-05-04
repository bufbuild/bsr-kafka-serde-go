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

// Package serdetest provides shared test helpers for Kafka serde integration tests.
package serdetest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"buf.build/gen/go/bufbuild/registry/connectrpc/gosimple/buf/registry/module/v1/modulev1connect"
	modulev1 "buf.build/gen/go/bufbuild/registry/protocolbuffers/go/buf/registry/module/v1"
	extensionv1beta1 "buf.build/gen/go/bufbuild/registry/protocolbuffers/go/buf/registry/priv/extension/v1beta1"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// FDSHandler is a mock BSR Connect service that serves a fixed file descriptor set and commit.
type FDSHandler struct {
	modulev1connect.UnimplementedCommitServiceHandler

	CommitID string
}

func (h *FDSHandler) GetFileDescriptorSet(_ context.Context, _ *modulev1.GetFileDescriptorSetRequest) (*modulev1.GetFileDescriptorSetResponse, error) {
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
			Id: h.CommitID,
		},
	}, nil
}

func (h *FDSHandler) ListCommits(_ context.Context, _ *modulev1.ListCommitsRequest) (*modulev1.ListCommitsResponse, error) {
	return &modulev1.ListCommitsResponse{
		Commits: []*modulev1.Commit{{Id: h.CommitID}},
	}, nil
}

// NewServer starts a TLS httptest server with the BSR file descriptor set and commit services.
func NewServer(t *testing.T, handler *FDSHandler) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(modulev1connect.NewFileDescriptorSetServiceHandler(handler))
	mux.Handle(modulev1connect.NewCommitServiceHandler(handler))
	return httptest.NewTLSServer(mux)
}
