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

	"buf.build/gen/go/bufbuild/registry/connectrpc/gosimple/buf/registry/module/v1/modulev1connect"
	modulev1 "buf.build/gen/go/bufbuild/registry/protocolbuffers/go/buf/registry/module/v1"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// FDSHandler is a mock BSR Connect service that serves a fixed file descriptor set and commit.
type FDSHandler struct {
	modulev1connect.UnimplementedCommitServiceHandler

	CommitID string
}

func (h *FDSHandler) GetFileDescriptorSet(_ context.Context, _ *modulev1.GetFileDescriptorSetRequest) (*modulev1.GetFileDescriptorSetResponse, error) {
	return &modulev1.GetFileDescriptorSetResponse{
		FileDescriptorSet: FileDescriptorSet(),
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

func FileDescriptorSet() *descriptorpb.FileDescriptorSet {
	return newFileDescriptorSet(
		modulev1.File_buf_registry_module_v1_commit_proto,
		modulev1.File_buf_registry_module_v1_digest_proto,
		durationpb.File_google_protobuf_duration_proto,
		fieldmaskpb.File_google_protobuf_field_mask_proto,
	)
}

func newFileDescriptorSet(roots ...protoreflect.FileDescriptor) *descriptorpb.FileDescriptorSet {
	fileDescriptorSet := &descriptorpb.FileDescriptorSet{}
	seen := make(map[string]struct{})
	var addFile func(protoreflect.FileDescriptor)
	addFile = func(file protoreflect.FileDescriptor) {
		if _, ok := seen[file.Path()]; ok {
			return
		}
		seen[file.Path()] = struct{}{}
		imports := file.Imports()
		for i := range imports.Len() {
			addFile(imports.Get(i).FileDescriptor)
		}
		fileDescriptorSet.File = append(fileDescriptorSet.File, protodesc.ToFileDescriptorProto(file))
	}
	for _, root := range roots {
		addFile(root)
	}
	return fileDescriptorSet
}
