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
	"reflect"
	"runtime/debug"
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
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExtractCommitFromPseudoVersion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		version string
		want    string
	}{
		{
			version: "v1.19.1-20260126144947-819582968857.2",
			want:    "819582968857",
		},
		{
			version: "v1.36.11-20260126144947-819582968857.1",
			want:    "819582968857",
		},
		{
			version: "v1.36.11", // regular semver, no commit
			want:    "",
		},
		{
			version: "v0.0.0-20250101000000-abcdef123456.1",
			want:    "abcdef123456",
		},
	}
	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, extractCommitFromPseudoVersion(tt.version))
		})
	}
}

func TestFindDepForPkgPath(t *testing.T) {
	t.Parallel()
	registryDep := &debug.Module{
		Path:    "buf.build/gen/go/bufbuild/registry/protocolbuffers/go",
		Version: "v1.36.11-20260126144947-819582968857.1",
	}
	protovalidateDep := &debug.Module{
		Path:    "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go",
		Version: "v1.36.11-20260209202127-80ab13bee0bf.1",
	}
	googleDep := &debug.Module{
		Path:    "google.golang.org/protobuf",
		Version: "v1.36.11",
	}
	deps := []*debug.Module{registryDep, protovalidateDep, googleDep}

	t.Run("generated SDK module matches longest prefix", func(t *testing.T) {
		t.Parallel()
		// Package path is deeper than the module path; the module is the longest prefix.
		pkgPath := "buf.build/gen/go/bufbuild/registry/protocolbuffers/go/buf/registry/module/v1"
		dep := findDepForPkgPath(pkgPath, deps)
		require.NotNil(t, dep)
		assert.Equal(t, "819582968857", extractCommitFromPseudoVersion(dep.Version))
	})
	t.Run("local type with regular semver returns no commit", func(t *testing.T) {
		t.Parallel()
		// google.golang.org/protobuf uses regular semver; no commit can be extracted.
		pkgPath := "google.golang.org/protobuf/types/known/timestamppb"
		dep := findDepForPkgPath(pkgPath, deps)
		require.NotNil(t, dep)
		assert.Empty(t, extractCommitFromPseudoVersion(dep.Version))
	})
	t.Run("no matching dep returns nil", func(t *testing.T) {
		t.Parallel()
		pkgPath := "example.com/some/local/package"
		assert.Nil(t, findDepForPkgPath(pkgPath, deps))
	})
	t.Run("empty dep list returns nil", func(t *testing.T) {
		t.Parallel()
		pkgPath := "buf.build/gen/go/bufbuild/registry/protocolbuffers/go/buf/registry/module/v1"
		assert.Nil(t, findDepForPkgPath(pkgPath, nil))
	})
}

func TestParseGenSDKModulePath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		modulePath string
		wantOwner  string
		wantModule string
		wantOK     bool
	}{
		{
			modulePath: "buf.build/gen/go/bufbuild/registry/protocolbuffers/go",
			wantOwner:  "bufbuild",
			wantModule: "registry",
			wantOK:     true,
		},
		{
			modulePath: "demo.buf.dev/gen/go/bufbuild/bufstream-demo/protocolbuffers/go",
			wantOwner:  "bufbuild",
			wantModule: "bufstream-demo",
			wantOK:     true,
		},
		{
			modulePath: "google.golang.org/protobuf",
			wantOK:     false,
		},
		{
			modulePath: "buf.build/gen/go/bufbuild",
			wantOK:     false,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.modulePath, func(t *testing.T) {
			t.Parallel()
			owner, module, ok := parseGenSDKModulePath(testCase.modulePath)
			assert.Equal(t, testCase.wantOK, ok)
			assert.Equal(t, testCase.wantOwner, owner)
			assert.Equal(t, testCase.wantModule, module)
		})
	}
}

func TestParsePseudoVersionTimestamp(t *testing.T) {
	t.Parallel()
	tests := []struct {
		version  string
		wantTime string // RFC3339
		wantOK   bool
	}{
		{
			version:  "v1.36.11-20260126144947-819582968857.1",
			wantTime: "2026-01-26T14:49:47Z",
			wantOK:   true,
		},
		{
			version:  "v0.0.0-20250101000000-abcdef123456.1",
			wantTime: "2025-01-01T00:00:00Z",
			wantOK:   true,
		},
		{
			version: "v1.36.11", // regular semver, no timestamp
			wantOK:  false,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.version, func(t *testing.T) {
			t.Parallel()
			ts, ok := parsePseudoVersionTimestamp(testCase.version)
			assert.Equal(t, testCase.wantOK, ok)
			if testCase.wantOK {
				assert.Equal(t, testCase.wantTime, ts.UTC().Format(time.RFC3339))
			}
		})
	}
}

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

	t.Run("valid", func(t *testing.T) {
		t.Parallel()
		dynamicMessage, err := serde.Deserialize(ctx, value, "abc", "buf.registry.module.v1.Commit", time.Now)
		require.NoError(t, err)
		assert.Empty(t, cmp.Diff(commit, dynamicMessage, protocmp.Transform()))
	})
	t.Run("negative cache", func(t *testing.T) {
		t.Parallel()
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
	})
	t.Run("retries", func(t *testing.T) {
		t.Parallel()
		deserializedMessage, err := serde.Deserialize(ctx, value, "xyz", "buf.registry.module.v1.Commit", time.Now)
		require.NoError(t, err)
		// Should have hit the handler three times; first two were retried transparently.
		require.Equal(t, int32(3), handler.retries.Load())
		assert.Empty(t, cmp.Diff(commit, deserializedMessage, protocmp.Transform()))
	})
}

type fdsHandler struct {
	commitID string

	hits    atomic.Int32
	retries atomic.Int32
	misses  atomic.Int32
}

func (h *fdsHandler) GetFileDescriptorSet(_ context.Context, req *modulev1.GetFileDescriptorSetRequest) (*modulev1.GetFileDescriptorSetResponse, error) {
	validResponse := &modulev1.GetFileDescriptorSetResponse{
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
	}
	if req.ResourceRef.GetId() == "abc" {
		h.hits.Add(1)
		return validResponse, nil
	} else if req.ResourceRef.GetId() == "xyz" {
		// Should retry this error twice, then get a reasonable response.
		h.retries.Add(1)
		if h.retries.Load() > 2 {
			return validResponse, nil
		}
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("unavailable"))
	}
	h.misses.Add(1)
	return nil, connect.NewError(connect.CodeNotFound, errors.New("not found"))
}

func TestGenSDKCommitFromMessage(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	t.Run("skip commit resolution", func(t *testing.T) {
		t.Parallel()
		// WithoutCommitResolution should short-circuit before any dep scan or BSR call.
		serdeInstance := New("test.example.com", serde.WithoutCommitResolution())
		commit, err := serdeInstance.GenSDKCommitFromMessage(ctx, &timestamppb.Timestamp{})
		require.NoError(t, err)
		assert.Empty(t, commit)
		// typeCommits should be empty — we returned before populating it.
		serdeInstance.typeCommitsMutex.RLock()
		assert.Empty(t, serdeInstance.typeCommits)
		serdeInstance.typeCommitsMutex.RUnlock()
	})
	t.Run("non-gen-SDK type gets negative cache entry", func(t *testing.T) {
		t.Parallel()
		// In a non-main test binary, buildDeps() returns nil, so timestamppb.Timestamp has no
		// matching dep. The call should return "", nil and cache the negative result.
		serdeInstance := New("test.example.com")
		msgType := reflect.TypeFor[timestamppb.Timestamp]()

		commit, err := serdeInstance.GenSDKCommitFromMessage(ctx, &timestamppb.Timestamp{})
		require.NoError(t, err)
		assert.Empty(t, commit)

		serdeInstance.typeCommitsMutex.RLock()
		cached, ok := serdeInstance.typeCommits[msgType]
		serdeInstance.typeCommitsMutex.RUnlock()
		assert.True(t, ok, "expected type to be cached after first call")
		assert.Empty(t, cached, "expected empty string cached for non-gen-SDK type")
	})
	t.Run("subsequent calls use type cache", func(t *testing.T) {
		t.Parallel()
		// Pre-populate the type cache as if a previous call had resolved the commit.
		// This verifies GenSDKCommitFromMessage returns the cached value without scanning deps.
		serdeInstance := New("test.example.com")
		msgType := reflect.TypeFor[timestamppb.Timestamp]()
		const fakeCommit = "fakefullcommitid"
		serdeInstance.typeCommitsMutex.Lock()
		serdeInstance.typeCommits[msgType] = fakeCommit
		serdeInstance.typeCommitsMutex.Unlock()

		commit, err := serdeInstance.GenSDKCommitFromMessage(ctx, &timestamppb.Timestamp{})
		require.NoError(t, err)
		assert.Equal(t, fakeCommit, commit)
	})
}

func TestResolveCommit(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	// depVersion and its embedded timestamp, used for multi-result disambiguation.
	const (
		depPath     = "buf.build/gen/go/bufbuild/registry/protocolbuffers/go"
		depVersion  = "v1.36.11-20260126144947-819582968857.1"
		shortCommit = "819582968857"
	)
	// ts is the timestamp embedded in depVersion.
	ts, err := time.Parse("20060102150405", "20260126144947")
	require.NoError(t, err)
	// newTestSerde creates a fresh Serde backed by a new server+handler, giving each sub-test
	// an isolated cache and call counter.
	newTestSerde := func(commitID string) (*Serde, *commitHandler) {
		handler := &commitHandler{commitID: commitID, ts: ts}
		server := newCommitServer(t, handler)
		serverURL, err := url.Parse(server.URL)
		require.NoError(t, err)
		return New(serverURL.Host, serde.WithHTTPClient(server.Client())), handler
	}

	t.Run("single result", func(t *testing.T) {
		t.Parallel()
		commitID := rand.Text()
		s, _ := newTestSerde(commitID)
		commit, err := s.resolveCommit(ctx, depPath, depVersion, shortCommit)
		require.NoError(t, err)
		assert.Equal(t, commitID, commit)
	})
	t.Run("multiple results match timestamp", func(t *testing.T) {
		t.Parallel()
		commitID := rand.Text()
		s, _ := newTestSerde(commitID)
		commit, err := s.resolveCommit(ctx, depPath, depVersion, "multi")
		require.NoError(t, err)
		assert.Equal(t, commitID, commit)
	})
	t.Run("multiple results no timestamp match", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestSerde(rand.Text())
		_, err := s.resolveCommit(ctx, depPath, depVersion, "nomatch")
		require.Error(t, err)
	})
	t.Run("no results", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestSerde(rand.Text())
		_, err := s.resolveCommit(ctx, depPath, depVersion, "empty")
		require.Error(t, err)
	})
	t.Run("cached", func(t *testing.T) {
		t.Parallel()
		commitID := rand.Text()
		s, handler := newTestSerde(commitID)
		for range 3 {
			commit, err := s.resolveCommit(ctx, depPath, depVersion, shortCommit)
			require.NoError(t, err)
			assert.Equal(t, commitID, commit)
		}
		// Only one BSR call should have been made despite three resolveCommit calls.
		assert.Equal(t, int32(1), handler.calls.Load())
	})
	t.Run("retries", func(t *testing.T) {
		t.Parallel()
		commitID := rand.Text()
		s, handler := newTestSerde(commitID)
		commit, err := s.resolveCommit(ctx, depPath, depVersion, "retry")
		require.NoError(t, err)
		assert.Equal(t, commitID, commit)
		// Should have hit the handler three times; first two were retried transparently.
		assert.Equal(t, int32(3), handler.retries.Load())
	})
}

type commitHandler struct {
	modulev1connect.UnimplementedCommitServiceHandler

	commitID string
	ts       time.Time // expected timestamp for multi-result tests

	calls   atomic.Int32
	retries atomic.Int32
}

func (h *commitHandler) ListCommits(_ context.Context, req *modulev1.ListCommitsRequest) (*modulev1.ListCommitsResponse, error) {
	h.calls.Add(1)
	switch req.GetIdQuery() {
	case "multi":
		// Return two commits: one with the matching timestamp, one without.
		return &modulev1.ListCommitsResponse{
			Commits: []*modulev1.Commit{
				{Id: "other-id", CreateTime: timestamppb.New(h.ts.Add(time.Hour))},
				{Id: h.commitID, CreateTime: timestamppb.New(h.ts)},
			},
		}, nil
	case "nomatch":
		// Return two commits, neither with the matching timestamp.
		return &modulev1.ListCommitsResponse{
			Commits: []*modulev1.Commit{
				{Id: "commit-a", CreateTime: timestamppb.New(h.ts.Add(time.Hour))},
				{Id: "commit-b", CreateTime: timestamppb.New(h.ts.Add(2 * time.Hour))},
			},
		}, nil
	case "empty":
		return &modulev1.ListCommitsResponse{}, nil
	case "retry":
		h.retries.Add(1)
		if h.retries.Load() > 2 {
			return &modulev1.ListCommitsResponse{
				Commits: []*modulev1.Commit{{Id: h.commitID}},
			}, nil
		}
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("unavailable"))
	default:
		// Default: single result.
		return &modulev1.ListCommitsResponse{
			Commits: []*modulev1.Commit{{Id: h.commitID}},
		}, nil
	}
}

func newServer(t *testing.T, svc modulev1connect.FileDescriptorSetServiceHandler) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(modulev1connect.NewFileDescriptorSetServiceHandler(svc))
	return httptest.NewTLSServer(mux)
}

func newCommitServer(t *testing.T, svc modulev1connect.CommitServiceHandler) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(modulev1connect.NewCommitServiceHandler(svc))
	return httptest.NewTLSServer(mux)
}
