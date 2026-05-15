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
	"encoding/hex"
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

func TestCollectGenSDKDeps(t *testing.T) {
	t.Parallel()
	modules := []*debug.Module{
		{
			Path:    "buf.build/gen/go/bufbuild/registry/protocolbuffers/go",
			Version: "v1.36.11-20260126144947-819582968857.1",
		},
		{
			Path:    "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go",
			Version: "v1.36.11-20260209202127-80ab13bee0bf.1",
		},
		{
			Path:    "google.golang.org/protobuf",
			Version: "v1.36.11",
		},
	}
	deps := collectGenSDKDeps(modules)
	// Non-gen-SDK deps (google.golang.org/protobuf) are filtered out.
	require.Len(t, deps, 2)
	assert.Equal(t, "buf.build/gen/go/bufbuild/registry/protocolbuffers/go", deps[0].pkgPathPrefix)
	assert.Equal(t, "buf.build/bufbuild/registry", deps[0].moduleFullName)
	assert.Equal(t, "bufbuild", deps[0].owner)
	assert.Equal(t, "registry", deps[0].module)
	assert.Equal(t, "819582968857", deps[0].shortCommit)
}

func TestFindGenSDKDep(t *testing.T) {
	t.Parallel()
	registryDep := newGenSDKDep(t,
		"buf.build/gen/go/bufbuild/registry/protocolbuffers/go",
		"v1.36.11-20260126144947-819582968857.1",
	)
	protovalidateDep := newGenSDKDep(t,
		"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go",
		"v1.36.11-20260209202127-80ab13bee0bf.1",
	)
	deps := []genSDKDep{registryDep, protovalidateDep}

	t.Run("matches longest prefix", func(t *testing.T) {
		t.Parallel()
		// Package path is deeper than the module path; the module is the longest prefix.
		pkgPath := "buf.build/gen/go/bufbuild/registry/protocolbuffers/go/buf/registry/module/v1"
		dep, ok := findGenSDKDep(pkgPath, deps)
		require.True(t, ok)
		assert.Equal(t, registryDep, dep)
	})
	t.Run("no matching dep returns false", func(t *testing.T) {
		t.Parallel()
		pkgPath := "google.golang.org/protobuf/types/known/timestamppb"
		_, ok := findGenSDKDep(pkgPath, deps)
		assert.False(t, ok)
	})
	t.Run("empty dep list returns false", func(t *testing.T) {
		t.Parallel()
		pkgPath := "buf.build/gen/go/bufbuild/registry/protocolbuffers/go/buf/registry/module/v1"
		_, ok := findGenSDKDep(pkgPath, nil)
		assert.False(t, ok)
	})
	t.Run("exact match returns dep", func(t *testing.T) {
		t.Parallel()
		dep, ok := findGenSDKDep(registryDep.pkgPathPrefix, deps)
		require.True(t, ok)
		assert.Equal(t, registryDep, dep)
	})
	t.Run("string prefix without path boundary does not match", func(t *testing.T) {
		t.Parallel()
		// Sibling plugin path shares a string prefix with the registryDep dep (".../protocolbuffers/go"
		// vs ".../protocolbuffers/google") but is not actually under that module.
		pkgPath := "buf.build/gen/go/bufbuild/registry/protocolbuffers/google/foo"
		_, ok := findGenSDKDep(pkgPath, deps)
		assert.False(t, ok)
	})
}

func TestParseGenSDKModulePath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		modulePath   string
		wantRegistry string
		wantOwner    string
		wantModule   string
		wantOK       bool
	}{
		{
			modulePath:   "buf.build/gen/go/bufbuild/registry/protocolbuffers/go",
			wantRegistry: "buf.build",
			wantOwner:    "bufbuild",
			wantModule:   "registry",
			wantOK:       true,
		},
		{
			modulePath:   "buf.build/gen/go/opentelemetry/opentelemetry/protocolbuffers/go",
			wantRegistry: "buf.build",
			wantOwner:    "opentelemetry",
			wantModule:   "opentelemetry",
			wantOK:       true,
		},
		{
			modulePath:   "registry.example.com/gen/go/myorg/mymodule/protocolbuffers/go",
			wantRegistry: "registry.example.com",
			wantOwner:    "myorg",
			wantModule:   "mymodule",
			wantOK:       true,
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
			registry, owner, module, ok := parseGenSDKModulePath(testCase.modulePath)
			assert.Equal(t, testCase.wantOK, ok)
			assert.Equal(t, testCase.wantRegistry, registry)
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
	commitID := newCommitID()
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
	t.Run("resolver returns commit for gen-SDK type", func(t *testing.T) {
		t.Parallel()
		want := newCommitID()
		var (
			gotModuleFullName string
			callCount         int
		)
		resolver := func(_ context.Context, _ proto.Message, moduleFullName string) (string, error) {
			gotModuleFullName = moduleFullName
			callCount++
			return want, nil
		}
		serdeInstance := New("buf.build", serde.WithCommitResolver(resolver))
		serdeInstance.genSDKDeps = []genSDKDep{newGenSDKDep(t,
			"buf.build/gen/go/bufbuild/registry/protocolbuffers/go",
			"v1.36.11-20260126144947-819582968857.1",
		)}
		msg := &modulev1.Commit{}
		commit, err := serdeInstance.GenSDKCommitFromMessage(ctx, msg)
		require.NoError(t, err)
		assert.Equal(t, want, commit)
		assert.Equal(t, "buf.build/bufbuild/registry", gotModuleFullName)
		assert.Equal(t, 1, callCount)
		// Second call should hit the type cache and not invoke the resolver again.
		commit, err = serdeInstance.GenSDKCommitFromMessage(ctx, msg)
		require.NoError(t, err)
		assert.Equal(t, want, commit)
		assert.Equal(t, 1, callCount)
	})
	t.Run("resolver called for non-gen-SDK type with empty module full name", func(t *testing.T) {
		t.Parallel()
		want := newCommitID()
		var gotModuleFullName string
		resolver := func(_ context.Context, _ proto.Message, moduleFullName string) (string, error) {
			gotModuleFullName = moduleFullName
			return want, nil
		}
		serdeInstance := New("buf.build", serde.WithCommitResolver(resolver))
		commit, err := serdeInstance.GenSDKCommitFromMessage(ctx, &timestamppb.Timestamp{})
		require.NoError(t, err)
		assert.Equal(t, want, commit)
		assert.Empty(t, gotModuleFullName)
	})
	t.Run("resolver error propagates and is not cached", func(t *testing.T) {
		t.Parallel()
		wantErr := errors.New("resolver failure")
		var callCount int
		resolver := func(_ context.Context, _ proto.Message, _ string) (string, error) {
			callCount++
			return "", wantErr
		}
		serdeInstance := New("buf.build", serde.WithCommitResolver(resolver))
		_, err := serdeInstance.GenSDKCommitFromMessage(ctx, &modulev1.Commit{})
		require.ErrorIs(t, err, wantErr)
		// Second call should re-invoke the resolver, since errors are not cached.
		_, err = serdeInstance.GenSDKCommitFromMessage(ctx, &modulev1.Commit{})
		require.ErrorIs(t, err, wantErr)
		assert.Equal(t, 2, callCount)
	})
	t.Run("resolver empty result is cached", func(t *testing.T) {
		t.Parallel()
		var callCount int
		resolver := func(_ context.Context, _ proto.Message, _ string) (string, error) {
			callCount++
			return "", nil
		}
		serdeInstance := New("buf.build", serde.WithCommitResolver(resolver))
		commit, err := serdeInstance.GenSDKCommitFromMessage(ctx, &modulev1.Commit{})
		require.NoError(t, err)
		assert.Empty(t, commit)
		commit, err = serdeInstance.GenSDKCommitFromMessage(ctx, &modulev1.Commit{})
		require.NoError(t, err)
		assert.Empty(t, commit)
		assert.Equal(t, 1, callCount)
	})
	t.Run("WithoutCommitResolution wins over WithCommitResolver", func(t *testing.T) {
		t.Parallel()
		var called bool
		resolver := func(_ context.Context, _ proto.Message, _ string) (string, error) {
			called = true
			return newCommitID(), nil
		}
		serdeInstance := New("buf.build",
			serde.WithCommitResolver(resolver),
			serde.WithoutCommitResolution(),
		)
		commit, err := serdeInstance.GenSDKCommitFromMessage(ctx, &modulev1.Commit{})
		require.NoError(t, err)
		assert.Empty(t, commit)
		assert.False(t, called)
	})
	t.Run("WithStaticModuleCommits returns mapped commit", func(t *testing.T) {
		t.Parallel()
		want := newCommitID()
		serdeInstance := New("buf.build", serde.WithStaticModuleCommits(map[string]string{
			"buf.build/bufbuild/registry": want,
		}))
		serdeInstance.genSDKDeps = []genSDKDep{newGenSDKDep(t,
			"buf.build/gen/go/bufbuild/registry/protocolbuffers/go",
			"v1.36.11-20260126144947-819582968857.1",
		)}
		commit, err := serdeInstance.GenSDKCommitFromMessage(ctx, &modulev1.Commit{})
		require.NoError(t, err)
		assert.Equal(t, want, commit)
	})
	t.Run("WithStaticModuleCommits missing key returns empty", func(t *testing.T) {
		t.Parallel()
		serdeInstance := New("buf.build", serde.WithStaticModuleCommits(map[string]string{
			"buf.build/opentelemetry/opentelemetry": newCommitID(),
		}))
		serdeInstance.genSDKDeps = []genSDKDep{newGenSDKDep(t,
			"buf.build/gen/go/bufbuild/registry/protocolbuffers/go",
			"v1.36.11-20260126144947-819582968857.1",
		)}
		commit, err := serdeInstance.GenSDKCommitFromMessage(ctx, &modulev1.Commit{})
		require.NoError(t, err)
		assert.Empty(t, commit)
	})
	t.Run("WithStaticModuleCommits clones map", func(t *testing.T) {
		t.Parallel()
		want := newCommitID()
		commits := map[string]string{"buf.build/bufbuild/registry": want}
		serdeInstance := New("buf.build", serde.WithStaticModuleCommits(commits))
		serdeInstance.genSDKDeps = []genSDKDep{newGenSDKDep(t,
			"buf.build/gen/go/bufbuild/registry/protocolbuffers/go",
			"v1.36.11-20260126144947-819582968857.1",
		)}
		// Mutating the map after construction must not affect the option.
		clear(commits)
		commit, err := serdeInstance.GenSDKCommitFromMessage(ctx, &modulev1.Commit{})
		require.NoError(t, err)
		assert.Equal(t, want, commit)
	})
}

func TestResolveCommit(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	// ts is the timestamp embedded in the dep version below, used for multi-result disambiguation.
	ts, err := time.Parse("20060102150405", "20260126144947")
	require.NoError(t, err)
	newTestSerde := func(commitID string) (*Serde, *commitHandler) {
		handler := &commitHandler{commitID: commitID, ts: ts}
		server := newCommitServer(t, handler)
		serverURL, err := url.Parse(server.URL)
		require.NoError(t, err)
		return New(serverURL.Host, serde.WithHTTPClient(server.Client())), handler
	}
	// newDep returns a fresh dep with shortCommit overridden so each sub-test can steer the
	// handler to a specific response branch via the IdQuery.
	newDep := func(shortCommit string) genSDKDep {
		dep := newGenSDKDep(t,
			"buf.build/gen/go/bufbuild/registry/protocolbuffers/go",
			"v1.36.11-20260126144947-819582968857.1",
		)
		dep.shortCommit = shortCommit
		return dep
	}

	t.Run("single result", func(t *testing.T) {
		t.Parallel()
		commitID := newCommitID()
		s, _ := newTestSerde(commitID)
		dep := newGenSDKDep(t,
			"buf.build/gen/go/bufbuild/registry/protocolbuffers/go",
			"v1.36.11-20260126144947-819582968857.1",
		)
		commit, err := s.resolveCommit(ctx, dep)
		require.NoError(t, err)
		assert.Equal(t, commitID, commit)
	})
	t.Run("multiple results match timestamp", func(t *testing.T) {
		t.Parallel()
		commitID := newCommitID()
		s, _ := newTestSerde(commitID)
		commit, err := s.resolveCommit(ctx, newDep("multi"))
		require.NoError(t, err)
		assert.Equal(t, commitID, commit)
	})
	t.Run("multiple results no timestamp match", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestSerde(newCommitID())
		_, err := s.resolveCommit(ctx, newDep("nomatch"))
		require.Error(t, err)
	})
	t.Run("no results", func(t *testing.T) {
		t.Parallel()
		s, _ := newTestSerde(newCommitID())
		_, err := s.resolveCommit(ctx, newDep("empty"))
		require.Error(t, err)
	})
	t.Run("cached", func(t *testing.T) {
		t.Parallel()
		commitID := newCommitID()
		serdeInstance, handler := newTestSerde(commitID)
		dep := newGenSDKDep(t,
			"buf.build/gen/go/bufbuild/registry/protocolbuffers/go",
			"v1.36.11-20260126144947-819582968857.1",
		)
		for range 3 {
			commit, err := serdeInstance.resolveCommit(ctx, dep)
			require.NoError(t, err)
			assert.Equal(t, commitID, commit)
		}
		// Only one BSR call should have been made despite three resolveCommit calls.
		assert.Equal(t, int32(1), handler.calls.Load())
	})
	t.Run("retries", func(t *testing.T) {
		t.Parallel()
		commitID := newCommitID()
		s, handler := newTestSerde(commitID)
		commit, err := s.resolveCommit(ctx, newDep("retry"))
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

// newCommitID returns a random 32-char hex identifier matching the BSR commit ID format.
func newCommitID() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}

// newGenSDKDep builds a genSDKDep from a Go module path and pseudo-version, deriving
// the registry/owner/module/short-commit fields via the production parsing helpers.
func newGenSDKDep(t *testing.T, path, version string) genSDKDep {
	t.Helper()
	deps := collectGenSDKDeps([]*debug.Module{{Path: path, Version: version}})
	require.Len(t, deps, 1)
	return deps[0]
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
