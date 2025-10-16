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

// Package franz provides a [Serde] interface for serializing and deserializing Bufstream messages
// for usage with [github.com/twmb/franz-go/pkg/kgo].
package franz

import (
	"context"
	"fmt"
	"time"

	serde "github.com/bufbuild/bsr-kafka-serde-go"
	internalserde "github.com/bufbuild/bsr-kafka-serde-go/internal/serde"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Serde provides serialization and deserialization for Bufstream topics that are sent protobuf
// messages from BSR modules.
type Serde interface {
	// Serialize serializes src into a [*kgo.Record] suitable to producing to a topic.
	Serialize(src proto.Message) (*kgo.Record, error)
	// Deserialize deserializes the given record into a [proto.Message], based on the
	// "buf.registry.value.schema.commit" and "buf.registry.value.schema.message"
	// headers of the message.
	Deserialize(ctx context.Context, record *kgo.Record) (proto.Message, error)
	// DeserializeTo deserializes the given record into dest. It checks to ensure that record has the
	// "buf.registry.value.schema.message" header set, and that the header value matches the
	// fully-qualified name of dest.
	DeserializeTo(record *kgo.Record, dest proto.Message) error
}

// New creates a new [Serde] for serializing and deserializing [proto.Message] values to and from
// [kgo.Record] values.
func New(host string, options ...serde.Option) Serde {
	return &franzSerde{
		serde: internalserde.New(host, options...),
	}
}

type franzSerde struct {
	serde *internalserde.Serde
}

// Serialize serializes src into a [*kgo.Record] suitable to producing to a topic.
func (s *franzSerde) Serialize(src proto.Message) (*kgo.Record, error) {
	value, err := proto.Marshal(src)
	if err != nil {
		return nil, fmt.Errorf("serializing proto: %w", err)
	}
	return &kgo.Record{
		Value: value,
	}, nil
}

// Deserialize deserializes the given record into a [proto.Message], based on the
// "buf.registry.value.schema.commit" and "buf.registry.value.schema.message" headers of the
// message.
func (s *franzSerde) Deserialize(ctx context.Context, record *kgo.Record) (proto.Message, error) {
	var (
		commit     string
		messageFQN string
	)
	for _, header := range record.Headers {
		if header.Key == internalserde.BufRegistryValueSchemaCommit {
			commit = string(header.Value)
		}
		if header.Key == internalserde.BufRegistryValueSchemaMessage {
			messageFQN = string(header.Value)
		}
	}
	return s.serde.Deserialize(ctx, record.Value, commit, messageFQN, time.Now)
}

// DeserializeTo deserializes the given record into dest. It checks to ensure that record has the
// "buf.registry.value.schema.message" header set, and that the header value matches the
// fully-qualified name of dest.
func (s *franzSerde) DeserializeTo(record *kgo.Record, dest proto.Message) error {
	var messageFQN string
	for _, header := range record.Headers {
		if header.Key == internalserde.BufRegistryValueSchemaMessage {
			messageFQN = string(header.Value)
			break
		}
	}
	if messageFQN == "" {
		return fmt.Errorf("record does not have %q header set", internalserde.BufRegistryValueSchemaMessage)
	}
	if destFQN := string(dest.ProtoReflect().Descriptor().FullName()); messageFQN != destFQN {
		return fmt.Errorf(
			"record header %q value %q does not match fully qualified name of %q",
			internalserde.BufRegistryValueSchemaMessage,
			messageFQN,
			destFQN,
		)
	}
	return proto.Unmarshal(record.Value, dest)
}
