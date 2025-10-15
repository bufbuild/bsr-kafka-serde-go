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

// Package confluent provides a [Serde] interface for serializing and deserializing Bufstream
// messages for usage with [github.com/confluentinc/confluent-kafka-go/v2/kafka].
package confluent

import (
	"context"
	"fmt"
	"time"

	serde "github.com/bufbuild/bsr-kafka-serde-go"
	internalserde "github.com/bufbuild/bsr-kafka-serde-go/internal/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"
)

// Serde provides serialization and deserialization for Bufstream topics that are sent protobuf
// messages from BSR modules.
type Serde interface {
	// Serialize serializes src into a [*kafka.Message] suitable to producing to a topic.
	Serialize(src proto.Message) (*kafka.Message, error)
	// Deserialize deserializes the given message into a [proto.Message], based on the
	// "buf.registry.value.schema.commit" and "buf.registry.value.schema.message"
	// headers of the message.
	Deserialize(ctx context.Context, message *kafka.Message) (proto.Message, error)
	// DeserializeTo deserializes the given message into dest. It checks to ensure that message has the
	// "buf.registry.value.schema.message" header set, and that the header value matches the
	// fully-qualified name of dest.
	DeserializeTo(message *kafka.Message, dest proto.Message) error
}

// New creates a new [Serde] for deserializing [proto.Message] values from [kafka.Message]s.
func New(host string, options ...serde.Option) Serde {
	return &confluentSerde{
		serde: internalserde.New(host, options...),
	}
}

type confluentSerde struct {
	serde *internalserde.Serde
}

// Serialize serializes src into a [kafka.Message] suitable to producing to a topic.
func (s *confluentSerde) Serialize(src proto.Message) (*kafka.Message, error) {
	value, err := proto.Marshal(src)
	if err != nil {
		return nil, fmt.Errorf("serializing proto: %w", err)
	}
	return &kafka.Message{
		Value: value,
	}, nil
}

// Deserialize deserializes the given message into a [proto.Message], based on the
// "buf.registry.value.schema.commit" and "buf.registry.value.schema.message" headers of the
// message.
func (s *confluentSerde) Deserialize(ctx context.Context, message *kafka.Message) (proto.Message, error) {
	var (
		commit     string
		messageFQN string
	)
	for _, header := range message.Headers {
		if header.Key == internalserde.BufRegistryValueSchemaCommit {
			commit = string(header.Value)
		}
		if header.Key == internalserde.BufRegistryValueSchemaMessage {
			messageFQN = string(header.Value)
		}
	}
	return s.serde.Deserialize(ctx, message.Value, commit, messageFQN, time.Now)
}

// DeserializeTo deserializes the given message into dest. It checks to ensure that message has the
// "buf.registry.value.schema.message" header set, and that the header value matches the
// fully-qualified name of dest.
func (s *confluentSerde) DeserializeTo(message *kafka.Message, dest proto.Message) error {
	var messageFQN string
	for _, header := range message.Headers {
		if header.Key == internalserde.BufRegistryValueSchemaMessage {
			messageFQN = string(header.Value)
			break
		}
	}
	if messageFQN == "" {
		return fmt.Errorf("message does not have %q header set", internalserde.BufRegistryValueSchemaMessage)
	}
	if destFQN := string(dest.ProtoReflect().Descriptor().FullName()); messageFQN != destFQN {
		return fmt.Errorf(
			"message header %q value %q does not match fully qualified name of %q",
			internalserde.BufRegistryValueSchemaMessage,
			messageFQN,
			destFQN,
		)
	}
	return proto.Unmarshal(message.Value, dest)
}
