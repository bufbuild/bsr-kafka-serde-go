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
	"context"
	"fmt"
	"os"

	demov1 "demo.buf.dev/gen/go/bufbuild/bufstream-demo/protocolbuffers/go/bufstream/demo/v1"
	"github.com/bufbuild/bsr-kafka-serde-go/franz"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const topic = "demo"

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Printf("error: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	var (
		module  = "demo.buf.dev/bufbuild/bufstream-demo"
		message = "bufstream.demo.v1.EmailUpdated"
		mode    = "reject"
	)
	client, err := kgo.NewClient(
		// Assuming bufstream running locally...
		kgo.SeedBrokers("localhost:9092"),
		kgo.DefaultProduceTopic(topic),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}
	defer client.Close()
	adminClient := kadm.NewClient(client)
	if _, err := adminClient.CreateTopic(ctx, 1, 1, map[string]*string{
		"buf.registry.value.schema.module":  &module,
		"buf.registry.value.schema.message": &message,
		"bufstream.validate.mode":           &mode,
	}, topic); err != nil {
		return fmt.Errorf("creating topic: %w", err)
	}

	serde := franz.New("demo.buf.dev")
	record, err := serde.Serialize(&demov1.EmailUpdated{
		Id:              uuid.New().String(),
		OldEmailAddress: "test@example.com",
		NewEmailAddress: "new@example.com",
	})
	if err != nil {
		return fmt.Errorf("serializing email updated: %w", err)
	}
	results := client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("producing: %w", err)
	}

	fs := client.PollFetches(ctx)
	fs.EachRecord(func(r *kgo.Record) {
		emailUpdated, err := serde.Deserialize(ctx, r)
		if err != nil {
			fmt.Printf("deserializing record: %v", err)
		} else {
			fmt.Printf("Consumed value: %+v\n", emailUpdated)
		}
	})
	return nil
}
