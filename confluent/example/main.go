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
	"time"

	demov1 "demo.buf.dev/gen/go/bufbuild/bufstream-demo/protocolbuffers/go/bufstream/demo/v1"
	"github.com/bufbuild/bsr-kafka-serde-go/confluent"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Printf("error: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	// Assuming bufstream running locally...
	configMap := &kafka.ConfigMap{"bootstrap.servers": "localhost:9092"}
	adminClient, err := kafka.NewAdminClient(configMap)
	if err != nil {
		return fmt.Errorf("creating admin client: %w", err)
	}
	var topic = "demo"
	if _, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
			Config: map[string]string{
				"buf.registry.value.schema.module":  "demo.buf.dev/bufbuild/bufstream-demo",
				"buf.registry.value.schema.message": "bufstream.demo.v1.EmailUpdated",
				"bufstream.validate.mode":           "reject",
			},
		},
	}); err != nil {
		return fmt.Errorf("creating topic: %w", err)
	}

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		return fmt.Errorf("creating producer: %w", err)
	}
	defer producer.Close()

	if err := configMap.Set("group.id=test"); err != nil {
		return fmt.Errorf("setting group.id: %w", err)
	}
	if err := configMap.Set("auto.offset.reset=earliest"); err != nil {
		return fmt.Errorf("setting auto.offset.reset: %w", err)
	}
	consumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		return fmt.Errorf("creating consumer: %w", err)
	}
	defer consumer.Close()

	serde := confluent.New("demo.buf.dev")
	message, err := serde.Serialize(&demov1.EmailUpdated{
		Id:              uuid.New().String(),
		OldEmailAddress: "test@example.com",
		NewEmailAddress: "new@example.com",
	})
	if err != nil {
		return fmt.Errorf("serializing email updated: %w", err)
	}
	message.TopicPartition.Topic = &topic

	if err := producer.Produce(message, nil); err != nil {
		return fmt.Errorf("producing: %w", err)
	}
	producer.Flush(1000)

	if err := consumer.Subscribe(topic, nil); err != nil {
		return fmt.Errorf("subscribing to topic: %w", err)
	}
	receivedMessage, err := consumer.ReadMessage(10 * time.Second)
	if err != nil {
		return fmt.Errorf("reading message: %w", err)
	}
	emailUpdated, err := serde.Deserialize(ctx, receivedMessage)
	if err != nil {
		return fmt.Errorf("deserializing payload: %w", err)
	}
	fmt.Printf("Consumed value: %+v\n", emailUpdated)

	return nil
}
