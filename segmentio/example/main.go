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
	"net"
	"os"
	"strconv"

	logsv1 "buf.build/gen/go/opentelemetry/opentelemetry/protocolbuffers/go/opentelemetry/proto/logs/v1"
	"github.com/bufbuild/bsr-kafka-serde-go/segmentio"
	kafka "github.com/segmentio/kafka-go"
)

const (
	brokerAddr = "localhost:9092"
	topic      = "demo"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Printf("error: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	if err := createTopic(ctx); err != nil {
		return fmt.Errorf("creating topic: %w", err)
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddr),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	serde := segmentio.New("buf.build")
	message, err := serde.Serialize(ctx, &logsv1.LogRecord{
		SeverityText: "INFO",
		EventName:    "demo",
	})
	if err != nil {
		return fmt.Errorf("serializing log record: %w", err)
	}
	if err := writer.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("writing message: %w", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddr},
		Topic:   topic,
		GroupID: "test",
	})
	defer reader.Close()

	received, err := reader.ReadMessage(ctx)
	if err != nil {
		return fmt.Errorf("reading message: %w", err)
	}
	logRecord, err := serde.Deserialize(ctx, received)
	if err != nil {
		return fmt.Errorf("deserializing payload: %w", err)
	}
	fmt.Printf("Consumed value: %+v\n", logRecord)
	return nil
}

func createTopic(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", brokerAddr)
	if err != nil {
		return fmt.Errorf("dialing broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("getting controller: %w", err)
	}
	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dialing controller: %w", err)
	}
	defer controllerConn.Close()

	return controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: "buf.registry.value.schema.module", ConfigValue: "buf.build/opentelemetry/opentelemetry"},
			{ConfigName: "buf.registry.value.schema.message", ConfigValue: "opentelemetry.proto.logs.v1.LogRecord"},
			{ConfigName: "bufstream.validate.mode", ConfigValue: "reject"},
		},
	})
}
