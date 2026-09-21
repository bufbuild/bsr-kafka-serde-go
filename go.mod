module github.com/bufbuild/bsr-kafka-serde-go

go 1.26.0

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.12-20260825204119-511051f7f437.2
	buf.build/gen/go/bufbuild/registry/connectrpc/gosimple v1.21.0-20260831211851-b4432e12a6e7.1
	buf.build/gen/go/bufbuild/registry/protocolbuffers/go v1.36.12-20260831211851-b4432e12a6e7.2
	buf.build/gen/go/opentelemetry/opentelemetry/protocolbuffers/go v1.36.12-20260722205108-41ecbd638b41.2
	connectrpc.com/connect v1.21.0
	github.com/confluentinc/confluent-kafka-go/v2 v2.15.1
	github.com/google/go-cmp v0.7.0
	github.com/segmentio/kafka-go v0.4.51
	github.com/sethvargo/go-retry v0.4.0
	github.com/stretchr/testify v1.12.1
	github.com/twmb/franz-go v1.22.0
	github.com/twmb/franz-go/pkg/kadm v1.19.0
	golang.org/x/sync v0.23.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.30 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.14.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
)
