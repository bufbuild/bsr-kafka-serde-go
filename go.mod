module github.com/bufbuild/bsr-kafka-serde-go

go 1.26.0

require (
	buf.build/gen/go/bufbuild/registry/connectrpc/gosimple v1.21.1-20261021182822-0b1ad74125ef.1
	buf.build/gen/go/bufbuild/registry/protocolbuffers/go v1.36.12-20261021182822-0b1ad74125ef.2
	buf.build/gen/go/opentelemetry/opentelemetry/protocolbuffers/go v1.36.12-20261002203906-f3a7b1f1162b.2
	connectrpc.com/connect v1.21.1
	github.com/confluentinc/confluent-kafka-go/v2 v2.16.0
	github.com/google/go-cmp v0.7.0
	github.com/segmentio/kafka-go v0.4.51
	github.com/sethvargo/go-retry v0.5.0
	github.com/stretchr/testify v1.12.1
	github.com/twmb/franz-go v1.22.2
	github.com/twmb/franz-go/pkg/kadm v1.19.0
	golang.org/x/sync v0.24.0
	google.golang.org/protobuf v1.36.12
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.12-20260825204119-511051f7f437.2 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
)
