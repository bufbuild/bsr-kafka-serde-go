[![The Buf logo](.github/buf-logo.svg)][buf]

# bsr-kafka-serde-go

[![License](https://img.shields.io/github/license/bufbuild/bsr-kafka-serde-go?color=blue)](https://github.com/bufbuild/bsr-kafka-serde-go/blob/main/LICENSE)
[![CI](https://github.com/bufbuild/bsr-kafka-serde-go/actions/workflows/ci.yaml/badge.svg)](https://github.com/bufbuild/bsr-kafka-serde-go/actions/workflows/ci.yaml)
[![GoDoc](https://pkg.go.dev/badge/github.com/bufbuild/bsr-kafka-serde-go.svg)](https://pkg.go.dev/github.com/bufbuild/bsr-kafka-serde-go)
[![Slack](https://img.shields.io/badge/slack-buf-%23e01563)](https://buf.build/links/slack)

[bsr-kafka-serde-go][bsr-kafka-serde-go] provides a Kafka serializer and deserializer in Go for working with schemas defined in the [Buf Schema Registry][bsr].
It uses the following Kafka record headers to automatically deserialize record values from Protobuf:

* `buf.registry.value.schema.message` - The full name of the Protobuf message stored in the record's value (e.g. `payment.v1alpha1.Order`).
* `buf.registry.value.schema.commit` - The BSR commit ID for the Protobuf message's schema (e.g. `9a877cf260e1488d869a31fce3bea26d`).

These headers are automatically added to records produced to Bufstream when Bufstream is configured to use [semantic validation][bufstream-semantic-validation].
To use the deserializer with other brokers, it is up to producers to write record headers.

## Usage

`bsr-kafka-serde-go` currently supports [`confluent-kafka-go/v2`][confluent-kafka-go/v2] and [`franz-go`][franz-go],
found in the [`confluent`](https://pkg.go.dev/github.com/bufbuild/bsr-kafka-serde-go/confluent) and [`franz`](https://pkg.go.dev/github.com/bufbuild/bsr-kafka-serde-go/franz) packages, respectively.

```console
$ go get github.com/bufbuild/bsr-kafka-serde-go
```

## Examples

### confluent-kafka-go/v2

See [confluent/example/main.go](./confluent/example/main.go).

### franz-go

See [franz/example/main.go](./franz/example/main.go).

## Status: Beta

This module isn't stable yet. However, the final shape is unlikely to change drastically—future edits will be somewhat minor.

## Legal

Offered under the [Apache 2 license][license].

[bsr]: https://buf.build/docs/bsr/
[buf]: https://buf.build
[bufstream-semantic-validation]: https://buf.build/docs/bufstream/semantic-validation/
[bsr-kafka-serde-go]: https://github.com/bufbuild/bsr-kafka-serde-go
[license]: https://github.com/bufbuild/bsr-kafka-serde-go/blob/main/LICENSE
[confluent-kafka-go/v2]: https://pkg.go.dev/github.com/confluentinc/confluent-kafka-go/v2
[franz-go]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo
