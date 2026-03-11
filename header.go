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

// Headers used to serialize and deserialize Kafka records using the BSR.
const (
	// BufRegistryValueSchemaMessage is the Kafka record header name for the fully qualified
	// Protobuf message name (e.g. "payment.v1alpha1.Order").
	// Set by the serializer from the message descriptor, and read by the deserializer to
	// determine the type of the record's value.
	// Bufstream also sets this header when configured to use semantic validation.
	BufRegistryValueSchemaMessage = "buf.registry.value.schema.message"
	// BufRegistryValueSchemaCommit is the Kafka record header name for the BSR commit ID.
	// Set by the serializer by resolving the generated SDK module's pseudo-version against the
	// BSR API, and read by the deserializer to look up the schema from the BSR.
	// Bufstream also sets this header when configured to use semantic validation.
	// Use [WithoutCommitResolution] to disable automatic commit resolution in the serializer.
	BufRegistryValueSchemaCommit = "buf.registry.value.schema.commit"
)
