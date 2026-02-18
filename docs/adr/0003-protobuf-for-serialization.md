# 3. Use Protocol Buffers for Cross-Language Serialization

Date: 2026-02-17

## Status

Accepted (retroactive documentation of existing architecture)

## Context

Feast spans multiple languages (Python, Go, Java) and needs a consistent serialization
format for registry objects, feature values, and gRPC service definitions. The format
must support schema evolution, be efficient over the wire, and generate typed client
code for each language.

## Decision

Protocol Buffers (protobuf) is used as the canonical serialization format. Proto
definitions live in `protos/feast/` and are compiled to Python, Go, and Java. gRPC
services are defined in proto files and used for the registry server and feature
serving layer.

## Consequences

- Schema changes are backward-compatible when following proto3 conventions
- All languages share the same data model without manual synchronization
- Proto compilation is required after schema changes (`make protos`)
- Generated code should not be edited by hand
- New contributors need protobuf tooling installed for schema work
