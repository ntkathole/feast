# 1. Use Architecture Decision Records

Date: 2026-02-18

## Status

Accepted

## Context

Feast is a large, multi-language open source project with many architectural decisions
made over time across Python SDK, Go feature server, Java serving, Kubernetes operator,
and various storage backends. New contributors and AI coding agents need to understand
the rationale behind key design choices without reading through years of GitHub issues
and pull request discussions.

## Decision

We will use Architecture Decision Records (ADRs), to
document significant architectural decisions. ADRs will be stored in `docs/adr/` and
numbered sequentially (0001, 0002, ...).

Each ADR will include:
- **Status**: Proposed, Accepted, Deprecated, or Superseded
- **Context**: The forces at play and the problem being addressed
- **Decision**: The change being made
- **Consequences**: What becomes easier or harder as a result

## Consequences

- Architectural decisions are documented alongside the code they affect
- Future contributors can understand why things are built the way they are
- AI agents can consume ADRs as structured context for making changes
- ADRs are lightweight, version-controlled, and reviewable via normal PR process
- Existing decisions should be retroactively documented as time permits
