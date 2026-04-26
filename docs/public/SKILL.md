---
name: luxis-mysql
description: Use when working with the luxis-mysql library — a MySQL `TransactionManager` implementation for the Luxis web framework, backed by vertx-mysql-client. Triggers on imports from `io.kiw.luxis.mysql`, references to `MySqlTransactionManager`, or when the user asks about wiring MySQL transactions into a Luxis pipeline.
---

# luxis-mysql

`luxis-mysql` provides an implementation of `io.kiw.luxis.web.TransactionManager` that uses [vertx-mysql-client](https://vertx.io/docs/vertx-mysql-client/java/) for fully async, non-blocking access to MySQL. It is a thin glue library on top of:

- **Luxis core** (`io.kiw.luxis:luxis`) — the `TransactionManager<TX>` SPI and pipeline integration.
- **vertx-mysql-client** (`io.vertx:vertx-mysql-client`) — async MySQL pool and connection types.

The library has no other runtime dependencies, and it does not introduce JDBC. All I/O stays on the Vert.x event loop.

## Core rules

- **Async only.** The implementation returns Vert.x `Future`s. Use it from `asyncMap` / `asyncFlatMap` pipeline steps in Luxis — never from `blockingMap` / `blockingFlatMap`. JDBC has no place here.
- **One transaction per request.** A `TX` value represents a single in-flight MySQL transaction bound to a single connection. Do not share a `TX` across requests, threads, or pipelines.
- **Commit / rollback is owned by Luxis.** When wired up via `.inTransaction`, Luxis calls `begin`, `commit`, and `rollback` on the manager — application code should not call them directly. `onCommitted` is the supported hook for post-commit side effects (publishing events, cache invalidation, etc.).
- **Don't add dependencies casually.** This library is intentionally narrow: Luxis core + vertx-mysql-client. Anything else (connection-pool wrappers, migration tools, ORMs) belongs in the application, not here.

## Relationship to Luxis core

This project depends on Luxis core as a normal Maven artifact (`io.kiw.luxis:luxis`). Treat the Luxis API as fixed from this side — if a change is needed in the `TransactionManager` SPI, make it in the Luxis repo first, release, and then bump the dependency here. Do not vendor or fork Luxis types.

For Luxis itself (pipelines, validation, filters, testing), consult the Luxis SKILL or the published docs at **https://isolgpus.github.io/Luxis/**.

## Build & test

```bash
mvn test
```

> **Cloud mode:** Do NOT run `mvn test` / `mvn verify` / `mvn integration-test` when `CLAUDE_CODE_REMOTE=1` — Maven cannot download dependencies in the restricted cloud environment. Tests will run via CI when a PR is raised.

## Project structure

- `src/main/java/io/kiw/luxis/mysql/` — `MySqlTransactionManager` and supporting types.
- `src/test/java/io/kiw/luxis/mysql/` — unit tests. Integration tests that need a real MySQL should be gated so they don't run in cloud mode.
