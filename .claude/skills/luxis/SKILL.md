---
name: luxis
description: Use when working with the Luxis web framework for Java. Triggers on imports from `io.kiw.luxis`, usage of `HttpStream`, `JsonHandler`, `JsonFilter`, `Luxis.start`, `Luxis.test`, `RequestPipeline`, `HttpResult`, or when the user asks about building or testing a Luxis application.
---

# Luxis

Luxis is a type-safe, functional web framework built on Vert.x for Java 21. It provides declarative HTTP pipelines, a `Result` monad for error handling, and an in-memory test layer that removes Vert.x entirely from unit tests.

## Core rules

These are non-negotiable and the compiler will enforce most of them. Follow them without exception:

- **Application state** is only accessible inside non-blocking pipeline steps (`map`, `flatMap`, `asyncMap`, `asyncFlatMap`) or when calling a Luxis method directly. Blocking steps (`blockingMap`, `blockingFlatMap`, `asyncBlockingMap`, `asyncBlockingFlatMap`) run on a worker thread and cannot see application state.
- **Blocking I/O** (JDBC, file reads, synchronous HTTP) must use a `blocking*` variant. Never block the event loop.
- **Async I/O** (Vert.x HTTP client, Kafka, anything returning a `Future`) uses `asyncMap` / `asyncFlatMap` — these stay on the event loop.
- **Error handling** in the pipeline uses `HttpResult.error(ErrorStatusCode, body)` returned from a `flatMap` / `blockingFlatMap` / `asyncFlatMap` step. Do not throw for expected error cases — throwing is reserved for unexpected failures and produces a `500`.
- **Validation** uses the declarative `validate(v -> …)` step. It aggregates all failures and short-circuits with a `422`. Do not hand-roll validation in `map` steps.
- **Tests** go through `TestClient`. The same test code works against `StubTestClient` (in-memory, for fast unit tests) and `VertxTestClient` (real HTTP server, for end-to-end). Treat the server as a black box — only assert behaviour observable from the client. The only exception is asserting the exception handler was called.

## Pipeline cheat sheet

```java
return stream
    .validate(v -> {
        v.jsonField("name", r -> r.name).required().minLength(2);
        v.jsonField("email", r -> r.email).required().email();
    })
    .map(this::readFromAppState)                     // event loop, app state OK
    .blockingFlatMap(this::queryDatabase)            // worker thread, can return HttpResult.error
    .asyncMap(this::callDownstreamService)           // event loop, returns Future
    .complete(this::toResponse);                     // terminal, produces the response
```

- `map` / `flatMap` — event loop; `flat*` variants can short-circuit with `HttpResult.error`.
- `blockingMap` / `blockingFlatMap` — worker thread for synchronous I/O; no app state.
- `asyncMap` / `asyncFlatMap` — event loop; returns a `Future`.
- `asyncBlockingMap` / `asyncBlockingFlatMap` — worker thread; returns a `Future`.
- `complete` / `blockingComplete` — terminal step that produces the response.

## Documentation

Full documentation is at **https://isolgpus.github.io/Luxis/**. Before making non-trivial changes, fetch the relevant page with `WebFetch`:

- Getting Started — https://isolgpus.github.io/Luxis/getting-started/
- Handler Pipeline — https://isolgpus.github.io/Luxis/guides/handler-pipeline/
- HTTP Context — https://isolgpus.github.io/Luxis/guides/http-context/
- Validation — https://isolgpus.github.io/Luxis/guides/validation/
- Filters — https://isolgpus.github.io/Luxis/guides/filters/
- Error Handling — https://isolgpus.github.io/Luxis/guides/error-handling/
- File Upload & Download — https://isolgpus.github.io/Luxis/guides/file-upload-download/
- WebSockets — https://isolgpus.github.io/Luxis/guides/websockets/
- HTTP Client — https://isolgpus.github.io/Luxis/guides/http-client/
- Configuration — https://isolgpus.github.io/Luxis/guides/configuration/
- Testing Your Handlers — https://isolgpus.github.io/Luxis/testing/

Treat the published docs as authoritative. If this skill and the docs disagree, the docs win.
