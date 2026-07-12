# Progress Tracker

Update this file whenever the current phase, active feature, or implementation state changes.

## Current Phase

- SSE consumer integration

## Current Goal

- Wire the Wikimedia recentchanges SSE stream into the Kafka producer via an event handler

## Completed

- `WikimediaChangesProducer` — sends "hello world" to topic `demo_java` via fire-and-forget, sleeps 10 minutes, then closes. Entry point is its own `main` method.
- `WikimediaChangeHandler` — implements `BackgroundEventHandler` (okhttp-eventsource 4.1.0). Constructor takes a `KafkaProducer<String,String>` and topic. `onMessage` logs event data via SLF4J and sends it to Kafka; `onClosed` closes the producer; `onError` logs the throwable; `onOpen`/`onComment` are TODO stubs.
- Added `slf4j-simple` 1.7.36 dependency for compile-scope SLF4J API and runtime log binding.

## In Progress

- None

## Next Up

- Build the `EventSource` connecting to the Wikimedia recentchanges stream and register `WikimediaChangeHandler`
- Replace hardcoded "hello world" with SSE consumer reading Wikimedia recentchanges stream

## Open Questions

- None

## Architecture Decisions

- Producer config: `bootstrap.servers=127.0.0.1:9092`, `StringSerializer` for key and value
- `BOOTSTRAP_SERVERS` and `TOPIC` are `private static final` class constants
- Fire-and-forget send (no callback, no `.get()`)
- Manual `producer.close()` in `finally` block
- 10-minute sleep via `TimeUnit.MINUTES.sleep(10)` with interrupt-restoring catch
- SSE handler interface is `com.launchdarkly.eventsource.background.BackgroundEventHandler` (4.1.0 renamed the pre-4.0 `EventHandler`); `onMessage(String event, MessageEvent)` delivers event data via `messageEvent.getData()`
- Logging via SLF4J `LoggerFactory` with `slf4j-simple` binding

## Session Notes

- `WikimediaChangesProducer` has its own `main`; `Main.java` remains as the future entry point for the full SSE+Kafka app.
