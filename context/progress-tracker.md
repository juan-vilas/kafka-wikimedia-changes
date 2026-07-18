# Progress Tracker

Update this file whenever the current phase, active feature, or implementation state changes.

## Current Phase

- OpenSearch consumer scaffolding; docker-compose for local OpenSearch

## Current Goal

- Set up `kafka-consumer-opensearch` module for future Kafka consumer → OpenSearch sink implementation

## Completed

- `WikimediaChangesProducer` — sends "hello world" to topic `demo_java` via fire-and-forget, sleeps 10 minutes, then closes. Entry point is its own `main` method.
- `WikimediaChangeHandler` — implements `BackgroundEventHandler` (okhttp-eventsource 4.1.0). Constructor takes a `KafkaProducer<String,String>` and topic. `onMessage` logs event data via SLF4J and sends it to Kafka; `onClosed` closes the producer; `onError` logs the throwable; `onOpen`/`onComment` are TODO stubs.
- Added `slf4j-simple` 1.7.36 dependency for compile-scope SLF4J API and runtime log binding.
- Restructured single-module project into multi-module Maven project: root `pom.xml` is now parent POM (`packaging=pom`) with `dependencyManagement` centralizing versions.
- `kafka-producer-wikimedia` submodule — existing producer code (`WikimediaChangesProducer`, `WikimediaChangeHandler`, `Main`) moved here; pom inherits from parent.
- `kafka-consumer-opensearch` submodule — empty module created with only `pom.xml` (inherits from parent, has `kafka-clients` + `slf4j-simple` deps). No Java source yet.
- `kafka-consumer-opensearch/docker-compose.yml` — single-node OpenSearch + OpenSearch Dashboards with security disabled. OpenSearch runs on http://localhost:9200, Dashboards on http://localhost:5601.

## In Progress

- None

## Next Up

- Implement Kafka consumer in `kafka-consumer-opensearch` that reads from the `demo_java` topic and indexes events into OpenSearch

## Architecture Decisions

- Docker: single-node OpenSearch (`discovery.type=single-node`) with security plugin disabled via `DISABLE_SECURITY_PLUGIN=true` + `DISABLE_INSTALL_DEMO_CONFIG=true` (official Docker env-var approach, not `plugins.security.disabled` in opensearch.yml). Dashboards security disabled via `DISABLE_SECURITY_DASHBOARDS_PLUGIN=true`. `OPENSEARCH_HOSTS` uses `http://` (not `https://`). `bootstrap.memory_lock=true` + memlock/nofile ulimits per official docs.

## Open Questions

- None

## Architecture Decisions

- Producer config: `bootstrap.servers=127.0.0.1:9092`, `StringSerializer` for key and value, `compression.type=snappy`, `linger.ms=20`, `batch.size=32768`
- `BOOTSTRAP_SERVERS` and `TOPIC` are `private static final` class constants
- Fire-and-forget send (no callback, no `.get()`)
- Manual `producer.close()` in `finally` block
- 10-minute sleep via `TimeUnit.MINUTES.sleep(10)` with interrupt-restoring catch
- SSE handler interface is `com.launchdarkly.eventsource.background.BackgroundEventHandler` (4.1.0 renamed the pre-4.0 `EventHandler`); `onMessage(String event, MessageEvent)` delivers event data via `messageEvent.getData()`
- Logging via SLF4J `LoggerFactory` with `slf4j-simple` binding

## Session Notes

- `WikimediaChangesProducer` has its own `main`; `Main.java` remains as the future entry point for the full SSE+Kafka app.
