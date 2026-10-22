# Changelog

All significant changes to the ScopeDB JavaScript SDK are documented in this file.

## Unreleased

### Breaking Changes

* Require ScopeDB 0.6.0 and use native Statement, declarative Catalog, and table ingest APIs. Replace paginated Catalog APIs and the legacy execution timeout option; cancellation retains complete terminal responses.

* Removed `Client.insert()`, `Client.ingest()`, `Client.ingestStream()`, the ingest stream exports, and ingest protocol types. Use table `append()` or `appendStream()` and shape rows before sending them. See the [migration guide](README.md#migrate-from-ingest) for transformations, batch limits, delivery reports, concurrency, and retries.

* Removed the statement `maxParallelism` option. Elapsed and estimated duration limits replace the old execution timeout option.

### New Features

* Add complete Catalog replacements and deletion with dry-run, ETags, conditional updates, views, computed columns, and explicit destructive-change authorization. Preserve structured error codes/details and statement compute seconds.

* Table append requests now have a configurable `ClientOptions.writeTimeoutMs`, defaulting to 30 seconds including response-body reads and starting after local encoding, compression, and request preparation. Direct writes accept `WriteOptions.timeoutMs`; streams inherit the client timeout unless an append stream explicitly sets `attemptTimeoutMs()`. Timeout and in-flight cancellation report a `timeout` or `cancelled` reason with an `unknown` outcome through `ScopeDBError.writeDetails`, preserving existing kinds, causes, HTTP diagnostics, and subsequent stream errors, and are never automatically retried, regardless of the caller's abort reason.

### Bug Fixes

* Preserve HTTP status, request ID, `Retry-After`, and the original cause when reading an error response body fails. Append streams no longer retry known authentication failures as transport failures in this case.

### Improvements

* Append streams retry transient unknown outcomes other than client timeouts or cancellations with bounded elapsed time and jitter, honor `Retry-After` as a lower bound, and default every attempt to 30 seconds. Replays may duplicate committed rows.
* `AppendStream` now targets 4 MiB per batch by default; the maximum configurable target and uncompressed request limit remain 8 MiB.
* Direct caller-encoded table appends now use the same gzip compression and 8 MiB uncompressed request limit as `AppendStream` batches.
* Preserve structured failed-statement details, validate successful statement and catalog responses at runtime, reject duplicate result column names in object conversions, and treat committed append row-count mismatches as an unknown commit outcome.

## v0.2.2 (2026-08-22)

### Improvements

* `AppendStream` now caps each uncompressed NDJSON request at 8 MiB.

## v0.2.1 (2026-08-20)

### Improvements

* JSON request bodies and `AppendStream` batches now use gzip compression by default. Direct caller-encoded table appends remain identity-encoded.

## v0.2.0 (2026-08-06)

### Breaking Changes

* `StatementHandle.status()` now asynchronously requests the latest remote status. Use `lastStatus()` for the synchronous local snapshot.

### New Features

* `ClientOptions.apiKey` for server-side API-key authentication.
* Named table locations through `Client.table(name, { database, schema })`.
* Automatic database, schema, and table catalog iterators.
* `Table.describe()` with JavaScript-style metadata field names.
* `ResultSet.rawRows()`, `toValues()`, and `toObjects()` aliases.
* `StatementHandle.lastStatus()` and `wait()` lifecycle methods.
* `WaitOptions` for statement polling configuration.
* Clearer streaming-write configuration names and configurable row batching.
* HTTP status, request ID, retryability, and `Retry-After` diagnostics on `ScopeDBError`.
* Runtime templates for Next.js Route Handlers and Cloudflare Workers.

### Improvements

* Streaming writes now reject non-object top-level JSON values locally.
* Requests opt out of framework fetch caches.
* Safe append retries honor `Retry-After` up to the configured maximum backoff.
* Ambiguous append responses retain HTTP and request metadata for reconciliation.
* Fetch transports are invoked without an arbitrary receiver, including in workerd-based runtimes.
* The SDK source no longer requires Node.js `Buffer`, allowing Web-standard server runtimes to use streaming writes without Node compatibility shims.
* Examples use `SCOPEDB_API_KEY` and the new application-facing API names.
* Deprecated `ClientOptions.token` in favor of `apiKey`; mutable table location builders in favor of options passed to `Client.table()`; `ResultSet.jsonRows()`, `intoValues()`, and `intoObjects()` in favor of their JavaScript-style aliases; `StatementHandle.fetchOnce()` and `fetch()` in favor of `status()` and `wait()`; `FetchOptions` in favor of `WaitOptions`; and ambiguous streaming-write configuration names in favor of names that include their units or operational meaning. All deprecated names remain available as compatibility aliases.
