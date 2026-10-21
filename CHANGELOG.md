# Changelog

All significant changes to the ScopeDB JavaScript SDK are documented in this file.

## Unreleased

### Breaking Changes

* Removed the statement `maxParallelism` option. Execution timeout remains the only exposed statement execution control.
* Failed transform-ingest requests no longer trigger automatic retries that could duplicate committed rows. Transport failures and invalid success responses report an unknown commit outcome and preserve response diagnostics; reconcile or retry deliberately instead of relying on automatic replay.

### New Features

* Support ScopeDB 0.5.28 and 0.6 with legacy HTTP endpoints through the same client API. Preserve complete 0.6 cancellation results and fetch missing result or failure details after short 0.5 cancellation responses.
* Expose server error codes as `ScopeDBError.code`, including unknown codes.
* Table append and transform-ingest requests now have a configurable `ClientOptions.writeTimeoutMs`, defaulting to 30 seconds including response-body reads and starting after local encoding, compression, and request preparation. Direct writes accept `WriteOptions.timeoutMs`; streams inherit the client timeout unless an append stream explicitly sets `attemptTimeoutMs()`. Timeout and in-flight cancellation report a `timeout` or `cancelled` reason with an `unknown` outcome through `ScopeDBError.writeDetails`, preserving existing kinds, causes, HTTP diagnostics, and subsequent stream errors, and are never automatically retried, regardless of the caller's abort reason.

### Bug Fixes

* Preserve HTTP status, request ID, `Retry-After`, and the original cause when reading an error response body fails. Append streams no longer retry known authentication failures as transport failures in this case.

### Improvements

* Append streams retry transient unknown outcomes other than client timeouts or cancellations with bounded elapsed time and jitter, honor `Retry-After` as a lower bound, and default every attempt to 30 seconds. Replays may duplicate committed rows.
* `AppendStream` now targets 4 MiB per batch by default; the maximum configurable target and uncompressed request limit remain 8 MiB.
* Direct caller-encoded table appends now use the same gzip compression and 8 MiB uncompressed request limit as `AppendStream` batches.
* `IngestStream` now closes admission synchronously during idempotent shutdown, flushes from the first-row deadline under continuous traffic, and seals a batch before the next record would exceed its target size.
* Ingest streams preserve terminal failure diagnostics in subsequent `send()`, `flush()`, and `shutdown()` calls, and failed-stream flush waits now honor cancellation while requests finish settling.
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
