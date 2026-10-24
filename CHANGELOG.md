# Changelog

All significant changes to the ScopeDB JavaScript SDK are documented in this file.

## Unreleased

### Bug Fixes

* Preserve known statement IDs on SDK errors so callers can resume waiting with `statementHandle(id)`. After successful submission, `query()` and `execute()` no longer mark waiting failures as retryable; handle polling keeps its temporary error classification, and caller abort reasons remain unchanged.

## v0.3.0 (2026-10-23)

### Breaking Changes

* Replace legacy Ingest APIs with Append, which accepts rows matching the destination columns. Move transformations into producer code or a separate ScopeQL query; see [Upgrade from 0.2 to 0.3](docs/migration-0.3.md).
* Replace paginated Catalog methods with `client.catalog()` and `table.catalog()`. Replace Statement status helpers with complete observations through `snapshot()`, `refresh()`, `wait()`, and `cancel()`.
* Remove deprecated compatibility aliases and `withMaxParallelism()`. Replace `withExecTimeout()` with `withMaxElapsedSeconds()` and optional `withMaxEstimatedSeconds()`. Append results now use `status`, errors use `outcome`, and stream failure details are available through `stats().lastFailure.error`.
* Apply a 30-second default deadline to Append attempts and Catalog mutations, with a separate five-minute per-batch delivery budget for streams. Configure `writeTimeoutMs`, request `timeoutMs`, stream `attemptTimeoutMs`, and `maxElapsedTimeMs` as needed; `null` disables the per-attempt deadline.

### New Features

* Support declarative Catalog replacement and deletion with dry-run, ETags, conditional mutations, views, computed columns, and destructive-change controls. Preserve extension fields during read/modify/write.
* Add explicit `.retryUnknown(true)` support for callers willing to accept possible duplicate writes. Unknown outcomes remain non-retryable by default.
* Expose structured server errors, compute usage, and write interruption details. Distinguish confirmed server cancellation from aborting local waiting.

### Bug Fixes

* Keep Statement snapshots monotonic and isolated from caller mutation. Preserve terminal results and caller cancellation reasons, and mark unacknowledged submissions as non-retryable.
* Carry cancelled flush outcomes into the next delivery report without double counting, while retaining complete HTTP and timeout diagnostics.
* Validate numeric ranges and malformed result cells with row/column diagnostics. Handle special float values, preserve columns named `__proto__`, and reject duplicate column names during object conversion.
* Enforce the 64 KiB request-body limit in Next.js and Worker templates, including streamed bodies, and reject unsafe JSON numbers before Append.

### Improvements

* Use a 4 MiB default Append batch target. Compress direct appends with gzip and enforce the same 8 MiB and 200,000-row request limits as streams.
* Bound retries by count and elapsed time, add jitter, and honor `Retry-After` and server retry refusal. Client timeouts and cancellations are never replayed.

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
