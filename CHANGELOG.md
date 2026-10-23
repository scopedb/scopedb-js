# Changelog

All significant changes to the ScopeDB JavaScript SDK are documented in this file.

## Unreleased

### Breaking Changes

* Require ScopeDB 0.6.0 and adopt native Statement, declarative Catalog, and table ingest APIs. Remove transform Ingest, paginated Catalog APIs, the old execution timeout option, and `maxParallelism`; see the [migration guide](README.md#migrate-to-scopedb-06).

* Replace Statement status shortcuts with complete `snapshot()`, `refresh()`, `wait()`, and `cancel()` observations. Use `table.catalog()` for definitions, ETags, and conditional mutations. Remove compatibility aliases documented in the migration table.

* Unknown Append outcomes are no longer replayed by default. Use `.retryUnknown(true)` to explicitly accept possible duplicates. Append results use `status`; `AppendRowsError` uses `outcome`; stream `lastFailure.error` retains the complete diagnostic.

### New Features

* Support complete Catalog replacement/deletion with dry-run, ETags, conditional writes, views, computed columns, and destructive-change controls. Validate known fields while retaining extension fields during read/modify/write.

* Add configurable write deadlines with a 30-second default covering the complete HTTP attempt after local preparation. Set `writeTimeoutMs`, per-request `timeoutMs`, or stream `attemptTimeoutMs` to `null` to disable that deadline; caller cancellation and the stream delivery budget remain effective.

* Preserve structured query errors, unknown error codes, compute seconds, and complete cancellation observations. Confirmed server cancellation reports `StatementCancelled` separately from local request cancellation.

### Bug Fixes

* Prevent concurrent Statement responses from overwriting cached terminal results or allowing caller mutation to change the handle. Reject mismatched response identities.

* Honor server retry refusal, retain unknown Catalog gateway outcomes, and preserve full HTTP and timeout diagnostics in Append delivery reports. Classify records that cannot fit the configured stream buffer separately from temporary backpressure.

* Preserve signed/unsigned integer boundaries, raw JSON text, and special float values. Reject numeric overflow with row/column context and preserve result columns named `__proto__`.

### Improvements

* Target 4 MiB Append batches by default, bounded by the 8 MiB and 200,000-row request limits. Direct appends use the same gzip compression and request limits as streams.

* Keep retry attempts and elapsed time bounded, with jitter and `Retry-After` as a lower bound. Client timeouts and cancellations are never replayed; standard fetch redirect behavior is preserved.

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
