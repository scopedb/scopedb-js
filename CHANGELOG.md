# Changelog

## Unreleased

## 0.2.2 - 2026-08-22

### Changed

- `AppendStream` now caps each uncompressed NDJSON request at 8 MiB.

## 0.2.1 - 2026-08-20

### Changed

- JSON request bodies and `AppendStream` batches now use gzip compression by
  default. Direct caller-encoded table appends remain identity-encoded.

## 0.2.0 - 2026-08-06

### Added

- `ClientOptions.apiKey` for server-side API-key authentication.
- Named table locations through `Client.table(name, { database, schema })`.
- Automatic database, schema, and table catalog iterators.
- `Table.describe()` with JavaScript-style metadata field names.
- `ResultSet.rawRows()`, `toValues()`, and `toObjects()` aliases.
- `StatementHandle.lastStatus()` and `wait()` lifecycle methods.
- `WaitOptions` for statement polling configuration.
- Clearer streaming-write configuration names and configurable row batching.
- HTTP status, request ID, retryability, and `Retry-After` diagnostics on
  `ScopeDBError`.
- Runtime templates for Next.js Route Handlers and Cloudflare Workers.

### Changed

- Streaming writes now reject non-object top-level JSON values locally.
- Requests opt out of framework fetch caches.
- Safe append retries honor `Retry-After` up to the configured maximum backoff.
- Ambiguous append responses retain HTTP and request metadata for reconciliation.
- Fetch transports are invoked without an arbitrary receiver, including in
  workerd-based runtimes.
- The SDK source no longer requires Node.js `Buffer`, allowing Web-standard
  server runtimes to use streaming writes without Node compatibility shims.
- Examples use `SCOPEDB_API_KEY` and the new application-facing API names.
- **Breaking:** `StatementHandle.status()` now asynchronously requests the
  latest remote status. Use `lastStatus()` for the synchronous local snapshot.

### Deprecated

- `ClientOptions.token` in favor of `apiKey`.
- Mutable table location builders in favor of options passed to
  `Client.table()`.
- `ResultSet.jsonRows()`, `intoValues()`, and `intoObjects()` in favor of their
  JavaScript-style aliases.
- `StatementHandle.fetchOnce()` and `fetch()` in favor of `status()` and
  `wait()`.
- `FetchOptions` in favor of `WaitOptions`.
- Ambiguous streaming-write configuration names in favor of names that include
  their units or operational meaning.
