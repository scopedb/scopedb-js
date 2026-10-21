# ScopeDB SDK for JavaScript

[![Version](https://img.shields.io/npm/v/scopedb)](https://www.npmjs.com/package/scopedb) [![Docs](https://img.shields.io/badge/docs-scopedb.io-blue)](https://www.scopedb.io/docs/developer/nodejs) [![CI](https://img.shields.io/github/actions/workflow/status/scopedb/scopedb-js/ci.yml?branch=main&label=CI)](https://github.com/scopedb/scopedb-js/actions/workflows/ci.yml) [![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://www.apache.org/licenses/LICENSE-2.0)

The official server-side JavaScript client for [ScopeDB](https://www.scopedb.io), with TypeScript support. Run ScopeQL queries, browse the catalog, and write rows with bounded asynchronous streams. For application writes, start with `table.appendStream()`.

The package is ESM-only and has no runtime dependencies.

## Runtime support

| Environment        | Recommended usage                                                                                |
| ------------------ | ------------------------------------------------------------------------------------------------ |
| Node.js 20+        | Native ESM; Node 20 is a compatibility floor, so use a maintained Node LTS release in production |
| Next.js            | Route Handlers, Server Actions, and other `server-only` modules; prefer the Node runtime         |
| Bun                | Use the same ESM API and `bun add scopedb`; no Bun-specific adapter                              |
| Cloudflare Workers | Use secret bindings and Web APIs; `nodejs_compat` is not required                                |
| CommonJS           | Load the ESM package with dynamic `import()`                                                     |

Do not use this SDK from browser code, Next.js Client Components, or other untrusted clients. A ScopeDB API key grants server access and must not be included in a browser bundle. The [Next.js](examples/frameworks/nextjs-route-handler/route.ts) and [Cloudflare Worker](examples/frameworks/cloudflare-worker/worker.ts) templates show the intended boundary.

## Installation

```sh
npm install scopedb
# or: pnpm add scopedb
# or: bun add scopedb
```

## Quickstart

Set `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` using the [connection guide](https://www.scopedb.io/docs/guides/connect-to-scopedb). Save this program as `connect.mjs` and run `node connect.mjs`:

```js
import { Client } from "scopedb";

const endpoint = process.env.SCOPEDB_ENDPOINT;
const apiKey = process.env.SCOPEDB_API_KEY;
if (!endpoint || !apiKey) {
  throw new Error("Set SCOPEDB_ENDPOINT and SCOPEDB_API_KEY.");
}

const client = new Client(endpoint, { apiKey });
const result = await client.query("SELECT 1 AS ready");
console.log(result.toObjects());
```

## Documentation

- [SDK guide](https://www.scopedb.io/docs/developer/nodejs)
- [Examples](examples/README.md)
- [Changelog](CHANGELOG.md)
- [Report an issue](https://github.com/scopedb/scopedb-js/issues)

This SDK executes ScopeQL statements; the language is documented separately:

- [Quickstart](https://www.scopedb.io/docs/guides/quickstart)
- [Query guide](https://www.scopedb.io/docs/guides/query-events)
- [Language reference](https://www.scopedb.io/docs/reference)

## Client configuration

The SDK compresses JSON request bodies and table append requests with gzip by default using the Web Compression API. Append limits are based on the uncompressed NDJSON body.

## Run a statement

Use `client.query()` to submit a statement and wait for its result.

For a detached or long-running statement, keep its handle and choose between a local snapshot, one remote status request, or waiting for the result:

```ts
const handle = await client.statement("SELECT 1 AS ready").submit();

// Synchronous and local: the snapshot updated by submit(), status(), or wait().
console.log(handle.lastStatus()?.status);

// Asynchronous: requests the latest remote status while the statement is active.
const latest = await handle.status();
console.log(latest.status);

// Polls until the statement terminates and returns its result set.
const result = await handle.wait();
```

`status()` returns the cached snapshot without another request once the handle has reached a terminal state. Use `client.statementHandle(id)` to resume this lifecycle from a previously stored statement ID. `wait()`, `query()`, and `execute()` accept `WaitOptions` when polling delays or cancellation need to be configured.

## Integer representation

`int` and `uint` cells default to JS `bigint` to preserve full I64 precision. This is the safe default but is **not** directly JSON-serializable — `JSON.stringify(rowWithBigInt)` throws `TypeError: Do not know how to serialize a BigInt`.

`toValues()`, `toObjects()`, and `first()` accept an optional `{ integerMode }` to opt in to a different representation:

```ts
// Default: bigint (lossless, NOT JSON-safe)
const rowsBigint = result.toObjects();

// JSON-safe number. Loses precision for |x| > Number.MAX_SAFE_INTEGER
// (i.e. 2**53 - 1). Safe for typical count() / bounded counters.
const rowsNumber = result.toObjects({ integerMode: "number" });
JSON.stringify(rowsNumber); // ok

// Decimal string. Always safe, always JSON-safe.
// Recommended for unbounded I64 identifiers.
const rowsString = result.toObjects({ integerMode: "string" });
```

The option only affects `int` / `uint` columns; other types are unchanged.

## Table helper

```ts
import { Client } from "scopedb";

const table = client.table("events", {
  database: "scopedb",
  schema: "public",
});

const description = await table.describe();
console.log(description.columns);
```

## Append rows

Every table append and transform-ingest HTTP request has a client timeout of 30 seconds by default. Its budget starts after local encoding, compression, and request preparation and covers one HTTP attempt through reading the complete response body. Set `ClientOptions.writeTimeoutMs` to a positive integer to change it. Direct `table.append()`, `client.insert()`, and `client.ingest()` calls can override it with `{ timeoutMs: 60_000 }`. Streams inherit the client timeout unless an append stream explicitly sets `attemptTimeoutMs()`. Caller `AbortSignal` cancellation still applies. Each attempt gets its own timeout; admission, retry backoff, and an entire `flush()`/`shutdown()` barrier are not bounded by it. A timeout or in-flight cancellation does not roll back a write: its commit outcome can be unknown, and the SDK does not automatically retry it.

Client timeouts after an HTTP attempt begins preserve the existing error kind and cause and add `error.writeDetails` with `{ reason: "timeout", outcome: "unknown" }` and `error.retryable === false`. In-flight caller cancellation uses the `"cancelled"` reason; a caller `TimeoutError` deadline uses `"timeout"`. Cancellation before sending preserves the original abort reason. The structured details remain available on subsequent errors returned by the stream and batch failure events. A server HTTP 408 is still a server response; an explicitly rejected append follows the existing safe retry policy. Reconcile a timed-out or cancelled write before replaying it. Append streams retain their existing at-least-once retry policy for other transient unknown outcomes; those retries may duplicate committed rows.

Use an append stream to write JavaScript objects to an existing table. The SDK groups them into batches automatically. This example uses a table named `sdk_example_events` with `id int` and `name string` columns:

```ts
const table = client.table("sdk_example_events", {
  database: "scopedb",
  schema: "public",
});
const stream = table.appendStream().build();

await stream.send({ id: 1, name: "first" });
await stream.sendAll([
  { id: 2, name: "second" },
  { id: 3, name: "third" },
]);

// Optional: wait for the current writes while keeping the stream open.
// await stream.flush();

// When finished, wait for remaining writes and close the stream.
await stream.shutdown();
```

`send()` and `sendAll()` add records to the SDK's pending writes. Call `shutdown()` when you are done sending to wait for writing to finish. `sendAll()` accepts an iterable or async iterable. Use `flush()` when you need to wait for the current writes and then continue using the stream.

### Continue after a failed batch

To keep processing later batches when one fails, set `failurePolicy` to `"continue"` and check the report returned by `flush()` or `shutdown()`:

```ts
const stream = table.appendStream({ failurePolicy: "continue" }).build();

await stream.sendAll([
  { id: 4, name: "fourth" },
  { id: 5, name: "fifth" },
]);

const report = await stream.shutdown();
console.log(report);
```

### Append an NDJSON string

If you already have newline-delimited JSON, pass it to `table.append()`:

```ts
const result = await table.append('{"id":6,"name":"sixth"}\n');
console.log(result.num_rows_inserted);
```

## Browse the catalog

The RESTful catalog methods return database, schema, table-summary, and full table resources. Use async iterators for the common path; list methods remain available when an application needs explicit page boundaries.

```ts
for await (const database of client.iterateDatabases({ pageSize: 100 })) {
  console.log(database.name);
}

for await (
  const table of client.iterateTables({
    database: "scopedb",
    schema: "public",
    pageSize: 100,
  })
) {
  console.log(table.name);
}
```

## Errors

Server messages pass through unchanged. `ScopeDBError` adds structured diagnostics without requiring callers to parse the message:

```ts
import { ScopeDBError } from "scopedb";

try {
  await client.query("SELECT 1");
} catch (error) {
  if (error instanceof ScopeDBError) {
    console.error({
      message: error.message,
      httpStatus: error.httpStatus,
      requestId: error.requestId,
      retryable: error.retryable,
      retryAfterMs: error.retryAfterMs,
      statementDetails: error.statementDetails,
    });
  }
  throw error;
}
```

When a statement reaches the `failed` state, `statementDetails` preserves the server-provided `code`, `message`, and optional code-specific `details` object.

## CommonJS applications

The package is ESM-only. CommonJS applications can load it with dynamic import:

```js
async function main() {
  const { Client } = await import("scopedb");
  // ...
}

void main();
```

## Batched JSON ingest

This is also a write path: create the target first and use a disposable table while evaluating the example.

Transform ingest does not report a rejected-versus-unknown commit state. Failed requests are therefore never automatically retried, including HTTP 429/5xx, transport errors, timeouts, and invalid success responses. A failed stream closes admission and rejects its pending `flush()` and `shutdown()` barriers. Later operations preserve the terminal error's HTTP status, request ID, and `Retry-After` diagnostics. Check the destination before replaying data that may already have committed; use a durable outbox when payloads must survive process failure.

```ts
import { Client } from "scopedb";

const client = new Client(process.env.SCOPEDB_ENDPOINT!, {
  apiKey: process.env.SCOPEDB_API_KEY!,
});

const stream = client
  .ingestStream(`
    SELECT
      $0["ts"]::timestamp AS occurred_at,
      $0["name"]::string AS name
    INSERT INTO public.sdk_example_events (occurred_at, name)
  `)
  .build();

await stream.send({
  ts: "2026-03-13T12:00:00Z",
  name: "scopedb",
});

await stream.flush();
await stream.shutdown();
```

## Examples

See the runnable instructions in [`examples/README.md`](examples/README.md).

All examples import the public `scopedb` package entry and are checked with:

```sh
pnpm run check:examples
```

## Development

Use the pnpm version declared in `package.json`:

```sh
pnpm install --frozen-lockfile
pnpm test
pnpm run check
```

The package emits TypeScript declarations from `src/index.ts`. Generated artifacts stay out of git, and `prepack` runs the unit, type, example, and package-entry checks before creating a publishable tarball.

Release history and the maintainer runbook are in [CHANGELOG.md](CHANGELOG.md) and [DELIVERY.md](DELIVERY.md).

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
