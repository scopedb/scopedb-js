# ScopeDB SDK for JavaScript

[![Version](https://img.shields.io/npm/v/scopedb)](https://www.npmjs.com/package/scopedb) [![Docs](https://img.shields.io/badge/docs-scopedb.io-blue)](https://www.scopedb.io/docs/developer/nodejs) [![CI](https://img.shields.io/github/actions/workflow/status/scopedb/scopedb-js/ci.yml?branch=main&label=CI)](https://github.com/scopedb/scopedb-js/actions/workflows/ci.yml) [![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://www.apache.org/licenses/LICENSE-2.0)

Query [ScopeDB](https://www.scopedb.io) and write rows from server-side JavaScript and TypeScript applications.

Upgrading an existing application? See [Upgrade from 0.2 to 0.3](docs/migration-0.3.md), including the move from Ingest to Append.

## Install

Requires Node.js 20 or later, or a supported server runtime such as Bun, Next.js server code, or Cloudflare Workers. The package is ESM-only, includes TypeScript declarations, and has no runtime dependencies. Keep API keys in trusted server-side code, outside browser bundles and Next.js Client Components.

```sh
npm install scopedb
# or: pnpm add scopedb
# or: bun add scopedb
```

## Run a query

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

`query()` waits for the result. `toObjects()` returns rows keyed by column name; integer cells default to `bigint`. See [Read query results](docs/usage.md#read-query-results) for JSON-safe output and timestamp precision.

## Write rows

Using the client above and an existing `events` table with `id int` and `name string` columns:

```js
const stream = client.table("events").appendStream().build();
await stream.send({ id: 1, name: "page_view" });

const result = await stream.shutdown();
console.log("Written rows:", result?.num_rows_inserted ?? 0);
```

`send()` queues a row; `shutdown()` waits for pending writes and closes the stream. Use `flush()` to wait while keeping it open. If you already have NDJSON, use [`table.append()`](docs/usage.md#append-an-ndjson-string).

## Find what you need

| Task                         | Documentation                                                           |
| ---------------------------- | ----------------------------------------------------------------------- |
| Read query results           | [Result conversion](docs/usage.md#read-query-results)                   |
| Track or cancel a query      | [Statement handles](docs/usage.md#track-or-cancel-a-query)              |
| Read or change table schemas | [Catalog](docs/usage.md#read-or-change-table-schemas)                   |
| Encode rows for Append       | [JSON encodings](docs/usage.md#json-encoding-for-append)                |
| Tune batching and retries    | [Append streams](docs/usage.md#append-rows)                             |
| Write logs or telemetry      | [Telemetry](docs/usage.md#long-lived-telemetry-and-rate-limiting)       |
| Set timeouts or HTTP options | [Client configuration](docs/usage.md#timeouts-and-http-options)         |
| Inspect a failure            | [Errors](docs/usage.md#errors)                                          |
| Use Next.js, Workers, or Bun | [Runtime templates](examples/README.md#framework-and-runtime-templates) |
| Load from CommonJS           | [Dynamic import](docs/usage.md#commonjs-applications)                   |

For complete programs, see the [examples](examples/README.md). For ScopeQL syntax, see the [query guide](https://www.scopedb.io/docs/guides/query-events) and [language reference](https://www.scopedb.io/docs/reference). The [SDK guide](https://www.scopedb.io/docs/developer/nodejs) and [changelog](CHANGELOG.md) provide additional context.

## Development

Use the pnpm version declared in `package.json`:

```sh
pnpm install --frozen-lockfile
pnpm test
pnpm run check
```

Report problems through [GitHub issues](https://github.com/scopedb/scopedb-js/issues).

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
