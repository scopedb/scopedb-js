# ScopeDB SDK for Node.js

This package provides a TypeScript-first client for ScopeDB on Node.js.

## Installation

```sh
pnpm install scopedb
```

## Create a Client

```ts
import { Client } from "scopedb";

const client = new Client("http://127.0.0.1:6543");
```

## Run a Statement

```ts
import { Client } from "scopedb";

const client = new Client("http://127.0.0.1:6543");

const result = await client.statement("SELECT 1").execute();
console.log(result.intoValues());
```

## Integer Representation

`int` and `uint` cells default to JS `bigint` to preserve full I64 precision.
This is the safe default but is **not** directly JSON-serializable —
`JSON.stringify(rowWithBigInt)` throws `TypeError: Do not know how to serialize
a BigInt`.

`intoValues()`, `intoObjects()`, and `first()` accept an optional
`{ integerMode }` to opt in to a different representation:

```ts
// Default: bigint (lossless, NOT JSON-safe)
const rows = result.intoObjects();

// JSON-safe number. Loses precision for |x| > 2^53 - 1.
// Safe for typical count() / bounded counters.
const rows = result.intoObjects({ integerMode: "number" });
JSON.stringify(rows); // ok

// Decimal string. Always safe, always JSON-safe.
// Recommended for unbounded I64 identifiers.
const rows = result.intoObjects({ integerMode: "string" });
```

The option only affects `int` / `uint` columns; other types are unchanged.

## Table Helper

```ts
import { Client } from "scopedb";

const client = new Client("http://127.0.0.1:6543");

const table = client.table("events").withSchema("public");
console.log(table.identifier());

const schema = await table.tableSchema();
console.log(schema.fields().length);
```

## Batched JSON Ingest

```ts
import { Client } from "scopedb";

const client = new Client("http://127.0.0.1:6543");

const stream = client
  .ingestStream(`
    SELECT
      $0["ts"]::timestamp AS ts,
      $0["name"]::string AS name
    INSERT INTO public.events (ts, name)
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

See the TypeScript examples under [`examples/`](examples/):

- `examples/statement.ts`
- `examples/table.ts`
- `examples/batch.ts`

These examples import from `src/` directly so they stay close to the in-repo SDK surface while the package is still evolving.

## Development

```sh
pnpm test
pnpm run build
pnpm run check
```

## Delivery Notes

- The package is TypeScript-first and emits declarations from `src/index.ts`.
- Generated artifacts should stay out of git; `dist/`, `dist-test/` and `node_modules/` are ignored in [`.gitignore`](.gitignore).
- A broader package-delivery checklist lives in [DELIVERY.md](DELIVERY.md).
