# JavaScript SDK delivery notes

This repository ships an ESM-only TypeScript package for trusted server-side
JavaScript runtimes. ScopeDB API keys must never be included in browser or
mobile client bundles.

## Runtime targets

- Node.js 20, 22, and 24
- Bun
- Next.js server code, with the Node runtime as the preferred target
- Web-standard server runtimes such as Cloudflare Workers

The SDK source is compiled without Node ambient types. Tests and examples opt
into Node types separately. This keeps accidental Node-only runtime APIs out of
the package while still allowing Node-based development tools.

CommonJS does not have a package export. Existing CommonJS applications can
load the SDK with dynamic `import()`.

## Public surface

`src/index.ts` is the declaration root. New application examples should use:

- `Client.query()`, `Client.statement()`, and `Client.statementHandle()`
- catalog list/fetch methods or the automatic catalog iterators
- `Client.table(name, { database, schema })`
- `Table.describe()`, `Table.append()`, and `Table.appendStream()`
- `ResultSet.rawRows()`, `toValues()`, `toObjects()`, and `first()`

Compatibility aliases remain available for previously published names. Do not
add wire-level request payloads to the root exports unless applications must
construct them directly.

## Required checks

Before publishing a package:

```sh
pnpm install --frozen-lockfile
pnpm test
pnpm run check
pnpm run smoke:runtime
pnpm run smoke:web-runtime
pnpm run smoke:frameworks
pnpm pack --dry-run
```

`prepack` runs the unit, type, example, and package-entry checks. CI repeats the
runtime smoke test on Node.js 20, 22, 24, and Bun. The framework smoke check
packs the built SDK, installs only that tarball into a clean locked consumer,
then builds and runs the checked-in Next.js and Worker templates against a
local mock ScopeDB server.

The fast `smoke:web-runtime` check only removes Node's `Buffer`; it is not a
substitute for workerd. `smoke:frameworks` starts a real Next.js production
server and a real Wrangler/workerd process without `nodejs_compat`. It verifies
query, direct append, append-stream batching, API-key propagation, application
write authorization, and request-size rejection over HTTP.

Generated `dist/`, `dist-test/`, `node_modules/`, and tarball files are not
committed.
