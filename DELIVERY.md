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
- `StatementHandle.lastStatus()` for the local snapshot, `status()` for one
  remote update, and `wait()` for polling to completion
- `WaitOptions` for query and statement polling configuration
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
pnpm pack --dry-run
```

`prepack` runs the unit, type, example, and package-entry checks. CI repeats the
runtime smoke test on Node.js 20, 22, 24, and Bun. Framework consumers
should additionally build the checked-in Next.js and Worker templates from the
packed tarball.

The fast `smoke:web-runtime` check only removes Node's `Buffer`; it is not a
substitute for workerd. Until packed Next.js and Wrangler consumers run in CI,
every release candidate must run those two fresh-consumer checks explicitly.

Generated `dist/`, `dist-test/`, `node_modules/`, and tarball files are not
committed.
