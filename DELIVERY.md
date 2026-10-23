# JavaScript SDK delivery notes

This repository ships an ESM-only TypeScript package for trusted server-side JavaScript runtimes. ScopeDB API keys must never be included in browser or mobile client bundles.

## Runtime targets

- Node.js 20, 22, and 24
- Bun
- Next.js server code, with the Node runtime as the preferred target
- Web-standard server runtimes such as Cloudflare Workers

The SDK source is compiled without Node ambient types. Tests and examples opt into Node types separately. This keeps accidental Node-only runtime APIs out of the package while still allowing Node-based development tools.

CommonJS does not have a package export. Existing CommonJS applications can load the SDK with dynamic `import()`.

## Public surface

`src/index.ts` is the declaration root. New application examples should use:

- `Client.query()`, `Client.statement()`, and `Client.statementHandle()`
- `StatementHandle.snapshot()` for a caller-owned local snapshot, `refresh()` for one remote update, and `wait()` for polling to completion
- `WaitOptions` for query and statement polling configuration
- declarative Catalog selectors with `get()`, `apply()`, and `delete()`
- `Client.table(name, { database, schema })`
- `Table.catalog()`, `Table.append()`, and `Table.appendStream()`
- `ResultSet.rawRows()`, `toValues()`, `toObjects()`, and `first()`

The native 0.6 API removes legacy aliases; keep the migration table in README.md current. Do not add wire-level request payloads to the root exports unless applications must construct them directly.

## Lifecycle and delivery contracts

Statement handles retain complete, monotonic snapshots. Caller mutation, delayed refreshes, and cancellation races must not change a cached terminal result. Server cancellation has a distinct error kind; a local signal only interrupts waiting.

Catalog resources validate known structure and preserve extensions without projecting into a second model. Definitions and ETags travel together; interrupted mutations remain unknown and are never replayed. Table identity defaults must match across ScopeQL, Catalog, and Append.

Append keeps bounded admission, batch size, concurrency, and settlement barriers. Confirmed transient rejection is retryable; unknown replay requires explicit opt-in and respects server refusal. Client timeouts and cancellations are never replayed. Stream statistics retain complete errors, and shutdown settles admitted rows before returning a final result or failure. Fetch redirect behavior remains the transport's default.

Run `pnpm test:integration` with `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` for the opt-in native protocol test. It creates and removes a unique temporary schema. Use a bounded workload in shared environments; deterministic local tests cover running-cancellation and delayed-response races. Keep endpoints, credentials, and infrastructure identifiers out of public validation notes.

## Releasing

Releases are automated: pushing a `v*` tag runs the full check suite, publishes to npm with provenance, and creates the GitHub Release.

1. Open a release pull request that bumps `package.json` and moves the `Unreleased` CHANGELOG.md entries under a `## vX.Y.Z (YYYY-MM-DD)` heading.
2. After merging, tag the release commit and push the tag:

   ```sh
   git tag -a vX.Y.Z -m "Release X.Y.Z" <commit>
   git push origin vX.Y.Z
   ```

3. The `Publish` workflow (`.github/workflows/publish.yml`) fails if the tag does not match the `package.json` version, then runs the required checks below, publishes `scopedb@X.Y.Z` to npm with `--provenance`, and creates a GitHub Release from the CHANGELOG.md section.

Changelog entries follow the shared ScopeDB format: version headings are `## vX.Y.Z (YYYY-MM-DD)`, entries use `*` bullets with one item per source line, and sections appear in the order `### Breaking Changes`, `### New Features`, `### Bug Fixes`, `### Improvements`. Write each entry as one coherent user-observable change compared with the latest release tag, including migration guidance for breaking changes; deprecations are described in the entry that introduces their replacements rather than in a separate section. Exclude tests, refactors, documentation, and CI changes that users cannot observe.

One-time setup on npmjs.com: under the `scopedb` package settings, add a GitHub Actions **Trusted Publisher** for organization `scopedb`, repository `scopedb-js`, workflow filename `publish.yml`, environment name `release` (matching the publish job's `environment: release` declaration), with direct `npm publish` allowed (the workflow does not use staged publishing). These fields cannot be changed once the connection is created. The workflow then authenticates through OIDC and no npm token is required. If trusted publishing is not configured, store an npm granular access token with publish rights as the `NPM_TOKEN` repository secret instead; the publish step falls back to it.

Never retry a failed publish by moving a tag: once npm accepts a version it is immutable. Bump the version and tag again instead.

## Required checks

The publish workflow and `prepack` run these checks; run them locally before tagging a release:

```sh
pnpm install --frozen-lockfile
pnpm test
pnpm run check
pnpm run smoke:runtime
pnpm run smoke:web-runtime
pnpm pack --dry-run
```

`prepack` runs the unit, type, example, and package-entry checks. CI repeats the runtime smoke test on Node.js 20, 22, 24, and Bun, and dry-runs the pack on every change. Framework consumers should additionally build the checked-in Next.js and Worker templates from the packed tarball.

The fast `smoke:web-runtime` check only removes Node's `Buffer`; it is not a substitute for workerd. Until packed Next.js and Wrangler consumers run in CI, every release candidate must run those two fresh-consumer checks explicitly.

Generated `dist/`, `dist-test/`, `node_modules/`, and tarball files are not committed.
