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

The SDK 0.3 API removes legacy aliases; keep [Upgrade from 0.2 to 0.3](docs/migration-0.3.md) current. Do not add wire-level request payloads to the root exports unless applications must construct them directly.

## User documentation

Keep README.md focused on installation, a runnable query, a small write example, and task links. Put detailed contracts and recipes in [the usage guide](docs/usage.md). Organize migration guidance by SDK version and include Ingest changes in the same guide; do not present internal service versions or rollout steps as customer prerequisites. Include these guides in the npm package.

## Lifecycle and delivery contracts

Statement handles retain complete, monotonic snapshots. Caller mutation, delayed refreshes, and cancellation races must not change a cached terminal result. Server cancellation has a distinct error kind; a local signal only interrupts waiting.

Catalog resources validate known structure and preserve extensions without projecting into a second model. Definitions and ETags travel together; interrupted mutations remain unknown and are never replayed. Table identity defaults must match across ScopeQL, Catalog, and Append.

Append keeps bounded admission, batch size, concurrency, and settlement barriers. Confirmed transient rejection is retryable; unknown replay requires explicit opt-in and respects server refusal. Client timeouts and cancellations are never replayed. Stream statistics retain complete errors, and shutdown settles admitted rows before returning a final result or failure. Fetch redirect behavior remains the transport's default.

Run `pnpm test:integration` with `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` for the two opt-in live integration suites. They cover the native lifecycle, Append encodings, and atomic rejection, and each creates and removes a unique temporary schema. Default CI uses mocks and skips these live suites unless an endpoint is configured. Use a bounded workload in shared environments; deterministic local tests cover running-cancellation and delayed-response races. Keep endpoints, credentials, and infrastructure identifiers out of public validation notes.

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

Publishing uses [npm Trusted Publishing](https://docs.npmjs.com/trusted-publishers/) (OIDC) exclusively. In the [`scopedb` package settings](https://www.npmjs.com/package/scopedb/access), configure a GitHub Actions **Trusted Publisher** for organization `scopedb`, repository `scopedb-js`, workflow filename `publish.yml`, and environment name `release`, with direct `npm publish` allowed. These fields must match the workflow exactly. The GitHub-hosted runner obtains a short-lived publish credential through `id-token: write`; no npm publish token or local `npm login` is needed.

A new Trusted Publisher must complete its first successful publish within 48 hours. After that publish, the configuration is exempt from this expiry rule. Recreate an expired configuration before retrying. If publishing reports `ENEEDAUTH`, check the npm-side configuration, its expiry status, and direct-publish permission; a local `npm whoami` result does not test CI's OIDC authentication.

Check the publish log and npm registry before retrying a failed release. If the workflow stopped before npm accepted the version, such as an authentication failure, fix the configuration and rerun the failed workflow on the same tag. If npm already accepted the version and only GitHub Release creation failed, finish that release using the existing tag without republishing the package. Never move a release tag or overwrite a published npm version; source changes require a new version and tag.

## Required checks

Run these checks locally before tagging a release:

```sh
pnpm install --frozen-lockfile
pnpm test
pnpm run check
pnpm run smoke:runtime
pnpm run smoke:web-runtime
pnpm run smoke:frameworks
pnpm pack --dry-run
```

`prepack` runs the unit, type, example, and package-entry checks. CI repeats the runtime smoke test on Node.js 20, 22, 24, and Bun, and dry-runs the pack on every change. CI and the publish workflow also run `smoke:frameworks`, which installs the SDK tarball into a temporary consumer and runs the checked-in Next.js and Worker templates against a local mock ScopeDB 0.6 server.

The fast `smoke:web-runtime` check removes Node's `Buffer`. The framework check type-checks the installed Web consumer without Node ambient types, then runs query and append through the original templates in a real Next.js production server and Wrangler/workerd without `nodejs_compat`. Keep detailed protocol, batching, and input-validation cases in the regular tests. Direct framework dependencies are pinned in `tests/consumers/frameworks/package.json`; transitive dependencies resolve at installation, as in a fresh consumer project. This smoke test deliberately has no second lockfile and does not guarantee an identical dependency tree across runs.

Generated `dist/`, `dist-test/`, `node_modules/`, and tarball files are not committed.
