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

## Releasing

Releases are automated: pushing a `v*` tag runs the full check suite, publishes
to npm with provenance, and creates the GitHub Release.

1. Open a release pull request that bumps `package.json` and moves the
   `Unreleased` CHANGELOG.md entries under a `## X.Y.Z - YYYY-MM-DD` heading.
2. After merging, tag the release commit and push the tag:

   ```sh
   git tag -a vX.Y.Z -m "Release X.Y.Z" <commit>
   git push origin vX.Y.Z
   ```

3. The `Publish` workflow (`.github/workflows/publish.yml`) fails if the tag
   does not match the `package.json` version, then runs the required checks
   below, publishes `scopedb@X.Y.Z` to npm with `--provenance`, and creates a
   GitHub Release from the CHANGELOG.md section.

One-time setup on npmjs.com: under the `scopedb` package settings, add a
GitHub Actions **Trusted Publisher** for organization `scopedb`, repository
`scopedb-js`, workflow filename `publish.yml`. The workflow then authenticates
through OIDC and no npm token is required. If trusted publishing is not
configured, store an npm granular access token with publish rights as the
`NPM_TOKEN` repository secret instead; the publish step falls back to it.

Never retry a failed publish by moving a tag: once npm accepts a version it is
immutable. Bump the version and tag again instead.

## Required checks

The publish workflow and `prepack` run these checks; run them locally before
tagging a release:

```sh
pnpm install --frozen-lockfile
pnpm test
pnpm run check
pnpm run smoke:runtime
pnpm run smoke:web-runtime
pnpm pack --dry-run
```

`prepack` runs the unit, type, example, and package-entry checks. CI repeats
the runtime smoke test on Node.js 20, 22, 24, and Bun, and dry-runs the pack
on every change. Framework consumers should additionally build the checked-in
Next.js and Worker templates from the packed tarball.

The fast `smoke:web-runtime` check only removes Node's `Buffer`; it is not a
substitute for workerd. Until packed Next.js and Wrangler consumers run in CI,
every release candidate must run those two fresh-consumer checks explicitly.

Generated `dist/`, `dist-test/`, `node_modules/`, and tarball files are not
committed.
