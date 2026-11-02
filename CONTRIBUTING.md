# Contributing to the JavaScript SDK

Use the Node.js version range and pnpm version declared in `package.json`:

```sh
pnpm install --frozen-lockfile
pnpm run ci
```

`pnpm run ci` runs the behavior tests and package checks. `pnpm test` compiles the source and tests, then runs the behavior suites with Node's test runner. The default suite uses local fetch stubs and requires no ScopeDB service. `pnpm run check` builds the package, type-checks the examples and Node/Web consumers against its declarations, exercises query, catalog, and append operations, and runs publint against the package contents.

## Choosing a test

Test an observable contract through the public SDK interface. Keep regressions for precision, recovery, delivery certainty, resource limits, and concurrency. Stub the HTTP boundary to control server responses; use deferred responses or controlled timers when ordering matters.

Before adding a case, identify the failure it would catch and whether an existing test already covers it. Avoid separate tests for getters, method existence, or internal helper calls. A type-level contract, such as the return type selected by an Append failure policy, is part of the caller interface and is worth preserving.

## Runtime and package validation

```sh
pnpm run smoke:runtime
pnpm run smoke:frameworks
pnpm pack --dry-run
```

The runtime smoke program exercises the built package in Node.js; CI also runs it in Bun. The framework smoke installs a tarball into temporary consumers, verifies that source maps resolve to packaged files, and compiles the public declarations separately with Node and Web globals. It then runs Next.js and Workers consumers against a local mock backend. The Workers consumer runs without `nodejs_compat`, validating the package in a Web runtime. This check requires network access for consumer dependencies. `pnpm pack --dry-run` runs the prepack checks and lists the published files.

## Live integration

`pnpm test:integration` is opt-in. Set `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` for a service where you may create and delete temporary schemas. Each suite creates an isolated schema and removes it afterward. Keep shared-environment workloads bounded, and keep credentials and infrastructure identifiers out of test output and pull requests.

## Documentation and examples

Keep the [README](README.md) focused on installation and the first query and write. Put caller contracts and operational choices in the [usage guide](docs/usage.md), and runnable workflows in [examples](examples/README.md). Use SDK-versioned migration guides for breaking changes. Keep contributor commands and test infrastructure out of the usage guide.

Run `pnpm run example:list` to list the runnable examples. `pnpm run example -- <name>` builds and runs one; write examples require an explicitly selected disposable table. `pnpm run check:examples` verifies the examples without contacting a service.

Use relative links between repository documents. Keep each prose paragraph or list item on one source line, and align Markdown table columns.
