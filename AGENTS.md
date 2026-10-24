# Repository Guidelines

## Markdown

- Keep each semantic prose paragraph and each list item on one source line; do not hard-wrap within a paragraph or list item.
- Format Markdown tables with aligned pipe columns.

## User documentation

- Keep README.md short: installation, a runnable query, a small write example, and links by task. Put configuration, encoding, delivery, and error contracts in docs/usage.md.
- Organize migration guides by SDK versions, with concrete old-to-new replacements. Include Ingest and other API changes in the same guide for that upgrade.
- ScopeDB Cloud manages its service upgrades. Do not present internal backend versions or rollout steps as SDK user prerequisites or migration tasks.

## Package and verification

- Keep the package ESM-only and free of Node-only runtime APIs. Source compiles without Node ambient types; tests and examples opt into them separately. Keep wire-level request payloads out of the root exports unless applications must construct them directly.
- Default tests use a local mock backend. `pnpm test:integration` runs the two opt-in live suites with `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY`; each creates and removes a unique temporary schema. Use bounded workloads in shared environments, and omit endpoints, credentials, and infrastructure identifiers from public validation notes.
- `pnpm run smoke:frameworks` installs the SDK tarball into temporary Next.js and Workers consumers and checks query and append against a local mock backend. Keep detailed protocol cases in regular tests. Framework dependencies are pinned in the consumer manifest without a second lockfile; transitive versions resolve at installation.

## Releases

Open a release PR that updates `package.json` and moves the `Unreleased` changelog entries under `## vX.Y.Z (YYYY-MM-DD)`. Use `*` bullets and order sections as Breaking Changes, New Features, Bug Fixes, and Improvements. Describe user-observable changes against the latest release, including migration guidance; omit internal tests, refactors, documentation, and CI work.

Before tagging, run `pnpm install --frozen-lockfile`, `pnpm test`, `pnpm run check`, `pnpm run smoke:runtime`, `pnpm run smoke:web-runtime`, `pnpm run smoke:frameworks`, and `pnpm pack --dry-run`. Generated build directories and tarballs are not committed.

After the release PR is merged and publication is authorized, tag its commit with `git tag -a vX.Y.Z -m "Release X.Y.Z" <commit>` and push the tag with `git push origin vX.Y.Z`. The [Publish workflow](.github/workflows/publish.yml) verifies the package version, runs checks, publishes to npm with provenance, and creates a GitHub Release from the changelog.

Publishing uses [npm Trusted Publishing](https://docs.npmjs.com/trusted-publishers/) exclusively. In the [package settings](https://www.npmjs.com/package/scopedb/access), the GitHub Actions publisher must match organization `scopedb`, repository `scopedb-js`, workflow `publish.yml`, and environment `release`, with direct publishing allowed. The runner uses `id-token: write`; no npm token or local login is needed. A new publisher must complete its first successful publish within 48 hours; after that it is exempt from this expiry. For `ENEEDAUTH`, check this configuration, expiry, and direct-publish permission; local `npm whoami` does not test CI's OIDC authentication.

Check the publish log and npm registry before retrying. If npm has not accepted the version, fix the problem and rerun the workflow on the same tag. If npm accepted it but GitHub Release creation failed, finish the GitHub Release without republishing. Never move release tags or overwrite published versions; source changes require a new version and tag.
