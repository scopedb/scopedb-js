# Repository Guidelines

## Markdown

- Keep each semantic prose paragraph and each list item on one source line; do not hard-wrap within a paragraph or list item.
- Format Markdown tables with aligned pipe columns.

## User documentation

- Keep README.md short: installation, a runnable query, a small write example, and links by task. Put configuration, encoding, delivery, and error contracts in docs/usage.md.
- Organize migration guides by SDK versions, with concrete old-to-new replacements. Include Ingest and other API changes in the same guide for that upgrade.
- ScopeDB Cloud manages its service upgrades. Do not present internal backend versions or rollout steps as SDK user prerequisites or migration tasks.
