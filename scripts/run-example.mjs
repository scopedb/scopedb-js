/*
 * Copyright 2024 ScopeDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const entries = new Map([
  ["statement", "statement.js"],
  ["catalog", "catalog.js"],
  ["append", "append.js"],
  ["append-stream", "append-stream.js"],
  ["append-bulk", "patterns/bulk-import.js"],
  ["append-telemetry", "patterns/telemetry.js"],
  ["ingest-transform", "ingest-transform.js"],
]);

const args = process.argv.slice(2);
if (args[0] === "--") {
  args.shift();
}
const name = args[0];
if (name === "--list") {
  console.log([...entries.keys()].join("\n"));
  process.exit(0);
}

const relativeEntry = name === undefined ? undefined : entries.get(name);
if (relativeEntry === undefined || args.length !== 1) {
  console.error(
    `Usage: pnpm run example -- <name>\nAvailable examples:\n${
      [...entries.keys()].map((entry) => `  ${entry}`).join("\n")
    }`,
  );
  process.exit(1);
}

const entryUrl = new URL(
  `../dist-test/examples-build/examples/${relativeEntry}`,
  import.meta.url,
);
await import(entryUrl.href);
