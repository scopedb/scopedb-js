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

const { Client } = await import("scopedb");
const calls = [];
const fetch = async (input, init) => {
  const url = new URL(input instanceof Request ? input.url : input);
  calls.push({ url, init });
  if (url.pathname.endsWith("/rows")) {
    const rows = String(init?.body).split("\n").length;
    return Response.json({
      append_state: "committed",
      num_rows_inserted: rows,
    });
  }
  return Response.json({ items: [{ name: "scopedb", comment: null }] });
};

const client = new Client("http://127.0.0.1:6543", {
  apiKey: "runtime-smoke-key",
  fetch,
});

const databases = [];
for await (const database of client.iterateDatabases()) {
  databases.push(database.name);
}
check(databases.length === 1 && databases[0] === "scopedb", "catalog iterator");

const stream = client.table("events").appendStream()
  .maxBatchRows(10)
  .flushIntervalMs(60_000)
  .maxConcurrentBatches(1)
  .build();
const originalBuffer = globalThis.Buffer;
if (process.argv.includes("--without-buffer")) {
  Reflect.deleteProperty(globalThis, "Buffer");
}
try {
  await stream.sendAll([{ id: 1 }, { id: 2 }, { id: 3 }]);
} finally {
  if (originalBuffer !== undefined) {
    Reflect.set(globalThis, "Buffer", originalBuffer);
  }
}
const result = await stream.shutdown();
check(result?.num_rows_inserted === 3, "append stream result");
check(calls.length === 2, "catalog plus one append request");
check(
  calls.every(({ init }) => new Headers(init?.headers).get("Authorization") ===
    "Bearer runtime-smoke-key"),
  "API key propagation",
);

function check(condition, label) {
  if (!condition) {
    throw new Error(`runtime smoke failed: ${label}`);
  }
}
