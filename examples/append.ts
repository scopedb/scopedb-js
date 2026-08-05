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

import { AppendRowsError, Client } from "scopedb";

// Layer: quickstart (runnable write).
// Shows one caller-owned NDJSON request with no automatic batching.

const tableName = process.env["SCOPEDB_TABLE"];
if (tableName === undefined || tableName.length === 0) {
  throw new Error(
    "Set SCOPEDB_TABLE to a disposable table before running this write example",
  );
}

const client = new Client(
  process.env["SCOPEDB_ENDPOINT"] ?? "http://127.0.0.1:6543",
  { token: process.env["SCOPEDB_TOKEN"] },
);
const table = client
  .table(tableName)
  .withDatabase(process.env["SCOPEDB_DATABASE"] ?? "scopedb")
  .withSchema(process.env["SCOPEDB_SCHEMA"] ?? "public");

const rows = [
  { id: 1, occurred_at: new Date().toISOString(), name: "first" },
  { id: 2, occurred_at: new Date().toISOString(), name: "second" },
];
const ndjson = rows.map((row) => JSON.stringify(row)).join("\n");

try {
  const result = await table.append(ndjson);
  console.log("committed remotely", result.num_rows_inserted);
} catch (error) {
  if (error instanceof AppendRowsError && error.appendState === "unknown") {
    console.error("commit outcome is unknown; reconcile before replaying");
  }
  throw error;
}
