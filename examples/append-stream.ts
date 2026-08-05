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

import { Client } from "scopedb";

// Layer: quickstart (runnable write).
// Shows SDK-owned asynchronous batching with the default strict policy.

const tableName = process.env["SCOPEDB_TABLE"];
if (tableName === undefined || tableName.length === 0) {
  throw new Error(
    "Set SCOPEDB_TABLE to a disposable table before running this write example",
  );
}

const client = new Client(
  process.env["SCOPEDB_ENDPOINT"] ?? "http://127.0.0.1:6543",
  {
    apiKey: process.env["SCOPEDB_API_KEY"],
  },
);
const stream = client
  .table(tableName, {
    database: process.env["SCOPEDB_DATABASE"] ?? "scopedb",
    schema: process.env["SCOPEDB_SCHEMA"] ?? "public",
  })
  .appendStream()
  .build();

const accepted = await stream.sendAll([
  { id: 1, occurred_at: new Date().toISOString(), name: "first" },
  { id: 2, occurred_at: new Date().toISOString(), name: "second" },
]);
console.log("accepted locally", accepted.acceptedRows);

// Successful strict shutdown confirms the accepted prefix committed. See the
// bulk pattern for production failure handling and durable-resume boundaries.
const committed = await stream.shutdown();
console.log("committed remotely", committed?.num_rows_inserted ?? 0);
