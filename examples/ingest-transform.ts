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

// Layer: specialized runnable example for transform-oriented ingest.
// Requires SCOPEDB_TABLE and the disposable schema from examples/README.md.

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
const target = client.table(tableName, {
  database: process.env["SCOPEDB_DATABASE"] ?? "scopedb",
  schema: process.env["SCOPEDB_SCHEMA"] ?? "public",
});

// Prefer table.append()/appendStream() for rows already shaped like the target
// table. IngestStream remains useful when each JSON record needs SQL transforms.
// Table.identifier() safely quotes every configured ScopeQL identifier.
const stream = client
  .ingestStream(`
    SELECT
      $0["ts"]::timestamp AS occurred_at,
      $0["name"]::string AS name
    INSERT INTO ${target.identifier()} (occurred_at, name)
  `)
  .batchBytes(1024 * 1024)
  .build();

await stream.send({
  ts: "2026-03-13T12:00:00Z",
  name: "ScopeDB",
});

await stream.shutdown();
