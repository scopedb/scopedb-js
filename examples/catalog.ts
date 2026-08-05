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

// Layer: quickstart (read-only and runnable).
// Run: pnpm run example:catalog

const client = new Client(
  process.env["SCOPEDB_ENDPOINT"] ?? "http://127.0.0.1:6543",
  { token: process.env["SCOPEDB_TOKEN"] },
);
const database = process.env["SCOPEDB_DATABASE"] ?? "scopedb";
const schema = process.env["SCOPEDB_SCHEMA"] ?? "public";

let pageToken: string | undefined;
do {
  const page = await client.listDatabases({
    pageSize: 100,
    pageToken,
  });
  for (const item of page.items) {
    console.log("database", item.name, item.comment ?? "");
  }
  pageToken = page.next_page_token;
} while (pageToken !== undefined);

let schemaPageToken: string | undefined;
do {
  const page = await client.listSchemas(database, {
    pageSize: 100,
    pageToken: schemaPageToken,
  });
  for (const item of page.items) {
    console.log("schema", item.name, item.comment ?? "");
  }
  schemaPageToken = page.next_page_token;
} while (schemaPageToken !== undefined);

let firstTableName: string | undefined;
let tablePageToken: string | undefined;
do {
  const page = await client.listTables(database, schema, {
    pageSize: 100,
    pageToken: tablePageToken,
  });
  for (const item of page.items) {
    firstTableName ??= item.name;
    console.log("table", item.name, item.comment ?? "");
  }
  tablePageToken = page.next_page_token;
} while (tablePageToken !== undefined);

if (firstTableName !== undefined) {
  const resource = await client.fetchTable(database, schema, firstTableName);
  console.log("first table resource", resource);

  const tableSchema = await client
    .table(firstTableName)
    .withDatabase(database)
    .withSchema(schema)
    .tableSchema();
  console.log("first table fields", tableSchema.fields());
}
