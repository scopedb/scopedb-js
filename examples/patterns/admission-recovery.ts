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

import { Client, ScopeDBError } from "scopedb";

// A finite, single-producer example. Keep the source rows until their commit is
// confirmed. This is not durable checkpointing or recovery after a process crash.
const tableName = process.env["SCOPEDB_TABLE"];
if (tableName === undefined || tableName.length === 0) {
  throw new Error("Set SCOPEDB_TABLE to a disposable table before running this write example");
}
const client = new Client(process.env["SCOPEDB_ENDPOINT"] ?? "http://127.0.0.1:6543", {
  apiKey: process.env["SCOPEDB_API_KEY"],
});
const table = client.table(tableName, {
  database: process.env["SCOPEDB_DATABASE"] ?? "scopedb",
  schema: process.env["SCOPEDB_SCHEMA"] ?? "public",
});
const rows = [
  { id: 1n, occurred_at: new Date(), name: "first" },
  { id: NaN, occurred_at: new Date(), name: "needs-correction" },
  { id: 3n, occurred_at: new Date(), name: "third" },
];
const stream = table.appendStream().strictEncoding(true).build();
let admissionError: unknown;
try {
  await stream.sendAll(rows);
} catch (error) {
  admissionError = error;
}

// Even when sendAll throws, an earlier prefix can already be buffered or sent.
// No other producer shares this stream, so its accepted count identifies that
// prefix. Do not use a count as a source cursor with concurrent producers.
const acceptedRows = stream.stats().acceptedRows;
// A rejected or unknown batch makes this throw: exit without replaying anything.
// Earlier batches may have committed. Reconcile using durable IDs/checkpoints
// before deciding which source records can safely be submitted again.
const committed = await stream.shutdown();
if ((committed?.num_rows_inserted ?? 0) !== acceptedRows) {
  throw new Error("Accepted prefix was not fully confirmed; reconcile before replay");
}
if (!(admissionError instanceof ScopeDBError) ||
  admissionError.context().get("record_path") === undefined || acceptedRows !== 1) {
  throw admissionError ?? new Error("Expected the sample's second row to fail local encoding");
}
console.log("local validation failed", admissionError.message);
console.log("confirmed source prefix", acceptedRows);

// Correct the known bad source row, then resume only the unadmitted suffix.
// Previously committed rows are not submitted again.
rows[1] = { ...rows[1]!, id: 2n };
const recovery = table.appendStream().strictEncoding(true).build();
try {
  await recovery.sendAll(rows.slice(acceptedRows));
} finally {
  // Also settle a partially accepted suffix if its admission fails.
  await recovery.shutdown();
}
console.log("confirmed total", acceptedRows + recovery.stats().committedRows);
