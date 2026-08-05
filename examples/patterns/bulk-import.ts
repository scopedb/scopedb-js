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

// Layer: production pattern (runnable).
// Guarantees bounded local memory and strict delivery barriers.
// Does not provide durable resume, idempotency, or whole-job rollback.

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

async function* generateRows(count: number): AsyncIterable<unknown> {
  for (let id = 1; id <= count; id += 1) {
    yield {
      id,
      occurred_at: new Date().toISOString(),
      name: `example-${id}`,
    };
  }
}

// The default failure policy is strict `stop`. Sequential local admission keeps
// memory bounded; HTTP append concurrency is controlled independently below.
const stream = table
  .appendStream()
  // Small enough for this sample to exercise several concurrent HTTP batches.
  .batchBytes(32 * 1024)
  .flushInterval(1_000)
  .concurrency(4)
  .maxPendingBytes(1024 * 1024)
  .requestTimeout(30_000)
  .build();

let admissionError: unknown;
try {
  const accepted = await stream.sendAll(generateRows(10_000));
  console.log("accepted locally", accepted.acceptedRows);
} catch (error) {
  admissionError = error;
  // There is no transactional stream-wide abort. This example deliberately
  // settles the already accepted prefix below, even if the input iterator,
  // serialization, cancellation, or an early append ended sendAll().
  console.error("row admission stopped; settling the accepted prefix", error);
}

try {
  const committed = await stream.shutdown();
  console.log("committed remotely", committed?.num_rows_inserted ?? 0);
  console.log("final stream stats", stream.stats());
  if (admissionError !== undefined) {
    process.exitCode = 1;
  }
} catch (deliveryError) {
  // A strict stream is terminal after an append failure. Do not reuse it.
  if (
    deliveryError instanceof AppendRowsError &&
    deliveryError.appendState === "unknown"
  ) {
    console.error("commit outcome is unknown; reconcile before replaying");
  } else {
    console.error("bulk append failed", deliveryError);
  }
  // Even a rejected final request does not make this entire input safe to
  // replay: earlier concurrent batches may have committed. Production imports
  // need durable source checkpoints, stable IDs, or destination deduplication.
  console.error("final stream stats", stream.stats());
  process.exitCode = 1;
}
