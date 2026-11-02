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
const table = client.table(tableName, {
  database: process.env["SCOPEDB_DATABASE"] ?? "scopedb",
  schema: process.env["SCOPEDB_SCHEMA"] ?? "public",
});

async function* generateRows(count: number): AsyncIterable<object> {
  for (let id = 1; id <= count; id += 1) {
    yield {
      id,
      occurred_at: new Date().toISOString(),
      name: `example-${id}`,
    };
  }
}

const stream = table
  .appendStream()
  .targetBatchBytes(32 * 1024)
  .flushIntervalMs(1_000)
  .maxConcurrentBatches(4)
  .maxBufferedBytes(1024 * 1024)
  .attemptTimeoutMs(30_000)
  .build();

let admissionError: unknown;
try {
  const accepted = await stream.sendAll(generateRows(10_000));
  console.log("accepted locally", accepted.acceptedRows);
} catch (error) {
  admissionError = error;
  // Settle the admitted prefix before reporting the admission failure.
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
  if (
    deliveryError instanceof AppendRowsError &&
    deliveryError.outcome === "ambiguous"
  ) {
    console.error("commit outcome is unknown; reconcile before replaying");
  } else {
    console.error("bulk append failed", deliveryError);
  }
  console.error("final stream stats", stream.stats());
  process.exitCode = 1;
}
