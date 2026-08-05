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

// Layer: production pattern (runnable demo).
// Guarantees bounded, observable best-effort delivery.
// Does not retain rejected or ambiguous payloads for replay.

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

const telemetry = table
  .appendStream({ failurePolicy: "continue" })
  .targetBatchBytes(1024 * 1024)
  .flushIntervalMs(1_000)
  .maxConcurrentBatches(2)
  .maxBufferedBytes(32 * 1024 * 1024)
  .attemptTimeoutMs(10_000)
  .onBatchFailure(({ error, action }) => {
    // Use a different diagnostics sink, never this same telemetry stream.
    console.error("telemetry append error", action, error);
  })
  .build();

function track(
  name: string,
  attributes: Record<string, unknown>,
): boolean {
  return telemetry.trySend({
    event_id: crypto.randomUUID(),
    occurred_at: new Date().toISOString(),
    name,
    attributes,
  });
}

async function closeTelemetry(): Promise<void> {
  // In a real service: stop accepting requests and await producer tasks first.
  const report = await telemetry.shutdown();
  if (report.outcome !== "ok") {
    console.error("telemetry loss", report);
  }
  console.log("final stats", telemetry.stats());
}

// Demo driver only. Applications normally call track() from request/logging
// paths and closeTelemetry() from their graceful-shutdown hook.
let rejectedLocally = 0;
for (let request = 1; request <= 100; request += 1) {
  if (!track("request.completed", { request, status: 200 })) {
    rejectedLocally += 1;
  }
}
console.log("locally rejected", rejectedLocally);
await closeTelemetry();
