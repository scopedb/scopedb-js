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

import { Client, type AppendRowsResult, type AppendDeliveryReport } from "scopedb";

// The same caller must type-check with Node globals alone and with Web globals alone.
const endpoint = "https://example.invalid";
const client = new Client(endpoint, {
  apiKey: "consumer-key",
  headers: new Headers({ "X-Application": "consumer" }),
  fetch: globalThis.fetch,
});
new Client(endpoint, { headers: { "X-Application": "consumer" } });
new Client(endpoint, { headers: [["X-Application", "consumer"]] });

const rows = await client.query("SELECT 1 AS ready", {
  signal: AbortSignal.timeout(1_000),
});
JSON.stringify(rows.toObjects({ integerMode: "string" }));

const table = client.table("events");
const stream = table.appendStream().strictEncoding(true).build();
await stream.send({ id: 9007199254740993n });
const written = await stream.shutdown();
written satisfies AppendRowsResult | null;
// @ts-expect-error Stop-mode results do not have continue-mode delivery counters.
written?.ambiguousRows;

const telemetry = table.appendStream({ failurePolicy: "continue" }).build();
telemetry.trySend({ id: 1 });
const report = await telemetry.shutdown();
report satisfies AppendDeliveryReport;
// @ts-expect-error Continue-mode callers must inspect delivery counters.
report.num_rows_inserted;
