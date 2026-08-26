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
// Run: pnpm run example:statement

const client = new Client(
  process.env["SCOPEDB_ENDPOINT"] ?? "http://127.0.0.1:6543",
  {
    apiKey: process.env["SCOPEDB_API_KEY"],
  },
);

const handle = await client.statement("SELECT 42 AS answer").submit();

// lastStatus() is a synchronous local snapshot. status() requests the latest
// remote state while the statement is active, and wait() polls to completion.
console.log("submitted", handle.statementId, handle.lastStatus()?.status);
console.log("latest status", (await handle.status()).status);
const result = await handle.wait();

// `number` is JSON-safe. Keep the default bigint mode when full i64 precision
// matters and the result does not need to pass through JSON.stringify().
console.log(result.toObjects({ integerMode: "number" }));
