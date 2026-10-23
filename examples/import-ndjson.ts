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

import { createReadStream } from "node:fs";
import { Client } from "scopedb";

// Keep the original JSON text so integers beyond 2^53 are not rounded by
// JSON.parse/stringify. Admission and requests are bounded; writes are single attempts.
const endpoint = process.env["SCOPEDB_ENDPOINT"];
const tableName = process.env["SCOPEDB_TABLE"];
const file = process.env["SCOPEDB_INPUT_FILE"];
if (!endpoint || !tableName || !file) {
  throw new Error(
    "Set SCOPEDB_ENDPOINT, SCOPEDB_TABLE, and SCOPEDB_INPUT_FILE",
  );
}
const table = new Client(endpoint, { apiKey: process.env["SCOPEDB_API_KEY"] })
  .table(tableName, {
    database: process.env["SCOPEDB_DATABASE"] ?? "scopedb",
    schema: process.env["SCOPEDB_SCHEMA"] ?? "public",
  });
const requestLimit = 8 * 1024 * 1024;
const targetBytes = 4 * 1024 * 1024;
let pending: string[] = [];
let pendingBytes = 0;
let committedRows = 0;

async function flush(): Promise<void> {
  if (pending.length === 0) return;
  const body = pending.join("\n");
  // Detach before sending so a failed attempt is never replayed by cleanup.
  pending = [];
  pendingBytes = 0;
  committedRows += (await table.append(body)).num_rows_inserted;
}

async function* lines(path: string): AsyncIterable<string> {
  let buffered = "";
  for await (
    const chunk of createReadStream(path, {
      encoding: "utf8",
      highWaterMark: 64 * 1024,
    })
  ) {
    buffered += chunk;
    let newline: number;
    while ((newline = buffered.indexOf("\n")) !== -1) {
      const line = buffered.slice(0, newline);
      buffered = buffered.slice(newline + 1);
      yield line;
    }
    if (Buffer.byteLength(buffered) > requestLimit) {
      throw new Error("input line exceeds the 8 MiB request limit");
    }
  }
  if (buffered.length > 0) yield buffered;
}

let sourceError: unknown;
let lineNumber = 0;
try {
  for await (const line of lines(file)) {
    lineNumber++;
    if (line.trim() === "") continue;
    const bytes = Buffer.byteLength(line);
    if (bytes > requestLimit) {
      throw new Error(`line ${lineNumber} exceeds the 8 MiB request limit`);
    }
    let value: unknown;
    try {
      value = JSON.parse(line);
    } catch (cause) {
      throw new Error(`invalid JSON on line ${lineNumber}`, { cause });
    }
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
      throw new Error(`line ${lineNumber} must contain a JSON object`);
    }
    if (
      pending.length > 0 &&
      (pendingBytes + 1 + bytes > targetBytes || pending.length >= 200_000)
    ) await flush();
    pendingBytes += bytes + (pending.length > 0 ? 1 : 0);
    pending.push(line);
  }
} catch (error) {
  sourceError = error;
} finally {
  // A malformed later line does not discard the valid, admitted prefix.
  try {
    await flush();
  } finally {
    console.log("Committed rows:", committedRows);
  }
}
if (sourceError !== undefined) throw sourceError;
