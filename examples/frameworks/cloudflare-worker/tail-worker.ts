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

// Deploy this Worker and attach it to a producer Worker with `tail_consumers`
// in Wrangler configuration. It uses standard Web APIs without `nodejs_compat`
// and writes invocation summaries, console logs, and uncaught exceptions to
// the shared `sdk_example_events` schema.
interface Env {
  SCOPEDB_ENDPOINT: string;
  SCOPEDB_API_KEY: string;
  SCOPEDB_TABLE: string;
  SCOPEDB_DATABASE?: string;
  SCOPEDB_SCHEMA?: string;
}

export interface TailRequestEvent {
  readonly request?: {
    readonly url?: string;
    readonly method?: string;
    readonly headers?: Readonly<Record<string, string>>;
    readonly cf?: Readonly<Record<string, unknown>>;
  };
  readonly response?: {
    readonly status?: number;
  };
}

export interface TailLog {
  readonly timestamp?: number | null;
  readonly level: string;
  readonly message: readonly unknown[];
}

export interface TailException {
  readonly timestamp?: number | null;
  readonly name: string;
  readonly message: string;
  readonly stack?: string;
}

export interface TailItem {
  readonly scriptName?: string | null;
  readonly outcome: string;
  readonly eventTimestamp?: number | null;
  readonly wallTime?: number;
  readonly cpuTime?: number;
  readonly event?: TailRequestEvent | null;
  readonly logs?: readonly TailLog[];
  readonly exceptions?: readonly TailException[];
}

const MIN_INT64 = BigInt("-9223372036854775808");
const MAX_INT64 = BigInt("9223372036854775807");
const MAX_SANITIZE_DEPTH = 8;

export default {
  async tail(events: readonly TailItem[], env: Env): Promise<void> {
    const endpoint = requiredBinding(env.SCOPEDB_ENDPOINT, "SCOPEDB_ENDPOINT");
    const apiKey = requiredBinding(env.SCOPEDB_API_KEY, "SCOPEDB_API_KEY");
    const tableName = requiredBinding(env.SCOPEDB_TABLE, "SCOPEDB_TABLE");

    const rows = events.flatMap(toEventRows);
    if (rows.length === 0) {
      return;
    }

    const client = new Client(endpoint, { apiKey });
    const stream = client
      .table(tableName, {
        database: env.SCOPEDB_DATABASE ?? "scopedb",
        schema: env.SCOPEDB_SCHEMA ?? "public",
      })
      .appendStream()
      .strictEncoding(true)
      .maxConcurrentBatches(1)
      .attemptTimeoutMs(10_000)
      .maxElapsedTimeMs(25_000)
      .maxRetries(2)
      .build();

    await stream.sendAll(rows);
    await stream.shutdown();
  },
};

function toEventRows(item: TailItem): Record<string, unknown>[] {
  const invocationId = crypto.randomUUID();
  const occurredAt = formatTimestamp(item.eventTimestamp);
  const request = item.event?.request;
  const path = extractPathname(request?.url);
  const status = safeHttpStatus(item.event?.response?.status);
  const rayId = headerValue(request?.headers, "cf-ray");
  const scriptName = item.scriptName ?? null;
  const logs = item.logs ?? [];
  const exceptions = item.exceptions ?? [];

  const rows: Record<string, unknown>[] = [
    withRequestMetadata(
      {
        event_id: invocationId,
        occurred_at: occurredAt,
        name: "worker.invocation",
        attributes: {
          invocation_id: invocationId,
          script_name: scriptName,
          outcome: item.outcome,
          method: typeof request?.method === "string" ? request.method : null,
          ray_id: rayId,
          colo: stringField(request?.cf?.["colo"]),
          country: stringField(request?.cf?.["country"]),
          wall_time_ms: finiteNumber(item.wallTime),
          cpu_time_ms: finiteNumber(item.cpuTime),
          log_count: logs.length,
          exception_count: exceptions.length,
        },
      },
      path,
      status,
    ),
  ];

  for (const log of logs) {
    rows.push(
      withRequestMetadata(
        {
          event_id: crypto.randomUUID(),
          occurred_at: formatTimestamp(log.timestamp ?? item.eventTimestamp),
          name: "worker.log",
          attributes: {
            invocation_id: invocationId,
            script_name: scriptName,
            outcome: item.outcome,
            ray_id: rayId,
            level: log.level,
            message: sanitizeValue(log.message, 0, new Set()),
          },
        },
        path,
        status,
      ),
    );
  }

  for (const exception of exceptions) {
    rows.push(
      withRequestMetadata(
        {
          event_id: crypto.randomUUID(),
          occurred_at: formatTimestamp(exception.timestamp ?? item.eventTimestamp),
          name: "worker.exception",
          attributes: {
            invocation_id: invocationId,
            script_name: scriptName,
            outcome: item.outcome,
            ray_id: rayId,
            exception_name: exception.name,
            message: exception.message,
            stack: typeof exception.stack === "string" ? exception.stack : null,
          },
        },
        path,
        status,
      ),
    );
  }

  return rows;
}

function withRequestMetadata(
  row: Record<string, unknown>,
  path: string | undefined,
  status: number | undefined,
): Record<string, unknown> {
  if (path !== undefined) {
    row["path"] = path;
  }
  if (status !== undefined) {
    row["status"] = status;
  }
  return row;
}

function formatTimestamp(epochMs: number | null | undefined): string {
  if (typeof epochMs === "number" && Number.isFinite(epochMs)) {
    const date = new Date(epochMs);
    if (Number.isFinite(date.getTime())) {
      return date.toISOString();
    }
  }
  return new Date().toISOString();
}

// Keep only the URL pathname so query-string tokens or credentials are never stored.
function extractPathname(rawUrl: string | undefined): string | undefined {
  if (typeof rawUrl !== "string" || rawUrl.length === 0) {
    return undefined;
  }
  try {
    return new URL(rawUrl).pathname;
  } catch {
    return undefined;
  }
}

function safeHttpStatus(status: number | undefined): number | undefined {
  if (typeof status === "number" && Number.isSafeInteger(status) && status >= 100 && status <= 599) {
    return status;
  }
  return undefined;
}

function headerValue(
  headers: Readonly<Record<string, string>> | undefined,
  targetName: string,
): string | null {
  if (headers === undefined) {
    return null;
  }
  const lowerTarget = targetName.toLowerCase();
  for (const [name, value] of Object.entries(headers)) {
    if (name.toLowerCase() === lowerTarget && typeof value === "string") {
      return value;
    }
  }
  return null;
}

function stringField(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function finiteNumber(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return null;
  }
  if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
    return null;
  }
  return value;
}

// Normalize arbitrary console.log arguments so non-finite numbers, unsafe
// integers, sparse arrays, Errors, or circular objects never reject the batch.
function sanitizeValue(
  value: unknown,
  depth: number,
  ancestors: Set<object>,
): unknown {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value))) {
      return String(value);
    }
    return value;
  }
  if (typeof value === "bigint") {
    return value >= MIN_INT64 && value <= MAX_INT64 ? value : value.toString();
  }
  if (value === undefined) {
    return null;
  }
  if (typeof value !== "object") {
    return String(value);
  }
  if (value instanceof Date) {
    const time = Date.prototype.getTime.call(value);
    return Number.isFinite(time) ? Date.prototype.toISOString.call(value) : "Invalid Date";
  }
  if (value instanceof Error) {
    return {
      name: value.name,
      message: value.message,
      ...(typeof value.stack === "string" ? { stack: value.stack } : {}),
    };
  }
  if (ancestors.has(value)) {
    return "[Circular]";
  }
  if (depth >= MAX_SANITIZE_DEPTH) {
    return "[MaxDepth]";
  }
  if (!Array.isArray(value) && !isPlainObject(value)) {
    return String(value);
  }

  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      return Array.from(value, (item) => sanitizeValue(item, depth + 1, ancestors));
    }
    const sanitized: Record<string, unknown> = {};
    for (const key of Object.keys(value)) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (descriptor === undefined || !("value" in descriptor) || descriptor.value === undefined) {
        continue;
      }
      sanitized[key] = sanitizeValue(descriptor.value, depth + 1, ancestors);
    }
    return sanitized;
  } finally {
    ancestors.delete(value);
  }
}

function isPlainObject(value: object): value is Record<string, unknown> {
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function requiredBinding(value: unknown, name: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`Missing Worker binding: ${name}`);
  }
  return value;
}
