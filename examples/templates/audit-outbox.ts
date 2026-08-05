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

// Layer: integration template (type-checked, not directly runnable).
// Requires a transactional outbox with crash-safe ATTEMPTING recovery.

const MAX_APPEND_ROWS = 200_000;
const MAX_APPEND_BYTES = 16 * 1024 * 1024;

/** One durable attempt maps to exactly one HTTP append request. */
export interface PreparedAuditAttempt {
  attemptId: string;
  eventIds: readonly string[];
  /** One compact JSON object per line, in the same order as eventIds. */
  ndjson: string;
}

/**
 * Implement this with a transactional database outbox. beginAppendAttempt()
 * must atomically persist READY -> ATTEMPTING before returning and must:
 *
 * - keep attemptId, eventIds, and ndjson immutable;
 * - return unique, stable event IDs matching each row's event_id in order;
 * - enforce both maxRows and the UTF-8 maxBytes limit; and
 * - never return an ATTEMPTING attempt for append again. After a crash, an
 *   expired ATTEMPTING attempt goes to reconciliation, even if no request may
 *   have been sent. Stable IDs alone do not make the destination idempotent.
 */
export interface DurableAuditOutbox {
  beginAppendAttempt(options: {
    maxRows: number;
    maxBytes: number;
  }): Promise<PreparedAuditAttempt | null>;
  markDelivered(attemptId: string, eventIds: readonly string[]): Promise<void>;
  markNeedsReconciliation(
    attemptId: string,
    eventIds: readonly string[],
    message: string,
  ): Promise<void>;
  markRejected(
    attemptId: string,
    eventIds: readonly string[],
    message: string,
  ): Promise<void>;
  markInvalid(
    attemptId: string,
    eventIds: readonly string[],
    message: string,
  ): Promise<void>;
}

const tableName = process.env["SCOPEDB_TABLE"];
if (tableName === undefined || tableName.length === 0) {
  throw new Error(
    "Set SCOPEDB_TABLE to the audit destination before starting this worker",
  );
}

const client = new Client(
  process.env["SCOPEDB_ENDPOINT"] ?? "http://127.0.0.1:6543",
  {
    apiKey: process.env["SCOPEDB_API_KEY"],
  },
);
const auditTable = client.table(tableName, {
  database: process.env["SCOPEDB_DATABASE"] ?? "scopedb",
  schema: process.env["SCOPEDB_SCHEMA"] ?? "public",
});

export async function drainAuditOutbox(
  outbox: DurableAuditOutbox,
): Promise<void> {
  for (;;) {
    // This boundary is deliberate: one durable attempt equals one HTTP
    // request, so a rejected request cannot hide an earlier committed prefix.
    const attempt = await outbox.beginAppendAttempt({
      maxRows: MAX_APPEND_ROWS,
      maxBytes: MAX_APPEND_BYTES,
    });
    if (attempt === null) {
      return;
    }
    try {
      validateAttempt(attempt);
    } catch (error) {
      // No network request has started, so this is a local poison attempt, not
      // an ambiguous remote commit. Persist that state before stopping.
      await recordThenThrow(
        "failed to record an invalid audit attempt",
        error,
        () => outbox.markInvalid(
          attempt.attemptId,
          attempt.eventIds,
          error instanceof Error ? error.message : String(error),
        ),
      );
    }

    let result;
    try {
      result = await auditTable.append(attempt.ndjson, {
        signal: AbortSignal.timeout(30_000),
      });
    } catch (error) {
      if (
        error instanceof AppendRowsError &&
        error.appendState === "rejected"
      ) {
        await recordThenThrow(
          "failed to record a rejected audit batch",
          error,
          () => outbox.markRejected(
            attempt.attemptId,
            attempt.eventIds,
            error.message,
          ),
        );
      }

      // Timeouts, transport failures, and malformed success responses are
      // unknown: the rows may already exist remotely. Never blindly replay.
      await recordThenThrow(
        "failed to record an ambiguous audit batch",
        error,
        () => outbox.markNeedsReconciliation(
          attempt.attemptId,
          attempt.eventIds,
          error instanceof Error ? error.message : String(error),
        ),
      );
      throw error;
    }

    if (result.num_rows_inserted !== attempt.eventIds.length) {
      const mismatch = new Error(
        `append reported ${result.num_rows_inserted} rows for ${attempt.eventIds.length} audit events`,
      );
      await recordThenThrow(
        "failed to record an inconsistent audit result",
        mismatch,
        () => outbox.markNeedsReconciliation(
          attempt.attemptId,
          attempt.eventIds,
          mismatch.message,
        ),
      );
    }

    try {
      await outbox.markDelivered(attempt.attemptId, attempt.eventIds);
    } catch (error) {
      // The remote commit succeeded but the local ACK did not. Keep this batch
      // out of normal replay even if recording reconciliation also fails.
      await recordThenThrow(
        "failed to persist the committed audit batch state",
        error,
        () => outbox.markNeedsReconciliation(
          attempt.attemptId,
          attempt.eventIds,
          error instanceof Error ? error.message : String(error),
        ),
      );
    }
  }
}

function validateAttempt(attempt: PreparedAuditAttempt): void {
  const bytes = new TextEncoder().encode(attempt.ndjson).byteLength;
  if (attempt.attemptId.length === 0) {
    throw new Error("the outbox returned an empty audit attempt ID");
  }
  if (attempt.eventIds.length === 0) {
    throw new Error("the outbox returned an empty audit attempt");
  }
  if (
    attempt.eventIds.length > MAX_APPEND_ROWS ||
    bytes > MAX_APPEND_BYTES
  ) {
    throw new Error("the outbox returned an attempt above the append limit");
  }
  if (new Set(attempt.eventIds).size !== attempt.eventIds.length) {
    throw new Error("the outbox returned duplicate audit event IDs");
  }

  const body = attempt.ndjson.endsWith("\n")
    ? attempt.ndjson.slice(0, -1)
    : attempt.ndjson;
  const lines = body.split("\n");
  if (lines.length !== attempt.eventIds.length) {
    throw new Error("audit event IDs do not match the NDJSON row count");
  }
  for (let index = 0; index < lines.length; index += 1) {
    const eventId = attempt.eventIds[index];
    const line = lines[index];
    if (eventId === undefined || eventId.length === 0 || line === undefined) {
      throw new Error("the outbox returned an invalid audit event mapping");
    }
    let row: unknown;
    try {
      row = JSON.parse(line);
    } catch (cause) {
      throw new Error(`audit NDJSON row ${index} is invalid JSON`, { cause });
    }
    if (
      typeof row !== "object" ||
      row === null ||
      Array.isArray(row) ||
      !("event_id" in row) ||
      row.event_id !== eventId
    ) {
      throw new Error(`audit NDJSON row ${index} has the wrong event_id`);
    }
  }
}

async function recordThenThrow(
  message: string,
  cause: unknown,
  record: () => Promise<void>,
): Promise<never> {
  try {
    await record();
  } catch (recordError) {
    throw new AggregateError([cause, recordError], message, { cause });
  }
  throw cause;
}
