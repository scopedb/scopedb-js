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

import type { Client, RequestOptions } from "./client.js";
import { ScopeDBError } from "./errors.js";
import type { StatementSnapshot } from "./protocol.js";
import { statementIsTerminated } from "./protocol.js";
import { ResultSet } from "./result.js";
import {
  MAX_TIMER_MS,
  nonnegativeIntegerConfig,
  sleep,
} from "./stream-internals.js";

export interface WaitOptions extends RequestOptions {
  /** Initial polling delay in milliseconds; doubles up to maxDelayMs. Defaults to 5. */
  initialDelayMs?: number;
  /** Maximum polling delay in milliseconds. Defaults to 1,000. */
  maxDelayMs?: number;
}

export class Statement {
  private statementIdValue?: string;
  private maxElapsedSeconds?: number;
  private maxEstimatedSeconds?: number;

  constructor(
    private readonly client: Client,
    private readonly statementText: string,
  ) {}

  withStatementId(statementId: string): this {
    this.statementIdValue = statementId;
    return this;
  }

  withMaxElapsedSeconds(seconds: number): this {
    this.maxElapsedSeconds = durationLimit(seconds);
    return this;
  }

  withMaxEstimatedSeconds(seconds: number): this {
    this.maxEstimatedSeconds = durationLimit(seconds);
    return this;
  }

  async submit(options: RequestOptions = {}): Promise<StatementHandle> {
    const statementId = this.statementIdValue;
    try {
      const snapshot = await this.client.submitStatement({
        statement: this.statementText,
        statement_id: statementId,
        max_elapsed_seconds: this.maxElapsedSeconds,
        max_estimated_seconds: this.maxEstimatedSeconds,
        format: "json",
      }, options);
      return new StatementHandle(
        this.client,
        statementId ?? snapshot.statement_id,
        snapshot,
      );
    } catch (cause) {
      throw attachStatementId(cause, statementId, options.signal);
    }
  }

  async execute(options: WaitOptions = {}): Promise<ResultSet> {
    const handle = await this.submit(options);
    try {
      return await handle.wait(options);
    } catch (cause) {
      // Retrying this convenience method would submit a second execution.
      // Temporary read failures remain retryable when using a retained handle.
      if (
        cause instanceof ScopeDBError && cause !== options.signal?.reason &&
        cause.isTemporary()
      ) {
        cause.setPersistent();
      }
      throw cause;
    }
  }
}

export class StatementHandle {
  private currentSnapshot?: StatementSnapshot;
  private nextObservation = 0;
  private appliedObservation = 0;
  readonly statementId: string;

  constructor(
    private readonly client: Client,
    statementId: string,
    snapshot?: StatementSnapshot,
  ) {
    this.statementId = canonicalStatementId(statementId);
    if (snapshot !== undefined) this.remember(snapshot, 0);
  }

  /** Returns a caller-owned copy of the latest observation without a request. */
  snapshot(): StatementSnapshot | undefined {
    return this.currentSnapshot === undefined
      ? undefined
      : structuredClone(this.currentSnapshot);
  }

  /** Fetches one observation, or returns a cached terminal snapshot. Failed requests leave the cache intact. */
  async refresh(options: RequestOptions = {}): Promise<StatementSnapshot> {
    if (
      this.currentSnapshot !== undefined &&
      statementIsTerminated(this.currentSnapshot)
    ) {
      return structuredClone(this.currentSnapshot);
    }
    const observation = ++this.nextObservation;
    try {
      return this.remember(
        await this.client.fetchStatement(this.statementId, options),
        observation,
      );
    } catch (cause) {
      throw attachStatementId(cause, this.statementId, options.signal);
    }
  }

  /** Waits for a terminal result. Aborting stops local waiting; use cancel() to request server cancellation. */
  async wait(options: WaitOptions = {}): Promise<ResultSet> {
    try {
      let delayMs = nonnegativeIntegerConfig(
        "initialDelayMs",
        options.initialDelayMs ?? 5,
        MAX_TIMER_MS,
      );
      const maxDelayMs = nonnegativeIntegerConfig(
        "maxDelayMs",
        options.maxDelayMs ?? 1_000,
        MAX_TIMER_MS,
      );
      delayMs = Math.min(delayMs, maxDelayMs);
      for (;;) {
        options.signal?.throwIfAborted();
        const snapshot = await this.refresh(options);
        switch (snapshot.status) {
          case "finished":
            return ResultSet.fromStatementResultSet(snapshot.result_set);
          case "failed":
            throw new ScopeDBError("StatementFailed", snapshot.error.message, {
              code: snapshot.error.code,
              details: snapshot.error.details,
              statementDetails: snapshot.error,
            });
          case "cancelled":
            throw new ScopeDBError(
              "StatementCancelled",
              `statement ${this.statementId} is cancelled`,
            );
          case "pending":
          case "running":
            await sleep(delayMs, options.signal);
            delayMs = Math.min(delayMs * 2, maxDelayMs);
        }
      }
    } catch (cause) {
      throw attachStatementId(cause, this.statementId, options.signal);
    }
  }

  /** Requests cancellation and returns the observed state, which may still be running or already finished/failed. */
  async cancel(options: RequestOptions = {}): Promise<StatementSnapshot> {
    if (
      this.currentSnapshot !== undefined &&
      statementIsTerminated(this.currentSnapshot)
    ) {
      return structuredClone(this.currentSnapshot);
    }
    const observation = ++this.nextObservation;
    try {
      return this.remember(
        await this.client.cancelStatement(this.statementId, options),
        observation,
      );
    } catch (cause) {
      throw attachStatementId(cause, this.statementId, options.signal);
    }
  }

  private remember(
    snapshot: StatementSnapshot,
    observation: number,
  ): StatementSnapshot {
    if (canonicalStatementId(snapshot.statement_id) !== this.statementId) {
      throw new ScopeDBError(
        "Unexpected",
        "statement response ID does not match the handle",
      );
    }
    const previous = this.currentSnapshot;
    // Concurrent refresh/cancel responses may arrive out of order. Terminal
    // results stay cached even after the server expires its saved result.
    if (previous !== undefined) {
      const previousTime = previous.progress.nanos_from_submitted;
      const observedTime = snapshot.progress.nanos_from_submitted;
      const olderProgress = previous.status === snapshot.status &&
        (observedTime < previousTime ||
          (observedTime === previousTime &&
            observation < this.appliedObservation));
      const regressesToPending = previous.status === "running" &&
        snapshot.status === "pending";
      if (
        statementIsTerminated(previous) ||
        (!statementIsTerminated(snapshot) &&
          (regressesToPending || olderProgress))
      ) {
        return this.snapshot()!;
      }
    }
    this.currentSnapshot = structuredClone(snapshot);
    this.appliedObservation = observation;
    return this.snapshot()!;
  }
}

function attachStatementId(
  cause: unknown,
  statementId: string | undefined,
  signal?: AbortSignal,
): unknown {
  // Caller-owned abort reasons retain their identity and metadata, even if
  // the caller supplied a ScopeDBError from another operation.
  if (
    cause instanceof ScopeDBError && cause !== signal?.reason &&
    statementId !== undefined
  ) {
    cause.withContext("statement_id", canonicalStatementId(statementId));
  }
  return cause;
}

const UUID_PATTERN =
  /^(?:[0-9a-f]{32}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/i;

function canonicalStatementId(value: string): string {
  let normalized = value;
  if (normalized.toLowerCase().startsWith("urn:uuid:")) {
    normalized = normalized.slice("urn:uuid:".length);
  }
  if (normalized.startsWith("{") && normalized.endsWith("}")) {
    normalized = normalized.slice(1, -1);
  }
  if (!UUID_PATTERN.test(normalized)) return value;
  const hex = normalized.replaceAll("-", "").toLowerCase();
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function durationLimit(seconds: number): number {
  if (!Number.isFinite(seconds) || seconds <= 0) {
    throw new ScopeDBError(
      "ConfigInvalid",
      "statement duration limits must be finite and positive",
    );
  }
  return seconds;
}
