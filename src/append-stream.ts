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

import type { Client } from "./client.js";
import { AppendRowsError, ScopeDBError, asScopeDBError } from "./errors.js";
import type { AppendRowsResult } from "./protocol.js";
import {
  AsyncBoundedQueue,
  Deferred,
  PendingBytesBudget,
  PendingBytesClosedError,
  PendingBytesExceedsCapacityError,
  type PendingBytesReservation,
  MAX_TIMER_MS,
  QUEUE_CLOSED,
  QUEUE_TIMEOUT,
  abortReason,
  byteLength,
  nextBackoff,
  nonnegativeIntegerConfig,
  positiveIntegerConfig,
  sleep,
  throwIfAborted,
} from "./stream-internals.js";

const MAX_APPEND_BODY_BYTES = 16 * 1024 * 1024;
const MAX_APPEND_ROWS = 200_000;
const DEFAULT_BATCH_BYTES = MAX_APPEND_BODY_BYTES;
const DEFAULT_FLUSH_INTERVAL_MS = 1_000;
const DEFAULT_CHANNEL_CAPACITY = 1024;
const DEFAULT_MAX_IN_FLIGHT_REQUESTS = 4;
const DEFAULT_MAX_PENDING_BYTES =
  DEFAULT_BATCH_BYTES * DEFAULT_MAX_IN_FLIGHT_REQUESTS;
const DEFAULT_MAX_RETRIES = 8;
const DEFAULT_INITIAL_BACKOFF_MS = 100;
const DEFAULT_MAX_BACKOFF_MS = 5_000;
const DEFAULT_CONTINUE_ATTEMPT_TIMEOUT_MS = 30_000;
const DEFAULT_CIRCUIT_FAILURE_THRESHOLD = 5;
const DEFAULT_CIRCUIT_COOLDOWN_MS = 30_000;

export type AppendFailurePolicy = "stop" | "continue";
export type AppendStreamState = "open" | "closing" | "closed" | "failed";
export type AppendCircuitState = "closed" | "open" | "half-open";

export interface AppendStreamOptions<
  Policy extends AppendFailurePolicy = AppendFailurePolicy,
> {
  /**
   * Whether a failed batch stops the stream or allows later batches to continue.
   */
  failurePolicy: Policy;
}

export interface AppendWaitOptions {
  signal?: AbortSignal;
}

/**
 * Local admission result from `sendAll()`; it does not confirm remote delivery.
 */
export interface AppendAdmissionResult {
  acceptedRows: number;
}

/** Delivery outcomes covered by a continue-mode `flush()` or `shutdown()`. */
export interface AppendDeliveryReport {
  /**
   * `unknown` means no covered rows are known to have committed and at least
   * one batch may have committed; callers must not blindly replay it.
   */
  outcome: "ok" | "partial" | "failed" | "unknown";
  acceptedRows: number;
  committedRows: number;
  failedRows: number;
  unknownRows: number;
  droppedRows: number;
  committedBatches: number;
  failedBatches: number;
  unknownBatches: number;
  retries: number;
  /** Wall time from requesting this barrier until every covered batch settled. */
  durationMs: number;
}

export interface AppendStreamStats {
  state: AppendStreamState;
  circuitState: AppendCircuitState;
  acceptedRows: number;
  committedRows: number;
  failedRows: number;
  unknownRows: number;
  droppedRows: number;
  droppedByReason: Readonly<{
    bufferFull: number;
    invalidRecord: number;
    recordTooLarge: number;
    circuitOpen: number;
    closed: number;
  }>;
  retries: number;
  pendingRows: number;
  pendingBytes: number;
  inFlightBatches: number;
  lastFailure?: Readonly<{
    atMs: number;
    message: string;
    appendState?: "rejected" | "unknown";
  }>;
  lastReport?: Readonly<AppendDeliveryReport>;
}

export interface AppendBatchFailureEvent {
  error: ScopeDBError;
  batchRows: number;
  batchBytes: number;
  outcome: "rejected" | "unknown";
  action: "continuing" | "circuit-opened" | "stopped";
}

export interface AppendCircuitBreakerOptions {
  failureThreshold: number;
  cooldownMs: number;
}

export type AppendBarrierResult<Policy extends AppendFailurePolicy> =
  Policy extends "continue" ? AppendDeliveryReport : AppendRowsResult | null;

interface RetryConfig {
  maxRetries: number;
  initialBackoffMs: number;
  maxBackoffMs: number;
}

interface AppendStreamConfig<Policy extends AppendFailurePolicy> {
  readonly client: Client;
  readonly database: string;
  readonly schema: string;
  readonly table: string;
  readonly batchBytes: number;
  readonly batchRows: number;
  readonly flushIntervalMs: number;
  readonly channelCapacity: number;
  readonly maxPendingBytes: number;
  readonly maxInFlightRequests: number;
  readonly retry: RetryConfig;
  readonly failurePolicy: Policy;
  readonly attemptTimeoutMs: number | undefined;
  readonly circuitBreaker: AppendCircuitBreakerOptions | false;
  readonly batchFailureListeners: ReadonlyArray<
    (event: AppendBatchFailureEvent) => void | PromiseLike<void>
  >;
}

interface SerializedRecord {
  payload: string;
  bytes: number;
}

interface BufferedRecord extends SerializedRecord {
  reservation: PendingBytesReservation;
  countedAsAccepted: boolean;
}

interface RecordCommand {
  type: "record";
  record: BufferedRecord;
}

type BarrierOutput = AppendRowsResult | AppendDeliveryReport | null;

interface FlushCommand {
  type: "flush";
  ack: Deferred<BarrierOutput>;
  droppedRowsAtBarrier: number;
  startedAt: number;
}

interface ShutdownCommand {
  type: "shutdown";
  ack: Deferred<BarrierOutput>;
  droppedRowsAtBarrier: number;
  startedAt: number;
}

type AppendCommand = RecordCommand | FlushCommand | ShutdownCommand;

interface IntervalCounters {
  acceptedRows: number;
  committedRows: number;
  failedRows: number;
  unknownRows: number;
  committedBatches: number;
  failedBatches: number;
  unknownBatches: number;
  retries: number;
}

interface LifetimeCounters {
  acceptedRows: number;
  committedRows: number;
  failedRows: number;
  unknownRows: number;
  droppedRows: number;
  retries: number;
}

export class AppendStreamBuilder<
  Policy extends AppendFailurePolicy = "stop",
> {
  private currentBatchBytes = DEFAULT_BATCH_BYTES;
  private currentBatchRows = MAX_APPEND_ROWS;
  private currentFlushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS;
  private currentChannelCapacity = DEFAULT_CHANNEL_CAPACITY;
  private currentMaxPendingBytes = DEFAULT_MAX_PENDING_BYTES;
  private currentMaxInFlightRequests = DEFAULT_MAX_IN_FLIGHT_REQUESTS;
  private currentAttemptTimeoutMs: number | undefined;
  private currentCircuitBreaker: AppendCircuitBreakerOptions | false = {
    failureThreshold: DEFAULT_CIRCUIT_FAILURE_THRESHOLD,
    cooldownMs: DEFAULT_CIRCUIT_COOLDOWN_MS,
  };
  private readonly batchFailureListeners: Array<
    (event: AppendBatchFailureEvent) => void | PromiseLike<void>
  > = [];
  private currentRetry: RetryConfig = {
    maxRetries: DEFAULT_MAX_RETRIES,
    initialBackoffMs: DEFAULT_INITIAL_BACKOFF_MS,
    maxBackoffMs: DEFAULT_MAX_BACKOFF_MS,
  };

  /** @internal */
  static create<Policy extends AppendFailurePolicy>(
    client: Client,
    database: string,
    schema: string,
    table: string,
    failurePolicy: Policy,
  ): AppendStreamBuilder<Policy> {
    return new AppendStreamBuilder(
      client,
      database,
      schema,
      table,
      failurePolicy,
    );
  }

  private constructor(
    private readonly client: Client,
    private readonly database: string,
    private readonly schema: string,
    private readonly table: string,
    private readonly failurePolicy: Policy,
  ) {
    if (failurePolicy !== "stop" && failurePolicy !== "continue") {
      throw configError("failurePolicy must be 'stop' or 'continue'");
    }
  }

  /**
   * Target NDJSON payload size. A single row may exceed it, up to 16 MiB.
   * @deprecated Use `targetBatchBytes()`.
   */
  batchBytes(batchBytes: number): this {
    this.currentBatchBytes = positiveIntegerConfig(
      "batchBytes",
      batchBytes,
      MAX_APPEND_BODY_BYTES,
    );
    return this;
  }

  /** Target NDJSON payload size. A single row may exceed it, up to 16 MiB. */
  targetBatchBytes(targetBatchBytes: number): this {
    this.currentBatchBytes = positiveIntegerConfig(
      "targetBatchBytes",
      targetBatchBytes,
      MAX_APPEND_BODY_BYTES,
    );
    return this;
  }

  /** Maximum number of rows in one append request. */
  maxBatchRows(maxBatchRows: number): this {
    this.currentBatchRows = positiveIntegerConfig(
      "maxBatchRows",
      maxBatchRows,
      MAX_APPEND_ROWS,
    );
    return this;
  }

  /**
   * Maximum time from the first buffered row until its batch is dispatched.
   * @deprecated Use `flushIntervalMs()`.
   */
  flushInterval(flushIntervalMs: number): this {
    this.currentFlushIntervalMs = positiveIntegerConfig(
      "flushInterval",
      flushIntervalMs,
      MAX_TIMER_MS,
    );
    return this;
  }

  /** Maximum time from the first buffered row until its batch is dispatched. */
  flushIntervalMs(flushIntervalMs: number): this {
    this.currentFlushIntervalMs = positiveIntegerConfig(
      "flushIntervalMs",
      flushIntervalMs,
      MAX_TIMER_MS,
    );
    return this;
  }

  channelCapacity(channelCapacity: number): this {
    this.currentChannelCapacity = positiveIntegerConfig(
      "channelCapacity",
      channelCapacity,
    );
    return this;
  }

  /** @deprecated Use `maxBufferedBytes()`. */
  maxPendingBytes(maxPendingBytes: number): this {
    this.currentMaxPendingBytes = positiveIntegerConfig(
      "maxPendingBytes",
      maxPendingBytes,
    );
    return this;
  }

  maxBufferedBytes(maxBufferedBytes: number): this {
    this.currentMaxPendingBytes = positiveIntegerConfig(
      "maxBufferedBytes",
      maxBufferedBytes,
    );
    return this;
  }

  /**
   * Maximum number of append requests in flight. Defaults to 4.
   * @deprecated Use `maxConcurrentBatches()`.
   */
  maxInFlightRequests(maxInFlightRequests: number): this {
    this.currentMaxInFlightRequests = positiveIntegerConfig(
      "maxInFlightRequests",
      maxInFlightRequests,
    );
    return this;
  }

  /** Maximum number of append requests in flight. Defaults to 4. */
  maxConcurrentBatches(maxConcurrentBatches: number): this {
    this.currentMaxInFlightRequests = positiveIntegerConfig(
      "maxConcurrentBatches",
      maxConcurrentBatches,
    );
    return this;
  }

  maxRetries(maxRetries: number): this {
    this.currentRetry.maxRetries = nonnegativeIntegerConfig(
      "maxRetries",
      maxRetries,
    );
    return this;
  }

  initialBackoff(initialBackoffMs: number): this {
    this.currentRetry.initialBackoffMs = nonnegativeIntegerConfig(
      "initialBackoff",
      initialBackoffMs,
      MAX_TIMER_MS,
    );
    return this;
  }

  maxBackoff(maxBackoffMs: number): this {
    this.currentRetry.maxBackoffMs = nonnegativeIntegerConfig(
      "maxBackoff",
      maxBackoffMs,
      MAX_TIMER_MS,
    );
    return this;
  }

  /**
   * Per-attempt HTTP timeout in milliseconds. A timeout has an unknown outcome.
   * @deprecated Use `attemptTimeoutMs()`.
   */
  attemptTimeout(attemptTimeoutMs: number): this {
    this.currentAttemptTimeoutMs = positiveIntegerConfig(
      "attemptTimeout",
      attemptTimeoutMs,
      MAX_TIMER_MS,
    );
    return this;
  }

  /** Per-attempt HTTP timeout in milliseconds. */
  attemptTimeoutMs(attemptTimeoutMs: number): this {
    this.currentAttemptTimeoutMs = positiveIntegerConfig(
      "attemptTimeoutMs",
      attemptTimeoutMs,
      MAX_TIMER_MS,
    );
    return this;
  }

  /** Configure the continue-mode circuit breaker, or disable it. */
  circuitBreaker(options: AppendCircuitBreakerOptions | false): this {
    if (options === false) {
      this.currentCircuitBreaker = false;
      return this;
    }
    if (typeof options !== "object" || options === null) {
      throw configError("circuitBreaker options must be an object or false");
    }
    this.currentCircuitBreaker = {
      failureThreshold: positiveIntegerConfig(
        "circuitBreaker.failureThreshold",
        options.failureThreshold,
      ),
      cooldownMs: positiveIntegerConfig(
        "circuitBreaker.cooldownMs",
        options.cooldownMs,
        MAX_TIMER_MS,
      ),
    };
    return this;
  }

  /**
   * Observes rejected or ambiguous batch outcomes without affecting the worker.
   * Async listeners are observed but not awaited; listener failures are ignored.
   */
  onBatchFailure(
    listener: (event: AppendBatchFailureEvent) => void | PromiseLike<void>,
  ): this {
    if (typeof listener !== "function") {
      throw configError("onBatchFailure listener must be a function");
    }
    this.batchFailureListeners.push(listener);
    return this;
  }

  build(): AppendStream<Policy> {
    return AppendStream.create({
      client: this.client,
      database: this.database,
      schema: this.schema,
      table: this.table,
      batchBytes: this.currentBatchBytes,
      batchRows: this.currentBatchRows,
      flushIntervalMs: this.currentFlushIntervalMs,
      channelCapacity: this.currentChannelCapacity,
      maxPendingBytes: this.currentMaxPendingBytes,
      maxInFlightRequests: this.currentMaxInFlightRequests,
      retry: { ...this.currentRetry },
      failurePolicy: this.failurePolicy,
      attemptTimeoutMs: this.currentAttemptTimeoutMs ??
        (this.failurePolicy === "continue"
          ? DEFAULT_CONTINUE_ATTEMPT_TIMEOUT_MS
          : undefined),
      circuitBreaker: this.currentCircuitBreaker === false
        ? false
        : { ...this.currentCircuitBreaker },
      batchFailureListeners: [...this.batchFailureListeners],
    });
  }
}

export class AppendStream<Policy extends AppendFailurePolicy = "stop"> {
  private readonly queue: AsyncBoundedQueue<AppendCommand>;
  private readonly pendingBytes: PendingBytesBudget;
  private readonly task: Promise<void>;
  private readonly inFlight = new Set<Promise<void>>();
  private readonly fatalController = new AbortController();
  private rows: BufferedRecord[] = [];
  private currentBytes = 0;
  private batchDeadlineMs: number | null = null;
  private fatal: ScopeDBError | null = null;
  private accepting = true;
  private workerDone = false;
  private shutdownOperation?: Promise<BarrierOutput>;
  private interval = newIntervalCounters();
  private readonly lifetime: LifetimeCounters = {
    acceptedRows: 0,
    committedRows: 0,
    failedRows: 0,
    unknownRows: 0,
    droppedRows: 0,
    retries: 0,
  };
  private readonly droppedByReason = {
    bufferFull: 0,
    invalidRecord: 0,
    recordTooLarge: 0,
    circuitOpen: 0,
    closed: 0,
  };
  private reportedDroppedRows = 0;
  private pendingRowCount = 0;
  private lastFailure: AppendStreamStats["lastFailure"];
  private lastReport: AppendDeliveryReport | undefined;
  private circuitState: AppendCircuitState = "closed";
  private circuitOpenedUntil = 0;
  private consecutiveFailures = 0;
  private circuitProbeDone?: Deferred<void>;
  private circuitProbeAdmissionClaimed = false;

  /** @internal */
  static create<Policy extends AppendFailurePolicy>(
    config: AppendStreamConfig<Policy>,
  ): AppendStream<Policy> {
    return new AppendStream(config);
  }

  private constructor(private readonly config: AppendStreamConfig<Policy>) {
    this.queue = new AsyncBoundedQueue(config.channelCapacity);
    this.pendingBytes = new PendingBytesBudget(config.maxPendingBytes);
    this.task = this.runWorker();
  }

  /**
   * Attempts synchronous local admission. It never waits or throws; false means
   * the record was invalid, the bounded buffer was full, or the stream closed.
   */
  trySend(record: unknown): boolean {
    if (!this.accepting || this.fatal !== null) {
      this.noteDrop("closed");
      return false;
    }
    const claimsCircuitProbe =
      this.config.failurePolicy === "continue" &&
      this.circuitState === "open" &&
      Date.now() >= this.circuitOpenedUntil &&
      !this.circuitProbeAdmissionClaimed;
    if (
      this.config.failurePolicy === "continue" &&
      this.circuitState !== "closed" &&
      !claimsCircuitProbe
    ) {
      this.noteDrop("circuitOpen");
      return false;
    }

    let serialized: SerializedRecord;
    try {
      serialized = prepareRecord(record);
    } catch (cause) {
      this.noteDrop(
        cause instanceof RecordTooLargeError
          ? "recordTooLarge"
          : "invalidRecord",
      );
      return false;
    }

    const reservation = this.pendingBytes.tryAcquire(serialized.bytes + 1);
    if (reservation === null) {
      this.noteDrop("bufferFull");
      return false;
    }

    const buffered: BufferedRecord = {
      ...serialized,
      reservation,
      countedAsAccepted: false,
    };
    if (!this.queue.trySend({ type: "record", record: buffered })) {
      reservation.release();
      this.noteDrop(this.accepting ? "bufferFull" : "closed");
      return false;
    }
    this.ensureAccepted(buffered);
    if (claimsCircuitProbe) {
      this.circuitProbeAdmissionClaimed = true;
    }
    return true;
  }

  /**
   * Serializes and enqueues one row. Completion means accepted by the local
   * stream, not yet committed; use `flush()` or `shutdown()` as a commit barrier.
   */
  async send(record: unknown, options: AppendWaitOptions = {}): Promise<void> {
    throwIfAborted(options.signal);
    this.checkUsable();
    const serialized = prepareRecord(record);
    const reservedBytes = serialized.bytes + 1;
    let reservation: PendingBytesReservation;
    try {
      reservation = await this.pendingBytes.acquire(
        reservedBytes,
        options.signal,
      );
    } catch (cause) {
      if (options.signal?.aborted === true) {
        throw abortReason(options.signal);
      }
      throw this.mapPendingBytesError(cause, reservedBytes);
    }

    if (!this.accepting || this.fatal !== null) {
      reservation.release();
      throw this.closedOrFatalError();
    }

    const buffered: BufferedRecord = {
      ...serialized,
      reservation,
      countedAsAccepted: false,
    };
    try {
      await this.queue.send(
        { type: "record", record: buffered },
        options.signal,
      );
      this.ensureAccepted(buffered);
    } catch (cause) {
      reservation.release();
      if (options.signal?.aborted === true) {
        throw abortReason(options.signal);
      }
      throw this.closedOrFatalError();
    }

    this.checkFatal();
  }

  /** Consumes a sync or async iterable one row at a time with bounded backpressure. */
  async sendAll(
    records: Iterable<unknown> | AsyncIterable<unknown>,
    options: AppendWaitOptions = {},
  ): Promise<AppendAdmissionResult> {
    let acceptedRows = 0;
    for await (const record of records) {
      throwIfAborted(options.signal);
      await this.send(record, options);
      acceptedRows += 1;
    }
    return { acceptedRows };
  }

  /** Dispatches rows accepted before this barrier and waits for their outcomes. */
  async flush(
    options: AppendWaitOptions = {},
  ): Promise<AppendBarrierResult<Policy>> {
    const output = await this.flushInner(options);
    return output as AppendBarrierResult<Policy>;
  }

  /** Flushes all accepted rows and permanently closes the stream. */
  shutdown(
    options: AppendWaitOptions = {},
  ): Promise<AppendBarrierResult<Policy>> {
    if (this.shutdownOperation === undefined) {
      if (options.signal?.aborted === true) {
        return Promise.reject(abortReason(options.signal));
      }
      this.accepting = false;
      this.shutdownOperation = this.shutdownInner(
        this.lifetime.droppedRows,
        Date.now(),
      );
    }
    const operation = this.shutdownOperation as Promise<
      AppendBarrierResult<Policy>
    >;
    return options.signal === undefined
      ? operation
      : waitWithSignal(operation, options.signal);
  }

  stats(): Readonly<AppendStreamStats> {
    const state: AppendStreamState = this.fatal !== null
      ? "failed"
      : this.accepting
      ? "open"
      : this.workerDone
      ? "closed"
      : "closing";
    return {
      state,
      circuitState: this.circuitState,
      acceptedRows: this.lifetime.acceptedRows,
      committedRows: this.lifetime.committedRows,
      failedRows: this.lifetime.failedRows,
      unknownRows: this.lifetime.unknownRows,
      droppedRows: this.lifetime.droppedRows,
      droppedByReason: { ...this.droppedByReason },
      retries: this.lifetime.retries,
      pendingRows: this.pendingRowCount,
      pendingBytes: this.pendingBytes.usedBytes(),
      inFlightBatches: this.inFlight.size,
      ...(this.lastFailure === undefined
        ? {}
        : { lastFailure: { ...this.lastFailure } }),
      ...(this.lastReport === undefined
        ? {}
        : { lastReport: { ...this.lastReport } }),
    };
  }

  private async flushInner(options: AppendWaitOptions): Promise<BarrierOutput> {
    throwIfAborted(options.signal);
    if (this.fatal !== null) {
      await this.task;
      throw this.fatal;
    }
    if (!this.accepting) {
      throw this.closedOrFatalError();
    }

    const ack = new Deferred<BarrierOutput>();
    try {
      await this.queue.send({
        type: "flush",
        ack,
        droppedRowsAtBarrier: this.lifetime.droppedRows,
        startedAt: Date.now(),
      }, options.signal);
      return await waitWithSignal(ack.promise, options.signal);
    } catch (cause) {
      if (options.signal?.aborted === true) {
        throw abortReason(options.signal);
      }
      if (this.fatal !== null) {
        await this.task;
        throw this.fatal;
      }
      if (cause instanceof ScopeDBError) {
        throw cause;
      }
      throw this.closedOrFatalError();
    }
  }

  private async shutdownInner(
    droppedRowsAtBarrier: number,
    startedAt: number,
  ): Promise<BarrierOutput> {
    if (this.fatal !== null) {
      await this.task;
      throw this.fatal;
    }

    const ack = new Deferred<BarrierOutput>();
    try {
      await this.queue.send({
        type: "shutdown",
        ack,
        droppedRowsAtBarrier,
        startedAt,
      });
    } catch {
      await this.task;
      throw this.closedOrFatalError();
    }
    try {
      const output = await ack.promise;
      await this.task;
      return output;
    } catch (cause) {
      await this.task;
      if (this.fatal !== null) {
        throw this.fatal;
      }
      throw cause;
    }
  }

  private async runWorker(): Promise<void> {
    try {
      for (;;) {
        this.checkFatal();
        const timeoutMs = this.nextBatchTimeout();
        if (timeoutMs === 0) {
          await this.dispatchBuffered();
          continue;
        }

        const command = await this.queue.receive(timeoutMs);
        if (this.fatal !== null) {
          if (command !== QUEUE_TIMEOUT && command !== QUEUE_CLOSED) {
            this.disposeCommand(command, this.fatal);
          }
          throw this.fatal;
        }
        if (command === QUEUE_TIMEOUT) {
          await this.dispatchBuffered();
          continue;
        }
        if (command === QUEUE_CLOSED) {
          break;
        }

        switch (command.type) {
          case "record":
            await this.bufferRecord(command.record);
            break;
          case "flush":
            await this.completeBarrier(command);
            break;
          case "shutdown":
            await this.completeBarrier(command);
            return;
        }
      }
    } catch (cause) {
      this.setFatal(asStreamError(cause));
    } finally {
      await this.waitForInFlight();
      this.addCommittedContextToFatal();
      this.releaseBufferedAsFailed();
      this.queue.close();
      this.drainQueued();
      this.pendingBytes.close();
      this.accepting = false;
      this.workerDone = true;
    }
  }

  private nextBatchTimeout(): number {
    if (this.batchDeadlineMs === null) {
      return -1;
    }
    return Math.max(0, this.batchDeadlineMs - Date.now());
  }

  private async bufferRecord(record: BufferedRecord): Promise<void> {
    this.ensureAccepted(record);
    this.interval.acceptedRows += 1;
    const separatorBytes = this.rows.length === 0 ? 0 : 1;
    if (
      this.rows.length > 0 &&
      this.currentBytes + separatorBytes + record.bytes > this.config.batchBytes
    ) {
      try {
        await this.dispatchBuffered();
      } catch (cause) {
        this.markLocalRowsFailed([record]);
        throw cause;
      }
    }

    if (this.rows.length === 0) {
      this.batchDeadlineMs = Date.now() + this.config.flushIntervalMs;
    } else {
      this.currentBytes += 1;
    }
    this.rows.push(record);
    this.currentBytes += record.bytes;

    if (
      this.currentBytes >= this.config.batchBytes ||
      this.rows.length >= this.config.batchRows
    ) {
      await this.dispatchBuffered();
    }
  }

  private async dispatchBuffered(): Promise<void> {
    if (this.rows.length === 0) {
      return;
    }
    await this.waitForCapacity();
    const circuitProbe = await this.waitForCircuit();
    this.checkFatal();

    const batch = this.rows;
    const batchBytes = this.currentBytes;
    this.rows = [];
    this.currentBytes = 0;
    this.batchDeadlineMs = null;

    let task!: Promise<void>;
    task = this.appendBatch(batch)
      .then((result) => {
        this.recordBatchCommitted(batch.length, result);
        this.recordCircuitSuccess(circuitProbe);
      })
      .catch((cause: unknown) => {
        const error = asStreamError(cause);
        const circuitOpened = this.recordCircuitFailure(error, circuitProbe);
        this.recordBatchFailure(batch.length, error);
        this.emitBatchFailure({
          error,
          batchRows: batch.length,
          batchBytes,
          outcome: appendOutcome(error),
          action: this.config.failurePolicy === "stop"
            ? "stopped"
            : circuitOpened
            ? "circuit-opened"
            : "continuing",
        });
        if (this.config.failurePolicy === "stop") {
          this.setFatal(error);
        }
      })
      .finally(() => {
        releaseRows(batch);
        this.inFlight.delete(task);
      });
    this.inFlight.add(task);
  }

  private async appendBatch(
    batch: BufferedRecord[],
  ): Promise<AppendRowsResult> {
    const payload = batch.map((row) => row.payload).join("\n");
    let retries = 0;
    let backoffMs = this.config.retry.initialBackoffMs;

    for (;;) {
      this.checkFatal();
      const timeoutSignal = this.config.attemptTimeoutMs === undefined
        ? undefined
        : AbortSignal.timeout(this.config.attemptTimeoutMs);
      try {
        const result = await this.config.client.appendRows(
          this.config.database,
          this.config.schema,
          this.config.table,
          payload,
          timeoutSignal === undefined ? {} : { signal: timeoutSignal },
        );
        if (result.num_rows_inserted !== batch.length) {
          throw rowCountMismatchError(batch.length, result.num_rows_inserted);
        }
        return result;
      } catch (cause) {
        const error = asStreamError(cause);
        if (
          timeoutSignal?.aborted === true &&
          this.config.attemptTimeoutMs !== undefined
        ) {
          error.withContext("attempt_timeout_ms", this.config.attemptTimeoutMs);
        }
        const retryable =
          error instanceof AppendRowsError &&
          error.appendState === "rejected" &&
          error.isTemporary();
        if (retryable && retries < this.config.retry.maxRetries) {
          const retryDelayMs = Math.min(
            Math.max(backoffMs, error.retryAfterMs ?? 0),
            this.config.retry.maxBackoffMs,
          );
          if (retryDelayMs > 0) {
            await sleep(retryDelayMs, this.fatalController.signal);
          }
          if (this.fatal !== null) {
            throw error.withContext("retry_cancelled_by_stream_failure", true);
          }
          retries += 1;
          this.noteRetry();
          backoffMs = nextBackoff(
            backoffMs,
            this.config.retry.maxBackoffMs,
          );
          continue;
        }
        if (retryable) {
          throw retryExhaustedError(retries, error);
        }
        throw error;
      }
    }
  }

  private async waitForCapacity(): Promise<void> {
    while (this.inFlight.size >= this.config.maxInFlightRequests) {
      await Promise.race(this.inFlight);
      this.checkFatal();
    }
  }

  private async waitForInFlight(): Promise<void> {
    if (this.inFlight.size > 0) {
      await Promise.all([...this.inFlight]);
    }
  }

  private async waitForCircuit(): Promise<boolean> {
    if (
      this.config.failurePolicy !== "continue" ||
      this.config.circuitBreaker === false
    ) {
      return false;
    }

    for (;;) {
      if (this.circuitState === "closed") {
        return false;
      }
      if (this.circuitState === "half-open") {
        const probe = this.circuitProbeDone;
        if (probe !== undefined) {
          await probe.promise;
        }
        continue;
      }

      const remaining = this.circuitOpenedUntil - Date.now();
      if (remaining > 0) {
        await sleep(remaining, this.fatalController.signal);
        this.checkFatal();
        continue;
      }
      // Requests dispatched before the circuit opened must settle before the
      // half-open probe starts. Otherwise a late old result could resolve or
      // reopen a newer probe generation.
      if (this.inFlight.size > 0) {
        await this.waitForInFlight();
        this.checkFatal();
        continue;
      }
      this.circuitState = "half-open";
      this.circuitProbeDone = new Deferred<void>();
      return true;
    }
  }

  private async completeBarrier(
    command: FlushCommand | ShutdownCommand,
  ): Promise<void> {
    try {
      await this.dispatchBuffered();
      await this.waitForInFlight();
      this.checkFatal();
      command.ack.resolve(this.takeBarrierOutput(
        command.droppedRowsAtBarrier,
        command.startedAt,
      ));
    } catch (cause) {
      const error = asStreamError(cause);
      this.setFatal(error);
      command.ack.reject(error);
      throw error;
    }
  }

  private takeBarrierOutput(
    droppedRowsAtBarrier: number,
    startedAt: number,
  ): BarrierOutput {
    if (this.config.failurePolicy === "continue") {
      return this.takeDeliveryReport(droppedRowsAtBarrier, startedAt);
    }
    const result = this.interval.committedBatches === 0
      ? null
      : {
        append_state: "committed" as const,
        num_rows_inserted: this.interval.committedRows,
      };
    this.interval = newIntervalCounters();
    return result;
  }

  private takeDeliveryReport(
    droppedRowsAtBarrier: number,
    startedAt: number,
  ): AppendDeliveryReport {
    const counters = this.interval;
    const droppedRows = Math.max(
      0,
      droppedRowsAtBarrier - this.reportedDroppedRows,
    );
    this.reportedDroppedRows = Math.max(
      this.reportedDroppedRows,
      droppedRowsAtBarrier,
    );
    const errorRows = counters.failedRows + counters.unknownRows;
    const lostRows = errorRows + droppedRows;
    const outcome: AppendDeliveryReport["outcome"] = lostRows === 0
      ? "ok"
      : counters.committedRows > 0
      ? "partial"
      : counters.unknownRows > 0
      ? "unknown"
      : "failed";
    const report: AppendDeliveryReport = {
      outcome,
      acceptedRows: counters.acceptedRows,
      committedRows: counters.committedRows,
      failedRows: counters.failedRows,
      unknownRows: counters.unknownRows,
      droppedRows,
      committedBatches: counters.committedBatches,
      failedBatches: counters.failedBatches,
      unknownBatches: counters.unknownBatches,
      retries: counters.retries,
      durationMs: Math.max(0, Date.now() - startedAt),
    };
    this.lastReport = report;
    this.interval = newIntervalCounters();
    return report;
  }

  private ensureAccepted(record: BufferedRecord): void {
    if (record.countedAsAccepted) {
      return;
    }
    record.countedAsAccepted = true;
    this.lifetime.acceptedRows += 1;
    this.pendingRowCount += 1;
  }

  private recordBatchCommitted(
    rows: number,
    result: AppendRowsResult,
  ): void {
    this.lifetime.committedRows += result.num_rows_inserted;
    this.interval.committedRows += result.num_rows_inserted;
    this.interval.committedBatches += 1;
    this.pendingRowCount = Math.max(0, this.pendingRowCount - rows);
  }

  private recordBatchFailure(rows: number, error: ScopeDBError): void {
    const unknown = appendOutcome(error) === "unknown";
    if (unknown) {
      this.lifetime.unknownRows += rows;
      this.interval.unknownRows += rows;
      this.interval.unknownBatches += 1;
    } else {
      this.lifetime.failedRows += rows;
      this.interval.failedRows += rows;
      this.interval.failedBatches += 1;
    }
    this.pendingRowCount = Math.max(0, this.pendingRowCount - rows);
    this.lastFailure = {
      atMs: Date.now(),
      message: error.message,
      ...(error instanceof AppendRowsError
        ? { appendState: error.appendState }
        : {}),
    };
  }

  private markLocalRowsFailed(rows: BufferedRecord[]): void {
    if (rows.length === 0) {
      return;
    }
    for (const row of rows) {
      this.ensureAccepted(row);
      row.reservation.release();
    }
    this.lifetime.failedRows += rows.length;
    this.interval.failedRows += rows.length;
    this.pendingRowCount = Math.max(0, this.pendingRowCount - rows.length);
  }

  private noteRetry(): void {
    this.lifetime.retries += 1;
    this.interval.retries += 1;
  }

  private noteDrop(reason: keyof typeof this.droppedByReason): void {
    this.lifetime.droppedRows += 1;
    this.droppedByReason[reason] += 1;
  }

  private recordCircuitSuccess(probe: boolean): void {
    if (this.config.failurePolicy !== "continue") {
      return;
    }
    this.consecutiveFailures = 0;
    if (probe) {
      this.closeCircuit();
    }
  }

  private recordCircuitFailure(error: ScopeDBError, probe: boolean): boolean {
    if (
      this.config.failurePolicy !== "continue" ||
      this.config.circuitBreaker === false
    ) {
      return false;
    }

    if (!isAvailabilityFailure(error)) {
      if (probe) {
        this.closeCircuit();
      }
      return false;
    }

    this.consecutiveFailures += 1;
    if (
      probe ||
      this.consecutiveFailures >= this.config.circuitBreaker.failureThreshold
    ) {
      this.circuitState = "open";
      this.circuitOpenedUntil =
        Date.now() + this.config.circuitBreaker.cooldownMs;
      this.circuitProbeAdmissionClaimed = false;
      this.circuitProbeDone?.resolve();
      this.circuitProbeDone = undefined;
      return true;
    }
    return false;
  }

  private closeCircuit(): void {
    this.circuitState = "closed";
    this.circuitOpenedUntil = 0;
    this.consecutiveFailures = 0;
    this.circuitProbeAdmissionClaimed = false;
    this.circuitProbeDone?.resolve();
    this.circuitProbeDone = undefined;
  }

  private emitBatchFailure(event: AppendBatchFailureEvent): void {
    for (const listener of this.config.batchFailureListeners) {
      try {
        const pending = listener(event);
        if (pending !== undefined) {
          void Promise.resolve(pending).catch(() => {});
        }
      } catch {
        // Diagnostics must never recursively fail the append worker.
      }
    }
  }

  private setFatal(error: ScopeDBError): void {
    if (this.fatal !== null) {
      if (
        appendOutcome(this.fatal) === "rejected" &&
        appendOutcome(error) === "unknown"
      ) {
        error.withContext("prior_append_state", "rejected");
        this.fatal = error;
      }
      return;
    }
    this.fatal = error;
    this.accepting = false;
    this.fatalController.abort();
    this.circuitProbeDone?.resolve();
    this.queue.close();
    this.pendingBytes.close();
  }

  private addCommittedContextToFatal(): void {
    if (this.fatal === null) {
      return;
    }
    if (this.interval.committedBatches > 0) {
      this.fatal
        .withContext("committed_batches", this.interval.committedBatches)
        .withContext("num_rows_inserted", this.interval.committedRows);
    }
    if (this.interval.failedRows > 0) {
      this.fatal.withContext("failed_rows", this.interval.failedRows);
    }
    if (this.interval.unknownRows > 0) {
      this.fatal.withContext("unknown_rows", this.interval.unknownRows);
    }
  }

  private releaseBufferedAsFailed(): void {
    const rows = this.rows;
    this.rows = [];
    this.currentBytes = 0;
    this.batchDeadlineMs = null;
    this.markLocalRowsFailed(rows);
  }

  private drainQueued(): void {
    const error = this.closedOrFatalError();
    for (const command of this.queue.drain()) {
      this.disposeCommand(command, error);
    }
  }

  private disposeCommand(command: AppendCommand, error: ScopeDBError): void {
    switch (command.type) {
      case "record":
        this.markLocalRowsFailed([command.record]);
        break;
      case "flush":
      case "shutdown":
        command.ack.reject(error);
        break;
    }
  }

  private checkUsable(): void {
    this.checkFatal();
    if (!this.accepting) {
      throw this.closedOrFatalError();
    }
  }

  private checkFatal(): void {
    if (this.fatal !== null) {
      throw this.fatal;
    }
  }

  private closedOrFatalError(): ScopeDBError {
    return this.fatal ?? new ScopeDBError(
      "Unexpected",
      "append stream is closed",
    ).setPersistent();
  }

  private mapPendingBytesError(cause: unknown, requested: number): ScopeDBError {
    if (cause instanceof PendingBytesClosedError) {
      return this.closedOrFatalError();
    }
    if (cause instanceof PendingBytesExceedsCapacityError) {
      return new ScopeDBError(
        "AppendRowsFailed",
        `append stream record requires ${requested} buffered bytes, exceeds max_pending_bytes=${cause.capacity}`,
      ).setPermanent();
    }
    return asStreamError(cause);
  }
}

class RecordTooLargeError extends ScopeDBError {}

function prepareRecord(record: unknown): SerializedRecord {
  const payload = serializeRecord(record);
  const bytes = byteLength(payload);
  if (bytes > MAX_APPEND_BODY_BYTES) {
    throw new RecordTooLargeError(
      "AppendRowsFailed",
      `append stream row requires ${bytes} bytes, exceeds the ${MAX_APPEND_BODY_BYTES}-byte append limit`,
    ).setPermanent();
  }
  return { payload, bytes };
}

function serializeRecord(record: unknown): string {
  if (typeof record !== "object" || record === null || Array.isArray(record)) {
    throw new ScopeDBError(
      "AppendRowsFailed",
      "append stream records must be JSON objects",
    );
  }
  try {
    const payload = JSON.stringify(record);
    if (payload === undefined) {
      throw new ScopeDBError(
        "AppendRowsFailed",
        "failed to serialize append stream record: record produced undefined JSON",
      );
    }
    if (!payload.startsWith("{")) {
      throw new ScopeDBError(
        "AppendRowsFailed",
        "append stream records must serialize to JSON objects",
      );
    }
    return payload;
  } catch (cause) {
    if (cause instanceof ScopeDBError) {
      throw cause;
    }
    throw new ScopeDBError(
      "AppendRowsFailed",
      "failed to serialize append stream record",
      { cause },
    );
  }
}

function releaseRows(rows: BufferedRecord[]): void {
  for (const row of rows) {
    row.reservation.release();
  }
}

function retryExhaustedError(
  retries: number,
  cause: AppendRowsError,
): AppendRowsError {
  return new AppendRowsError(
    {
      message: cause.message,
      append_state: cause.appendState,
      row_errors: [...cause.rowErrors],
      row_errors_truncated: cause.rowErrorsTruncated,
    },
    "append stream batch exhausted retry budget",
    {
      cause,
      httpStatus: cause.httpStatus,
      requestId: cause.requestId,
      retryAfterMs: cause.retryAfterMs,
    },
  )
    .withContext("retries", retries)
    .withContext("last_error", cause.message)
    .setPersistent();
}

function rowCountMismatchError(expected: number, actual: number): AppendRowsError {
  return new AppendRowsError(
    {
      message: "append response row count does not match the request",
      append_state: "unknown",
      row_errors: [],
      row_errors_truncated: false,
    },
    `append response reported ${actual} inserted rows for a ${expected}-row request`,
  ).setPersistent();
}

function appendOutcome(error: ScopeDBError): "rejected" | "unknown" {
  return error instanceof AppendRowsError && error.appendState === "rejected"
    ? "rejected"
    : "unknown";
}

function isAvailabilityFailure(error: ScopeDBError): boolean {
  return appendOutcome(error) === "unknown" ||
    error.isTemporary() ||
    error.isPersistent();
}

function newIntervalCounters(): IntervalCounters {
  return {
    acceptedRows: 0,
    committedRows: 0,
    failedRows: 0,
    unknownRows: 0,
    committedBatches: 0,
    failedBatches: 0,
    unknownBatches: 0,
    retries: 0,
  };
}

function configError(message: string): ScopeDBError {
  return new ScopeDBError("ConfigInvalid", message).setPermanent();
}

function asStreamError(cause: unknown): ScopeDBError {
  return asScopeDBError("Unexpected", "append stream failed", cause);
}

function waitWithSignal<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal === undefined) {
    return promise;
  }
  try {
    throwIfAborted(signal);
  } catch (cause) {
    // The operation may already own an enqueued barrier. Keep observing its
    // completion even though this caller no longer waits for the result.
    void promise.catch(() => {});
    return Promise.reject(cause);
  }

  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const finish = (complete: () => void): void => {
      if (settled) {
        return;
      }
      settled = true;
      signal.removeEventListener("abort", onAbort);
      complete();
    };
    const onAbort = (): void => {
      finish(() => reject(abortReason(signal)));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    void promise.then(
      (value) => finish(() => resolve(value)),
      (cause) => finish(() => reject(cause)),
    );
    if (signal.aborted) {
      onAbort();
    }
  });
}
