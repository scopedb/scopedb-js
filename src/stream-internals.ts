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

import { ScopeDBError } from "./errors.js";

export const MAX_TIMER_MS = 2_147_483_647;
export const MAX_APPEND_BODY_BYTES = 8 * 1024 * 1024;

export function positiveIntegerConfig(
  name: string,
  value: number,
  max = Number.MAX_SAFE_INTEGER,
): number {
  return integerConfig(name, value, 1, max);
}

export function nonnegativeIntegerConfig(
  name: string,
  value: number,
  max = Number.MAX_SAFE_INTEGER,
): number {
  return integerConfig(name, value, 0, max);
}

function integerConfig(
  name: string,
  value: number,
  min: number,
  max: number,
): number {
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new ScopeDBError(
      "ConfigInvalid",
      `${name} must be a safe integer between ${min} and ${max}`,
    );
  }
  return value;
}

export class Deferred<T> {
  readonly promise: Promise<T>;
  resolve!: (value: T) => void;
  reject!: (reason?: unknown) => void;

  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
    });
  }
}

class QueueClosedError extends Error {}

export const QUEUE_TIMEOUT = Symbol("QUEUE_TIMEOUT");
export const QUEUE_CLOSED = Symbol("QUEUE_CLOSED");
export const QUEUE_HEAD_UNAVAILABLE = Symbol("QUEUE_HEAD_UNAVAILABLE");

type QueueReceiveResult<T> = T | typeof QUEUE_TIMEOUT | typeof QUEUE_CLOSED;

export class AsyncBoundedQueue<T> {
  private readonly items: T[] = [];
  private readonly sendWaiters: Array<{
    item: T;
    ack: Deferred<void>;
    signal?: AbortSignal;
    onAbort?: () => void;
  }> = [];
  private readonly recvWaiters: Array<{
    deferred: Deferred<QueueReceiveResult<T>>;
    timer?: ReturnType<typeof setTimeout>;
  }> = [];
  private closed = false;

  constructor(private readonly capacity: number) {}

  async send(item: T, signal?: AbortSignal): Promise<void> {
    throwIfAborted(signal);
    if (this.closed) {
      throw new QueueClosedError();
    }

    const recvWaiter = this.recvWaiters.shift();
    if (recvWaiter !== undefined) {
      if (recvWaiter.timer !== undefined) {
        clearTimeout(recvWaiter.timer);
      }
      recvWaiter.deferred.resolve(item);
      return;
    }

    if (this.items.length < this.capacity) {
      this.items.push(item);
      return;
    }

    const ack = new Deferred<void>();
    const waiter: {
      item: T;
      ack: Deferred<void>;
      signal?: AbortSignal;
      onAbort?: () => void;
    } = { item, ack, signal };
    if (signal !== undefined) {
      waiter.onAbort = () => {
        const index = this.sendWaiters.indexOf(waiter);
        if (index < 0) {
          return;
        }
        this.sendWaiters.splice(index, 1);
        ack.reject(abortReason(signal));
      };
      signal.addEventListener("abort", waiter.onAbort, { once: true });
    }
    this.sendWaiters.push(waiter);
    if (signal?.aborted === true) {
      waiter.onAbort?.();
    }
    try {
      await ack.promise;
    } finally {
      this.removeSenderAbortListener(waiter);
    }
  }

  trySend(item: T): boolean {
    if (this.closed) {
      return false;
    }

    const recvWaiter = this.recvWaiters.shift();
    if (recvWaiter !== undefined) {
      if (recvWaiter.timer !== undefined) {
        clearTimeout(recvWaiter.timer);
      }
      recvWaiter.deferred.resolve(item);
      return true;
    }

    // Do not let a non-blocking producer jump ahead of existing senders.
    if (this.sendWaiters.length > 0 || this.items.length >= this.capacity) {
      return false;
    }
    this.items.push(item);
    return true;
  }

  async receive(timeoutMs: number): Promise<QueueReceiveResult<T>> {
    if (this.items.length > 0) {
      const item = this.items.shift()!;
      this.drainSenders();
      return item;
    }

    if (this.sendWaiters.length > 0) {
      const waiter = this.sendWaiters.shift()!;
      this.removeSenderAbortListener(waiter);
      waiter.ack.resolve();
      return waiter.item;
    }

    if (this.closed) {
      return QUEUE_CLOSED;
    }

    const deferred = new Deferred<QueueReceiveResult<T>>();
    const waiter: {
      deferred: Deferred<QueueReceiveResult<T>>;
      timer?: ReturnType<typeof setTimeout>;
    } = { deferred };
    if (timeoutMs > 0) {
      waiter.timer = setTimeout(() => {
        const index = this.recvWaiters.indexOf(waiter);
        if (index >= 0) {
          this.recvWaiters.splice(index, 1);
        }
        deferred.resolve(QUEUE_TIMEOUT);
      }, timeoutMs);
    }
    this.recvWaiters.push(waiter);
    return deferred.promise;
  }

  tryReceiveHeadIf(predicate: (item: T) => boolean): T | typeof QUEUE_HEAD_UNAVAILABLE {
    if (this.items.length > 0) {
      const item = this.items[0]!;
      if (!predicate(item)) {
        return QUEUE_HEAD_UNAVAILABLE;
      }
      this.items.shift();
      this.drainSenders();
      return item;
    }

    if (this.sendWaiters.length > 0) {
      const waiter = this.sendWaiters[0]!;
      if (!predicate(waiter.item)) {
        return QUEUE_HEAD_UNAVAILABLE;
      }
      this.sendWaiters.shift();
      this.removeSenderAbortListener(waiter);
      waiter.ack.resolve();
      return waiter.item;
    }

    return QUEUE_HEAD_UNAVAILABLE;
  }

  drain(): T[] {
    return this.items.splice(0);
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    while (this.sendWaiters.length > 0) {
      const waiter = this.sendWaiters.shift()!;
      this.removeSenderAbortListener(waiter);
      waiter.ack.reject(new QueueClosedError());
    }
    while (this.recvWaiters.length > 0) {
      const waiter = this.recvWaiters.shift()!;
      if (waiter.timer !== undefined) {
        clearTimeout(waiter.timer);
      }
      waiter.deferred.resolve(QUEUE_CLOSED);
    }
  }

  private drainSenders(): void {
    while (this.sendWaiters.length > 0) {
      const waiter = this.sendWaiters[0]!;
      const recvWaiter = this.recvWaiters.shift();
      if (recvWaiter !== undefined) {
        this.sendWaiters.shift();
        this.removeSenderAbortListener(waiter);
        if (recvWaiter.timer !== undefined) {
          clearTimeout(recvWaiter.timer);
        }
        recvWaiter.deferred.resolve(waiter.item);
        waiter.ack.resolve();
        continue;
      }
      if (this.items.length >= this.capacity) {
        break;
      }
      this.sendWaiters.shift();
      this.removeSenderAbortListener(waiter);
      this.items.push(waiter.item);
      waiter.ack.resolve();
    }
  }

  private removeSenderAbortListener(waiter: {
    signal?: AbortSignal;
    onAbort?: () => void;
  }): void {
    if (waiter.signal !== undefined && waiter.onAbort !== undefined) {
      waiter.signal.removeEventListener("abort", waiter.onAbort);
    }
  }
}

export class PendingBytesClosedError extends Error {}

export class PendingBytesExceedsCapacityError extends Error {
  constructor(readonly capacity: number) {
    super("pending bytes request exceeds capacity");
  }
}

export class PendingBytesReservation {
  private released = false;

  constructor(
    private readonly budget: PendingBytesBudget,
    readonly permits: number,
  ) {}

  release(): void {
    if (this.released) {
      return;
    }
    this.released = true;
    this.budget.release(this.permits);
  }
}

export class PendingBytesBudget {
  private available: number;
  private readonly waiters: Array<{
    requested: number;
    deferred: Deferred<PendingBytesReservation>;
    signal?: AbortSignal;
    onAbort?: () => void;
  }> = [];
  private closed = false;

  constructor(private readonly capacity: number) {
    this.available = capacity;
  }

  async acquire(
    requested: number,
    signal?: AbortSignal,
  ): Promise<PendingBytesReservation> {
    throwIfAborted(signal);
    if (requested > this.capacity) {
      throw new PendingBytesExceedsCapacityError(this.capacity);
    }
    if (this.closed) {
      throw new PendingBytesClosedError();
    }
    if (this.waiters.length === 0 && requested <= this.available) {
      this.available -= requested;
      return new PendingBytesReservation(this, requested);
    }

    const deferred = new Deferred<PendingBytesReservation>();
    const waiter: {
      requested: number;
      deferred: Deferred<PendingBytesReservation>;
      signal?: AbortSignal;
      onAbort?: () => void;
    } = { requested, deferred, signal };
    if (signal !== undefined) {
      waiter.onAbort = () => {
        const index = this.waiters.indexOf(waiter);
        if (index < 0) {
          return;
        }
        this.waiters.splice(index, 1);
        deferred.reject(abortReason(signal));
        this.drainWaiters();
      };
      signal.addEventListener("abort", waiter.onAbort, { once: true });
    }
    this.waiters.push(waiter);
    if (signal?.aborted === true) {
      waiter.onAbort?.();
    }
    try {
      return await deferred.promise;
    } finally {
      this.removeWaiterAbortListener(waiter);
    }
  }

  tryAcquire(requested: number): PendingBytesReservation | null {
    if (
      this.closed ||
      requested > this.capacity ||
      this.waiters.length > 0 ||
      requested > this.available
    ) {
      return null;
    }
    this.available -= requested;
    return new PendingBytesReservation(this, requested);
  }

  usedBytes(): number {
    return this.capacity - this.available;
  }

  release(permits: number): void {
    this.available += permits;
    this.drainWaiters();
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    while (this.waiters.length > 0) {
      const waiter = this.waiters.shift()!;
      this.removeWaiterAbortListener(waiter);
      waiter.deferred.reject(new PendingBytesClosedError());
    }
  }

  private drainWaiters(): void {
    while (this.waiters.length > 0) {
      if (this.closed) {
        return;
      }
      const next = this.waiters[0]!;
      if (next.requested > this.available) {
        return;
      }
      this.waiters.shift();
      this.removeWaiterAbortListener(next);
      this.available -= next.requested;
      next.deferred.resolve(new PendingBytesReservation(this, next.requested));
    }
  }

  private removeWaiterAbortListener(waiter: {
    signal?: AbortSignal;
    onAbort?: () => void;
  }): void {
    if (waiter.signal !== undefined && waiter.onAbort !== undefined) {
      waiter.signal.removeEventListener("abort", waiter.onAbort);
    }
  }
}

export function abortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("The operation was aborted", "AbortError");
}

export function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted === true) {
    throw abortReason(signal);
  }
}

const UTF8_ENCODER = new TextEncoder();

export function byteLength(payload: string): number {
  const buffer = (
    globalThis as typeof globalThis & {
      Buffer?: { byteLength(value: string, encoding: "utf8"): number };
    }
  ).Buffer;
  return buffer === undefined
    ? UTF8_ENCODER.encode(payload).byteLength
    : buffer.byteLength(payload, "utf8");
}

export function nextBackoff(currentMs: number, maxBackoffMs: number): number {
  if (currentMs === 0) {
    return 0;
  }
  return Math.min(currentMs * 2, maxBackoffMs);
}

export async function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (ms <= 0 || signal?.aborted === true) {
    return;
  }

  await new Promise<void>((resolve) => {
    let settled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const finish = (): void => {
      if (settled) {
        return;
      }
      settled = true;
      if (timer !== undefined) {
        clearTimeout(timer);
      }
      signal?.removeEventListener("abort", finish);
      resolve();
    };

    timer = setTimeout(finish, ms);
    signal?.addEventListener("abort", finish, { once: true });
    if (signal?.aborted === true) {
      finish();
    }
  });
}

export async function waitForAck<T>(
  deferred: Deferred<T>,
  makeClosedError: () => ScopeDBError,
): Promise<T> {
  try {
    return await deferred.promise;
  } catch (cause) {
    if (cause instanceof ScopeDBError) {
      throw cause;
    }
    throw makeClosedError();
  }
}
