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

const MIN_INTEGER = -(1n << 63n);
const MAX_INTEGER = (1n << 63n) - 1n;

/**
 * Encodes one plain object as JSON without a trailing newline. Signed 64-bit
 * bigints become exact number tokens. Non-finite numbers, unsafe integer-valued
 * numbers, undefined values, sparse arrays, cycles, and custom objects fail
 * locally with a `record_path` error context. Valid Dates become ISO strings.
 * This does not validate destination column types or the append request size.
 */
export function encodeAppendRecord(record: unknown): string {
  try {
    if (!isPlainObject(record)) {
      throw encodingError("$", "record must be a plain object");
    }
    return encode(record, "$", new Set());
  } catch (cause) {
    if (cause instanceof ScopeDBError) throw cause;
    throw new ScopeDBError("AppendRowsFailed", "failed to encode append record", {
      cause,
    });
  }
}

function encode(value: unknown, path: string, ancestors: Set<object>): string {
  if (value === null) return "null";
  switch (typeof value) {
    case "string":
    case "boolean":
      return JSON.stringify(value);
    case "number":
      if (!Number.isFinite(value)) {
        throw encodingError(path, "number must be finite");
      }
      if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
        throw encodingError(path, "integer-valued number must be safe; use bigint for exact integers");
      }
      return JSON.stringify(value);
    case "bigint":
      if (value < MIN_INTEGER || value > MAX_INTEGER) {
        throw encodingError(path, "bigint is outside the signed 64-bit Append input range");
      }
      return value.toString();
    case "object":
      break;
    default:
      throw encodingError(path, `unsupported ${typeof value} value; omit the property or use null explicitly`);
  }

  if (value instanceof Date) {
    const time = Date.prototype.getTime.call(value);
    if (!Number.isFinite(time)) throw encodingError(path, "Date must be valid");
    return JSON.stringify(Date.prototype.toISOString.call(value));
  }
  if (!Array.isArray(value) && !isPlainObject(value)) {
    throw encodingError(path, "expected a plain object, array, or Date; convert custom objects explicitly");
  }
  if (ancestors.has(value)) throw encodingError(path, "circular reference");
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      const items: string[] = [];
      for (let index = 0; index < value.length; index += 1) {
        const childPath = `${path}[${index}]`;
        items.push(encode(dataProperty(value, String(index), childPath), childPath, ancestors));
      }
      return `[${items.join(",")}]`;
    }
    if (Object.getOwnPropertySymbols(value).some((key) =>
      Object.prototype.propertyIsEnumerable.call(value, key)
    )) {
      throw encodingError(path, "object keys must be strings");
    }
    const fields = Object.keys(value).map((key) => {
      const encodedKey = JSON.stringify(key);
      const childPath = `${path}[${encodedKey}]`;
      return `${encodedKey}:${encode(dataProperty(value, key, childPath), childPath, ancestors)}`;
    });
    return `{${fields.join(",")}}`;
  } finally {
    ancestors.delete(value);
  }
}

function dataProperty(value: object, key: string, path: string): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  if (descriptor === undefined) throw encodingError(path, "sparse arrays are not supported; use null explicitly");
  if (!("value" in descriptor)) throw encodingError(path, "accessor properties are not supported; provide a data value");
  return descriptor.value;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null) return false;
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function encodingError(path: string, message: string): ScopeDBError {
  return new ScopeDBError("AppendRowsFailed", `invalid append record at ${path}: ${message}`)
    .withContext("record_path", path);
}
