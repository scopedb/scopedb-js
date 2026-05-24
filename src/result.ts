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
import type { DataType, StatementResultSet } from "./protocol.js";

export type Value = bigint | number | boolean | string | Date | null;

/**
 * How to represent integer values returned from `int` / `uint` columns.
 *
 * - `'bigint'` (default): native JS `bigint`, preserves I64 precision but is
 *   NOT directly JSON-serializable (`JSON.stringify` will throw
 *   `TypeError: Do not know how to serialize a BigInt`).
 * - `'number'`: JS `number`. Fast and JSON-safe, but loses precision for
 *   values outside the safe integer range (`Number.MIN_SAFE_INTEGER` to
 *   `Number.MAX_SAFE_INTEGER`, i.e. `±(2**53 - 1)`). Safe for typical
 *   `count(...)` results and bounded counters; not safe for unbounded I64
 *   identifiers. Throws if the cell is not a base-10 integer string.
 * - `'string'`: decimal string. Always safe; callers can `BigInt(s)` on read.
 */
export type IntegerMode = "bigint" | "number" | "string";

export interface IntoOptions {
  /** How to represent `int` / `uint` cell values. Defaults to `'bigint'`. */
  integerMode?: IntegerMode;
}

export class FieldSchema {
  constructor(
    private readonly fieldName: string,
    private readonly fieldDataType: DataType,
  ) {}

  name(): string {
    return this.fieldName;
  }

  dataType(): DataType {
    return this.fieldDataType;
  }
}

export class Schema {
  constructor(private readonly schemaFields: FieldSchema[]) {}

  fields(): readonly FieldSchema[] {
    return this.schemaFields;
  }
}

export class ResultSet {
  constructor(
    private readonly resultSchema: Schema,
    private readonly totalRows: number,
    private readonly rows: Array<Array<string | null>>,
  ) {}

  numRows(): number {
    return this.totalRows;
  }

  schema(): Schema {
    return this.resultSchema;
  }

  jsonRows(): ReadonlyArray<ReadonlyArray<string | null>> {
    return this.rows;
  }

  intoValues(options: IntoOptions = {}): Value[][] {
    const integerMode = options.integerMode ?? "bigint";
    return this.rows.map((row) => {
      const fields = this.resultSchema.fields();
      if (row.length !== fields.length) {
        throw new ScopeDBError(
          "Unexpected",
          `row field count mismatch: expected ${fields.length}, got ${row.length}`,
        );
      }
      return row.map((cell, index) =>
        parseCell(cell, fields[index]!.dataType(), integerMode),
      );
    });
  }

  /**
   * Returns all rows as plain objects keyed by column name.
   *
   * This is the most convenient form for typical application code — use this
   * instead of `intoValues()` when you need to access columns by name.
   *
   * @example
   * // Default: integer cells come back as bigint (preserves I64 precision,
   * // NOT directly JSON-serializable).
   * const rows = result.intoObjects();
   * console.log(rows[0]?.["user_id"]); // bigint
   *
   * @example
   * // Opt in to JSON-safe representation for integer cells.
   * const rows = result.intoObjects({ integerMode: "number" });
   * JSON.stringify(rows[0]); // safe
   */
  intoObjects(options: IntoOptions = {}): Record<string, Value>[] {
    const integerMode = options.integerMode ?? "bigint";
    const fields = this.resultSchema.fields();
    return this.rows.map((row) => {
      if (row.length !== fields.length) {
        throw new ScopeDBError(
          "Unexpected",
          `row field count mismatch: expected ${fields.length}, got ${row.length}`,
        );
      }
      const obj: Record<string, Value> = {};
      row.forEach((cell, i) => {
        obj[fields[i]!.name()] = parseCell(cell, fields[i]!.dataType(), integerMode);
      });
      return obj;
    });
  }

  /**
   * Returns the first row as a plain object keyed by column name, or `null`
   * if the result set is empty.
   *
   * Useful for queries that return at most one row (lookups, aggregates, etc.).
   *
   * @example
   * const row = result.first();
   * if (row !== null) {
   *   console.log(row["count"]); // bigint
   * }
   *
   * @example
   * // JSON-safe integer cells.
   * const row = result.first({ integerMode: "number" });
   * JSON.stringify(row); // safe
   */
  first(options: IntoOptions = {}): Record<string, Value> | null {
    const integerMode = options.integerMode ?? "bigint";
    const row = this.rows[0];
    if (row === undefined) {
      return null;
    }
    const fields = this.resultSchema.fields();
    if (row.length !== fields.length) {
      throw new ScopeDBError(
        "Unexpected",
        `row field count mismatch: expected ${fields.length}, got ${row.length}`,
      );
    }
    const obj: Record<string, Value> = {};
    row.forEach((cell, i) => {
      obj[fields[i]!.name()] = parseCell(cell, fields[i]!.dataType(), integerMode);
    });
    return obj;
  }

  static fromStatementResultSet(resultSet: StatementResultSet): ResultSet {
    return new ResultSet(
      new Schema(
        resultSet.metadata.fields.map(
          (field) => new FieldSchema(field.name, field.data_type),
        ),
      ),
      resultSet.metadata.num_rows,
      resultSet.rows,
    );
  }
}

function parseCell(
  cell: string | null,
  dataType: DataType,
  integerMode: IntegerMode = "bigint",
): Value {
  if (cell === null) {
    return null;
  }

  switch (dataType) {
    case "int":
    case "uint":
    case "u_int": // backward-compat alias
      return parseInteger(cell, integerMode);
    case "float": {
      const value = Number(cell);
      if (Number.isNaN(value)) {
        throw new ScopeDBError("Unexpected", `failed to parse float value: ${cell}`);
      }
      return value;
    }
    case "timestamp": {
      const value = new Date(cell);
      if (Number.isNaN(value.getTime())) {
        throw new ScopeDBError("Unexpected", `failed to parse timestamp value: ${cell}`);
      }
      return value;
    }
    case "boolean":
      if (cell === "true") {
        return true;
      }
      if (cell === "false") {
        return false;
      }
      throw new ScopeDBError("Unexpected", `failed to parse boolean value: ${cell}`);
    case "interval":
    case "string":
    case "binary":
    case "array":
    case "object":
    case "any":
    case "null":
      return cell;
  }
}

const INTEGER_CELL_RE = /^-?\d+$/;

function parseInteger(cell: string, integerMode: IntegerMode): Value {
  switch (integerMode) {
    case "bigint":
      try {
        return BigInt(cell);
      } catch (cause) {
        throw new ScopeDBError("Unexpected", `failed to parse integer value: ${cell}`, {
          cause,
        });
      }
    case "number": {
      // Constrain to base-10 integer strings so non-integer forms accepted by
      // `Number()` (e.g. "1.5", "1e3", " ", "") cannot silently slip through
      // for `int` / `uint` columns. Precision loss for values outside the
      // safe-integer range is the user's explicit opt-in and is intentionally
      // NOT validated here.
      if (!INTEGER_CELL_RE.test(cell)) {
        throw new ScopeDBError("Unexpected", `failed to parse integer value: ${cell}`);
      }
      const value = Number(cell);
      if (!Number.isFinite(value)) {
        throw new ScopeDBError("Unexpected", `failed to parse integer value: ${cell}`);
      }
      return value;
    }
    case "string":
      return cell;
  }
}
