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
 * - `'bigint'` (default): native JS `bigint`, preserves signed/unsigned 64-bit precision but is
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

export interface ResultOptions {
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

  rawRows(): ReadonlyArray<ReadonlyArray<string | null>> {
    return this.rows;
  }

  toValues(options: ResultOptions = {}): Value[][] {
    return this.rows.map((row, index) => this.parseRow(row, index, options));
  }

  /** Returns objects keyed by column name; integer cells default to bigint. */
  toObjects(options: ResultOptions = {}): Record<string, Value>[] {
    validateUniqueFieldNames(this.resultSchema.fields());
    return this.toValues(options).map((row) => this.rowObject(row));
  }

  /** Returns the first object, or null for an empty result. */
  first(options: ResultOptions = {}): Record<string, Value> | null {
    if (this.rows.length === 0) return null;
    validateUniqueFieldNames(this.resultSchema.fields());
    return this.rowObject(this.parseRow(this.rows[0]!, 0, options));
  }

  private rowObject(row: Value[]): Record<string, Value> {
    return Object.fromEntries(
      this.resultSchema.fields().map((
        field,
        index,
      ) => [field.name(), row[index]!]),
    );
  }

  private parseRow(
    row: Array<string | null>,
    rowIndex: number,
    options: ResultOptions,
  ): Value[] {
    const fields = this.resultSchema.fields();
    if (row.length !== fields.length) {
      throw new ScopeDBError(
        "Unexpected",
        `row field count mismatch: expected ${fields.length}, got ${row.length}`,
      ).withContext("row_index", rowIndex);
    }
    return row.map((cell, index) => {
      const field = fields[index]!;
      try {
        return parseCell(
          cell,
          field.dataType(),
          options.integerMode ?? "bigint",
        );
      } catch (cause) {
        throw new ScopeDBError(
          "Unexpected",
          `failed to decode row ${rowIndex}, column ${
            JSON.stringify(field.name())
          }`,
          { cause },
        )
          .withContext("row_index", rowIndex).withContext(
            "column",
            field.name(),
          ).withContext("data_type", field.dataType());
      }
    });
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

function validateUniqueFieldNames(fields: readonly FieldSchema[]): void {
  const names = new Set<string>();
  for (const field of fields) {
    if (names.has(field.name())) {
      throw new ScopeDBError(
        "Unexpected",
        `cannot convert result to objects because column name ${
          JSON.stringify(field.name())
        } is duplicated; use toValues() instead`,
      );
    }
    names.add(field.name());
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
      return parseInteger(cell, dataType, integerMode);
    case "float": {
      if (cell === "NaN") return NaN;
      if (cell === "inf" || cell === "Infinity") return Infinity;
      if (cell === "-inf" || cell === "-Infinity") return -Infinity;
      const value = Number(cell);
      if (cell.trim() === "" || !Number.isFinite(value)) {
        throw new ScopeDBError(
          "Unexpected",
          `failed to parse float value: ${cell}`,
        );
      }
      return value;
    }
    case "timestamp": {
      const value = new Date(cell);
      if (Number.isNaN(value.getTime())) {
        throw new ScopeDBError(
          "Unexpected",
          `failed to parse timestamp value: ${cell}`,
        );
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
      throw new ScopeDBError(
        "Unexpected",
        `failed to parse boolean value: ${cell}`,
      );
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

function parseInteger(
  cell: string,
  dataType: "int" | "uint",
  integerMode: IntegerMode,
): Value {
  if (!INTEGER_CELL_RE.test(cell)) {
    throw new ScopeDBError("Unexpected", `invalid ${dataType} value: ${cell}`);
  }
  const integer = BigInt(cell);
  const min = dataType === "uint" ? 0n : -(1n << 63n);
  const max = dataType === "uint" ? (1n << 64n) - 1n : (1n << 63n) - 1n;
  if (integer < min || integer > max) {
    throw new ScopeDBError(
      "Unexpected",
      `${dataType} value out of range: ${cell}`,
    );
  }
  switch (integerMode) {
    case "bigint":
      return integer;
    case "number": {
      // Precision loss outside the safe-integer range is an explicit caller choice.
      const value = Number(integer);
      return value;
    }
    case "string":
      return cell;
    default: {
      const exhaustive: never = integerMode;
      throw new ScopeDBError(
        "Unexpected",
        `unknown integerMode: ${String(exhaustive)}`,
      );
    }
  }
}
