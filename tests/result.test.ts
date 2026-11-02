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

import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { type DataType, FieldSchema, ResultSet, Schema, ScopeDBError } from "../src/index.js";

function makeResultSet(
  fields: Array<{ name: string; data_type: DataType }>,
  rows: Array<Array<string | null>>,
): ResultSet {
  return new ResultSet(
    new Schema(fields.map((field) => new FieldSchema(field.name, field.data_type))),
    rows.length,
    rows,
  );
}

describe("ResultSet schema and tabular accessors", () => {
  it("exposes schema metadata, rawRows, toValues, toObjects, and first", () => {
    const rs = makeResultSet(
      [
        { name: "id", data_type: "int" },
        { name: "score", data_type: "float" },
        { name: "label", data_type: "string" },
        { name: "active", data_type: "boolean" },
        { name: "created_at", data_type: "timestamp" },
        { name: "span", data_type: "interval" },
      ],
      [
        ["1", "9.5", "alpha", "true", "2024-06-01T12:00:00Z", "P1D"],
        ["2", "3.0", null, "false", null, null],
      ],
    );

    assert.equal(rs.numRows(), 2);
    const fields = rs.schema().fields();
    assert.equal(fields.length, 6);
    assert.equal(fields[0]!.name(), "id");
    assert.equal(fields[0]!.dataType(), "int");
    assert.deepEqual(rs.rawRows()[0], [
      "1",
      "9.5",
      "alpha",
      "true",
      "2024-06-01T12:00:00Z",
      "P1D",
    ]);

    const values = rs.toValues();
    assert.equal(values[0]![0], 1n);
    assert.equal(values[0]![1], 9.5);
    assert.equal(values[0]![2], "alpha");
    assert.equal(values[0]![3], true);
    assert.ok(values[0]![4] instanceof Date);
    assert.equal((values[0]![4] as Date).toISOString(), "2024-06-01T12:00:00.000Z");
    assert.equal(values[0]![5], "P1D");
    assert.equal(values[1]![2], null);
    assert.equal(values[1]![4], null);

    const objects = rs.toObjects();
    assert.equal(objects.length, 2);
    assert.equal(objects[0]!["id"], 1n);
    assert.equal(objects[0]!["label"], "alpha");
    assert.equal(objects[1]!["label"], null);
    assert.deepEqual(rs.first(), objects[0]);

    const empty = makeResultSet([{ name: "id", data_type: "int" }], []);
    assert.deepEqual(empty.toObjects(), []);
    assert.equal(empty.first(), null);
  });

  it("retains exact 64-bit integer boundaries, special floats, and prototype-like column names", () => {
    const result = makeResultSet(
      [
        { name: "min", data_type: "int" },
        { name: "max", data_type: "uint" },
        { name: "nan", data_type: "float" },
        { name: "__proto__", data_type: "float" },
      ],
      [["-9223372036854775808", "18446744073709551615", "NaN", "-inf"]],
    );
    const row = result.first()!;
    assert.equal(row["min"], -(1n << 63n));
    assert.equal(row["max"], (1n << 64n) - 1n);
    assert.ok(Number.isNaN(row["nan"]));
    assert.equal(row["__proto__"], -Infinity);
  });

  it("rejects duplicate column names in toObjects and first", () => {
    const rs = makeResultSet(
      [
        { name: "value", data_type: "int" },
        { name: "value", data_type: "int" },
      ],
      [["1", "2"]],
    );
    assert.throws(() => rs.toObjects(), /column name .* is duplicated/);
    assert.throws(() => rs.first(), /column name .* is duplicated/);
    assert.deepEqual(rs.toValues(), [[1n, 2n]]);
  });

  it("preserves structured cells as text and raw timestamp precision", () => {
    const timestamp = "2026-10-29T00:00:00.123456789Z";
    const object = '{"id":9007199254740993}';
    const result = makeResultSet(
      [
        { name: "attrs", data_type: "object" },
        { name: "items", data_type: "array" },
        { name: "value", data_type: "any" },
        { name: "time", data_type: "timestamp" },
      ],
      [[object, "[1,null]", "true", timestamp]],
    );

    assert.deepEqual(result.first({ integerMode: "string" }), {
      attrs: object,
      items: "[1,null]",
      value: "true",
      time: new Date("2026-10-29T00:00:00.123Z"),
    });
    assert.equal(result.rawRows()[0]![3], timestamp);
  });

  it("rejects malformed cells and mismatched row lengths with contextual ScopeDBError", () => {
    const badCases: Array<[DataType, string]> = [
      ["int", "not-a-number"],
      ["uint", "-1"],
      ["float", "invalid-float"],
      ["boolean", "yes"],
      ["timestamp", "not-a-date"],
    ];
    for (const [dataType, cell] of badCases) {
      const rs = makeResultSet([{ name: "col", data_type: dataType }], [[cell]]);
      assert.throws(
        () => rs.toValues(),
        (err: unknown) =>
          err instanceof ScopeDBError && err.context().get("column") === "col",
      );
    }

    const mismatched = makeResultSet(
      [
        { name: "a", data_type: "string" },
        { name: "b", data_type: "string" },
      ],
      [["only-one-cell"]],
    );
    assert.throws(() => mismatched.toValues(), ScopeDBError);
    assert.throws(() => mismatched.toObjects(), ScopeDBError);
  });
});

describe("ResultSet integerMode option", () => {
  const intRow = (cell: string, dataType: "int" | "uint" = "int") =>
    makeResultSet([{ name: "n", data_type: dataType }], [[cell]]);

  it("supports bigint (default), number, and string representations", () => {
    assert.equal(intRow("42").first()!["n"], 42n);
    assert.equal(intRow("42").first({ integerMode: "number" })!["n"], 42);
    assert.equal(intRow("-7").first({ integerMode: "number" })!["n"], -7);
    assert.equal(intRow("7", "uint").first({ integerMode: "number" })!["n"], 7);
    assert.equal(
      intRow(String(Number.MAX_SAFE_INTEGER)).first({ integerMode: "number" })!["n"],
      Number.MAX_SAFE_INTEGER,
    );
    assert.equal(
      JSON.stringify(intRow("123").toObjects({ integerMode: "number" })),
      '[{"n":123}]',
    );

    const i64Max = "9223372036854775807";
    assert.equal(intRow(i64Max).first({ integerMode: "string" })!["n"], i64Max);
    assert.equal(
      JSON.stringify(intRow(i64Max).toObjects({ integerMode: "string" })),
      `[{"n":"${i64Max}"}]`,
    );

    const unsafe = intRow("9007199254740993");
    assert.equal(unsafe.first()!["n"], 9007199254740993n);
    assert.equal(unsafe.first({ integerMode: "string" })!["n"], "9007199254740993");
    assert.equal(unsafe.first({ integerMode: "number" })!["n"], 9007199254740992);
  });

  it("rejects non-integer strings and unknown integerMode values", () => {
    for (const bad of ["1.5", "1e3", "", " ", " 42", "42 ", "+42"]) {
      assert.throws(
        () => intRow(bad).toValues({ integerMode: "number" }),
        ScopeDBError,
      );
    }
    const badOptions = { integerMode: "decimal" } as unknown as {
      integerMode: "bigint";
    };
    assert.throws(() => intRow("42").toValues(badOptions), ScopeDBError);
  });
});
