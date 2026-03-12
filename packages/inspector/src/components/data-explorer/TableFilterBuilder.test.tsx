import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { TableFilterBuilder, type TableFilterClause } from "./TableFilterBuilder";
import type { ColumnDescriptor } from "jazz-tools";

const schemaColumns = [
  { name: "title", column_type: { type: "Text" }, nullable: false },
  { name: "done", column_type: { type: "Boolean" }, nullable: false },
  { name: "count", column_type: { type: "Integer" }, nullable: false },
  { name: "assignee_id", column_type: { type: "Uuid" }, nullable: true, references: "users" },
  { name: "meta", column_type: { type: "Row", columns: [] }, nullable: true },
] satisfies ColumnDescriptor[];

describe("TableFilterBuilder", () => {
  afterEach(() => {
    cleanup();
  });

  it("derives operator options from jazz-tools support", () => {
    const onClausesChange = vi.fn();
    render(
      <TableFilterBuilder
        schemaColumns={[...schemaColumns]}
        clauses={[]}
        onClausesChange={onClausesChange}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /Filter/ }));
    fireEvent.change(screen.getByLabelText("Column"), { target: { value: "done" } });
    const operatorSelect = screen.getByLabelText("Operator");
    const booleanOperators = Array.from(operatorSelect.querySelectorAll("option")).map((option) =>
      option.getAttribute("value"),
    );
    expect(booleanOperators).toEqual(["eq"]);

    fireEvent.change(screen.getByLabelText("Column"), { target: { value: "assignee_id" } });
    const refOperators = Array.from(operatorSelect.querySelectorAll("option")).map((option) =>
      option.getAttribute("value"),
    );
    expect(refOperators).toEqual(["eq", "ne", "isNull"]);
  });

  it("does not show unsupported columns", () => {
    const onClausesChange = vi.fn();
    render(
      <TableFilterBuilder
        schemaColumns={[...schemaColumns]}
        clauses={[]}
        onClausesChange={onClausesChange}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /Filter/ }));
    const columnSelect = screen.getByLabelText("Column");
    const columnNames = Array.from(columnSelect.querySelectorAll("option")).map((option) =>
      option.getAttribute("value"),
    );
    expect(columnNames).toEqual(["id", "title", "done", "count", "assignee_id"]);
  });

  it("parses numeric values for numeric operators", () => {
    const onClausesChange = vi.fn();
    render(
      <TableFilterBuilder
        schemaColumns={[...schemaColumns]}
        clauses={[]}
        onClausesChange={onClausesChange}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /Filter/ }));
    fireEvent.change(screen.getByLabelText("Column"), { target: { value: "count" } });
    fireEvent.change(screen.getByLabelText("Operator"), { target: { value: "gt" } });
    fireEvent.change(screen.getByLabelText("Value"), { target: { value: "3" } });
    fireEvent.click(screen.getByRole("button", { name: "Add where clause" }));

    expect(onClausesChange).toHaveBeenCalledTimes(1);
    const clauses = onClausesChange.mock.calls[0]?.[0] as TableFilterClause[];
    expect(clauses[0]).toMatchObject({
      column: "count",
      operator: "gt",
      value: 3,
    });
  });

  it("shows validation errors for invalid values", () => {
    const onClausesChange = vi.fn();
    render(
      <TableFilterBuilder
        schemaColumns={[...schemaColumns]}
        clauses={[]}
        onClausesChange={onClausesChange}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /Filter/ }));
    fireEvent.change(screen.getByLabelText("Column"), { target: { value: "done" } });
    fireEvent.change(screen.getByLabelText("Value"), { target: { value: "not-boolean" } });
    fireEvent.click(screen.getByRole("button", { name: "Add where clause" }));

    expect(screen.getByText('Boolean values must be "true" or "false".')).not.toBeNull();
    expect(onClausesChange).not.toHaveBeenCalled();
  });

  it("parses isNull=false as a boolean false value", () => {
    const onClausesChange = vi.fn();
    render(
      <TableFilterBuilder
        schemaColumns={[...schemaColumns]}
        clauses={[]}
        onClausesChange={onClausesChange}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /Filter/ }));
    fireEvent.change(screen.getByLabelText("Column"), { target: { value: "assignee_id" } });
    fireEvent.change(screen.getByLabelText("Operator"), { target: { value: "isNull" } });
    fireEvent.change(screen.getByLabelText("Value"), { target: { value: "false" } });
    fireEvent.click(screen.getByRole("button", { name: "Add where clause" }));

    expect(onClausesChange).toHaveBeenCalledTimes(1);
    const clauses = onClausesChange.mock.calls[0]?.[0] as TableFilterClause[];
    expect(clauses[0]).toMatchObject({
      column: "assignee_id",
      operator: "isNull",
      value: false,
    });
  });

  it("removes an existing clause", () => {
    const onClausesChange = vi.fn();
    const clauses: TableFilterClause[] = [
      { id: "a", column: "title", operator: "contains", value: "alpha" },
      { id: "b", column: "count", operator: "gt", value: 1 },
    ];
    render(
      <TableFilterBuilder
        schemaColumns={[...schemaColumns]}
        clauses={clauses}
        onClausesChange={onClausesChange}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /Filter/ }));
    fireEvent.click(screen.getByRole("button", { name: "Remove filter on title" }));
    expect(onClausesChange).toHaveBeenCalledWith([
      { id: "b", column: "count", operator: "gt", value: 1 },
    ]);
  });

  it("shows compact AND summary when filters are active", () => {
    const onClausesChange = vi.fn();
    const clauses: TableFilterClause[] = [
      { id: "a", column: "title", operator: "contains", value: "alpha" },
      { id: "b", column: "count", operator: "gt", value: 1 },
    ];
    render(
      <TableFilterBuilder
        schemaColumns={[...schemaColumns]}
        clauses={clauses}
        onClausesChange={onClausesChange}
      />,
    );

    expect(screen.getByRole("button", { name: "Filter (2)" })).not.toBeNull();
    expect(screen.getByText("title contains alpha AND count gt 1")).not.toBeNull();
  });
});
