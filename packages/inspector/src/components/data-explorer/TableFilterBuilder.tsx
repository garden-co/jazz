import { useEffect, useMemo, useRef, useState } from "react";
import type { ColumnDescriptor, ColumnType } from "jazz-tools";
import {
  getSupportedWhereOperatorsForColumn,
  type WhereOperator,
} from "../../utility/where-operators.js";
import styles from "./TableFilterBuilder.module.css";

type FilterOperator = WhereOperator;

export interface TableFilterClause {
  id: string;
  column: string;
  operator: FilterOperator;
  value: unknown;
}

interface FilterableColumn {
  name: string;
  columnType: ColumnType;
  nullable: boolean;
  references?: string;
  implicitId?: boolean;
}

interface TableFilterBuilderProps {
  schemaColumns: ColumnDescriptor[];
  clauses: TableFilterClause[];
  onClausesChange: (next: TableFilterClause[]) => void;
}

interface DraftState {
  column: string;
  operator: FilterOperator;
  valueText: string;
}

function parseBoolean(value: string): boolean | null {
  const normalized = value.trim().toLowerCase();
  if (normalized === "true" || normalized === "1") return true;
  if (normalized === "false" || normalized === "0") return false;
  return null;
}

function parseBytea(value: string): Uint8Array {
  const parts = value
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
  const bytes = parts.map((part) => {
    const n = Number(part);
    if (!Number.isInteger(n) || n < 0 || n > 255) {
      throw new Error("Bytea bytes must be integers in range 0..255.");
    }
    return n;
  });
  return new Uint8Array(bytes);
}

function parseJsonValue(value: string): unknown {
  const trimmed = value.trim();
  if (trimmed.length === 0) return "";
  try {
    return JSON.parse(trimmed) as unknown;
  } catch {
    return trimmed;
  }
}

function parseScalarValue(columnType: ColumnType, value: string): unknown {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error("Value is required.");
  }

  switch (columnType.type) {
    case "Boolean": {
      const parsed = parseBoolean(trimmed);
      if (parsed === null) {
        throw new Error('Boolean values must be "true" or "false".');
      }
      return parsed;
    }
    case "Integer":
    case "BigInt":
    case "Double": {
      const parsed = Number(trimmed);
      if (!Number.isFinite(parsed)) {
        throw new Error("Numeric values must be finite numbers.");
      }
      return parsed;
    }
    case "Bytea":
      return parseBytea(trimmed);
    case "Json":
      return parseJsonValue(trimmed);
    case "Array":
      return JSON.parse(trimmed);
    case "Enum":
      if (!columnType.variants.includes(trimmed)) {
        throw new Error(`Expected one of: ${columnType.variants.join(", ")}`);
      }
      return trimmed;
    default:
      return trimmed;
  }
}

function parseFilterValue(
  column: FilterableColumn,
  operator: FilterOperator,
  value: string,
): unknown {
  if (operator === "isNull") {
    const parsed = parseBoolean(value);
    if (parsed === null) {
      throw new Error('isNull value must be "true" or "false".');
    }
    return parsed;
  }

  if (operator === "in") {
    const items = value
      .split(",")
      .map((item) => item.trim())
      .filter((item) => item.length > 0);
    if (items.length === 0) {
      throw new Error('The "in" operator requires at least one value.');
    }
    return items.map((item) => parseScalarValue(column.columnType, item));
  }

  if (operator === "contains" && column.columnType.type === "Array") {
    return parseScalarValue(column.columnType.element, value);
  }

  return parseScalarValue(column.columnType, value);
}

function formatClauseValue(value: unknown): string {
  if (value instanceof Uint8Array) return `[${Array.from(value).join(", ")}]`;
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}

function createClauseId(): string {
  return `filter-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export function TableFilterBuilder({
  schemaColumns,
  clauses,
  onClausesChange,
}: TableFilterBuilderProps) {
  const [isModalOpen, setIsModalOpen] = useState(false);
  const dialogRef = useRef<HTMLDialogElement | null>(null);
  const canUseModalApi =
    typeof HTMLDialogElement !== "undefined" &&
    typeof HTMLDialogElement.prototype.showModal === "function";
  const filterableColumns = useMemo<FilterableColumn[]>(
    () =>
      [
        {
          name: "id",
          columnType: { type: "Uuid" } as const,
          nullable: false,
          implicitId: true,
        },
        ...schemaColumns.map((column) => ({
          name: column.name,
          columnType: column.column_type,
          nullable: column.nullable,
          references: column.references,
        })),
      ].filter((column) => getSupportedWhereOperatorsForColumn(column).length > 0),
    [schemaColumns],
  );

  const initialColumn = filterableColumns[0]?.name ?? "id";
  const initialOperators = filterableColumns[0]
    ? getSupportedWhereOperatorsForColumn(filterableColumns[0])
    : [];
  const [draft, setDraft] = useState<DraftState>({
    column: initialColumn,
    operator: initialOperators[0] ?? "eq",
    valueText: "",
  });
  const [error, setError] = useState<string | null>(null);

  const selectedColumn = filterableColumns.find((column) => column.name === draft.column) ?? null;
  const operatorOptions = selectedColumn ? getSupportedWhereOperatorsForColumn(selectedColumn) : [];

  const shouldHideValueInput = draft.operator === "isNull" && selectedColumn !== null;
  const activeFiltersSummary = clauses
    .map((clause) => `${clause.column} ${clause.operator} ${formatClauseValue(clause.value)}`)
    .join(" AND ");
  const filterButtonLabel = clauses.length > 0 ? `Filter (${clauses.length})` : "Filter";

  useEffect(() => {
    if (!canUseModalApi) return;
    const dialog = dialogRef.current;
    if (!dialog) return;
    if (isModalOpen && !dialog.open) {
      dialog.showModal();
    } else if (!isModalOpen && dialog.open) {
      dialog.close();
    }
  }, [isModalOpen, canUseModalApi]);

  return (
    <section className={styles.container}>
      <button
        type="button"
        className={`${styles.button} ${styles.triggerButton}`}
        onClick={() => setIsModalOpen(true)}
      >
        {filterButtonLabel}
      </button>
      {clauses.length > 0 ? (
        <div className={styles.summaryBox}>
          <code className={styles.summary}>{activeFiltersSummary}</code>
        </div>
      ) : null}
      <dialog
        ref={dialogRef}
        className={styles.modal}
        {...(!canUseModalApi ? { open: isModalOpen } : {})}
        onClose={() => setIsModalOpen(false)}
        aria-label="Filter rows"
      >
        <form
          className={styles.form}
          onSubmit={(event) => {
            event.preventDefault();
            if (!selectedColumn) return;
            if (!operatorOptions.includes(draft.operator)) return;

            try {
              const clause = {
                id: createClauseId(),
                column: selectedColumn.name,
                operator: draft.operator,
                value: parseFilterValue(selectedColumn, draft.operator, draft.valueText),
              } satisfies TableFilterClause;
              onClausesChange([...clauses, clause]);
              setDraft((current) => ({
                ...current,
                valueText: current.operator === "isNull" ? "true" : "",
              }));
              setError(null);
            } catch (parseError) {
              setError(parseError instanceof Error ? parseError.message : "Invalid filter value.");
            }
          }}
        >
          <label className={styles.field}>
            Column
            <select
              className={styles.select}
              value={draft.column}
              onChange={(event) => {
                const nextColumn = filterableColumns.find(
                  (column) => column.name === event.target.value,
                );
                const nextOperators = nextColumn
                  ? getSupportedWhereOperatorsForColumn(nextColumn)
                  : [];
                const nextOperator = nextOperators[0] ?? "eq";
                setDraft({
                  column: event.target.value,
                  operator: nextOperator,
                  valueText: nextOperator === "isNull" ? "true" : "",
                });
                setError(null);
              }}
            >
              {filterableColumns.map((column) => (
                <option key={column.name} value={column.name}>
                  {column.name}
                </option>
              ))}
            </select>
          </label>
          <label className={styles.field}>
            Operator
            <select
              className={styles.select}
              value={draft.operator}
              onChange={(event) => {
                const nextOperator = event.target.value as FilterOperator;
                setDraft((current) => ({
                  ...current,
                  operator: nextOperator,
                  valueText: nextOperator === "isNull" ? "true" : current.valueText,
                }));
                setError(null);
              }}
            >
              {operatorOptions.map((operator) => (
                <option key={operator} value={operator}>
                  {operator}
                </option>
              ))}
            </select>
          </label>
          {!shouldHideValueInput ? (
            <label className={styles.field}>
              Value
              <input
                className={styles.input}
                value={draft.valueText}
                placeholder={draft.operator === "in" ? "value1,value2" : "value"}
                onChange={(event) => {
                  setDraft((current) => ({ ...current, valueText: event.target.value }));
                  setError(null);
                }}
              />
            </label>
          ) : (
            <label className={styles.field}>
              Value
              <select
                className={styles.select}
                value={draft.valueText || "true"}
                onChange={(event) =>
                  setDraft((current) => ({ ...current, valueText: event.target.value }))
                }
              >
                <option value="true">true</option>
                <option value="false">false</option>
              </select>
            </label>
          )}
          <button type="submit" className={styles.button} disabled={operatorOptions.length === 0}>
            Add where clause
          </button>
          <button type="button" className={styles.button} onClick={() => setIsModalOpen(false)}>
            Close
          </button>
        </form>
        {error ? <p className={styles.error}>{error}</p> : null}
        <div className={styles.clauses}>
          {clauses.length === 0 ? (
            <p className={styles.empty}>No filters</p>
          ) : (
            clauses.map((clause) => (
              <div key={clause.id} className={styles.clause}>
                <code className={styles.clauseText}>
                  {clause.column} {clause.operator} {formatClauseValue(clause.value)}
                </code>
                <button
                  type="button"
                  className={styles.removeButton}
                  onClick={() => onClausesChange(clauses.filter((entry) => entry.id !== clause.id))}
                  aria-label={`Remove filter on ${clause.column}`}
                >
                  Remove
                </button>
              </div>
            ))
          )}
        </div>
      </dialog>
    </section>
  );
}
