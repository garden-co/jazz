import {
  forwardRef,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type FormEvent,
} from "react";
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
  showTrigger?: boolean;
}

export interface TableFilterBuilderHandle {
  open: () => void;
  close: () => void;
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

function createDraftState(columnName: string, filterableColumns: FilterableColumn[]): DraftState {
  const selectedColumn = filterableColumns.find((column) => column.name === columnName);
  const operatorOptions = selectedColumn ? getSupportedWhereOperatorsForColumn(selectedColumn) : [];
  const operator = operatorOptions[0] ?? "eq";

  return {
    column: columnName,
    operator,
    valueText: operator === "isNull" ? "true" : "",
  };
}

function createInitialDraftState(filterableColumns: FilterableColumn[]): DraftState {
  return createDraftState(filterableColumns[0]?.name ?? "id", filterableColumns);
}

export const TableFilterBuilder = forwardRef<TableFilterBuilderHandle, TableFilterBuilderProps>(
  function TableFilterBuilder(
    { schemaColumns, clauses, onClausesChange, showTrigger = true }: TableFilterBuilderProps,
    ref,
  ) {
    const dialogRef = useRef<HTMLDialogElement | null>(null);
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

    const [draft, setDraft] = useState<DraftState>(() =>
      createInitialDraftState(filterableColumns),
    );
    const [error, setError] = useState<string | null>(null);

    const selectedColumn = filterableColumns.find((column) => column.name === draft.column) ?? null;
    const operatorOptions = selectedColumn
      ? getSupportedWhereOperatorsForColumn(selectedColumn)
      : [];

    const shouldHideValueInput = draft.operator === "isNull" && selectedColumn !== null;
    const activeFiltersSummary = clauses
      .map((clause) => `${clause.column} ${clause.operator} ${formatClauseValue(clause.value)}`)
      .join(" AND ");
    const filterButtonLabel = clauses.length > 0 ? `Filter (${clauses.length})` : "Filter";

    const openDialog = () => {
      const dialog = dialogRef.current;
      if (!dialog) return;
      if (!dialog.open) {
        setError(null);
        dialog.showModal();
      }
    };

    const closeDialog = () => {
      const dialog = dialogRef.current;
      if (!dialog) return;
      if (dialog.open) {
        dialog.close();
      }
      setError(null);
    };

    useImperativeHandle(
      ref,
      () => ({
        open: openDialog,
        close: closeDialog,
      }),
      [closeDialog],
    );

    const handleAddClause = (event: FormEvent<HTMLFormElement>) => {
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
    };

    const handleColumnChange = (event: ChangeEvent<HTMLSelectElement>) => {
      setDraft(createDraftState(event.target.value, filterableColumns));
      setError(null);
    };

    const handleOperatorChange = (event: ChangeEvent<HTMLSelectElement>) => {
      const nextOperator = event.target.value as FilterOperator;
      setDraft((current) => ({
        ...current,
        operator: nextOperator,
        valueText: nextOperator === "isNull" ? "true" : current.valueText,
      }));
      setError(null);
    };

    const handleValueChange = (event: ChangeEvent<HTMLInputElement>) => {
      setDraft((current) => ({ ...current, valueText: event.target.value }));
      setError(null);
    };

    const handleNullValueChange = (event: ChangeEvent<HTMLSelectElement>) => {
      setDraft((current) => ({ ...current, valueText: event.target.value }));
    };

    const handleRemoveClause = (clauseId: string) => {
      onClausesChange(clauses.filter((entry) => entry.id !== clauseId));
    };

    return (
      <>
        <section className={styles.container}>
          {showTrigger ? (
            <button
              type="button"
              className={`${styles.button} ${styles.triggerButton}`}
              onClick={openDialog}
            >
              {filterButtonLabel}
            </button>
          ) : null}
          {clauses.length > 0 ? (
            <div className={styles.summaryBox}>
              <code className={styles.summary}>{activeFiltersSummary}</code>
            </div>
          ) : null}
        </section>
        <dialog
          ref={dialogRef}
          className={styles.modal}
          onClose={() => setError(null)}
          aria-label="Filter rows"
        >
          <form className={styles.form} onSubmit={handleAddClause}>
            <label className={styles.field}>
              Column
              <select className={styles.select} value={draft.column} onChange={handleColumnChange}>
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
                onChange={handleOperatorChange}
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
                  onChange={handleValueChange}
                />
              </label>
            ) : (
              <label className={styles.field}>
                Value
                <select
                  className={styles.select}
                  value={draft.valueText || "true"}
                  onChange={handleNullValueChange}
                >
                  <option value="true">true</option>
                  <option value="false">false</option>
                </select>
              </label>
            )}
            <button type="submit" className={styles.button} disabled={operatorOptions.length === 0}>
              Add where clause
            </button>
            <button type="button" className={styles.button} onClick={closeDialog}>
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
                    onClick={() => handleRemoveClause(clause.id)}
                    aria-label={`Remove filter on ${clause.column}`}
                  >
                    Remove
                  </button>
                </div>
              ))
            )}
          </div>
        </dialog>
      </>
    );
  },
);
