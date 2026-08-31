import "react-data-grid/lib/styles.css";
import {
  Cell,
  DataGrid,
  Row,
  type Column,
  type DataGridHandle,
  type RenderEditCellProps,
  type Renderers,
  type RowsChangeData,
  type SortColumn,
} from "react-data-grid";
import {
  PersistedWriteRejectedError,
  type ColumnDescriptor,
  type ColumnType,
  type TableProxy,
  type Value,
  type WriteResult,
} from "jazz-tools";
import { useAll, useDb } from "jazz-tools/react";
import type { DynamicTableRow } from "../../utility/generic-query-builder.js";
import {
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type Dispatch,
  type KeyboardEvent,
  type RefObject,
  type SetStateAction,
} from "react";
import { Link, Navigate, useOutletContext, useParams, useSearchParams } from "react-router";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import { GenericQueryBuilder } from "../../utility/generic-query-builder.js";
import { useLocalStorageState } from "../../utility/use-local-storage-state.js";
import { Tooltip } from "../tooltip/Tooltip.js";
import {
  ColumnCustomizationModal,
  isColumnPreferences,
  reconcileColumnPreferences,
  type ColumnPreference,
} from "./ColumnCustomizationModal.js";
import { TableFilterBuilder, type TableFilterClause } from "./TableFilterBuilder.js";
import {
  formatMutationFieldValue,
  getFieldReadOnlyReason,
  parseMutationFieldValue,
} from "./row-mutation-form.js";
import { buildRelationFilterHref } from "./relation-navigation.js";
import styles from "./TableDataGrid.module.css";

const NULL_CELL_MARKER = "<null>";

function formatCellValue(value: unknown): string {
  if (value === null) return NULL_CELL_MARKER;
  if (value === undefined) return "";
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

const RELATION_LABEL_COLUMN_PRIORITY = [
  "name",
  "title",
  "label",
  "displayName",
  "display_name",
  "username",
  "handle",
  "slug",
  "email",
] as const;

const PAGE_SIZE_OPTIONS = [10, 25, 50] as const;
const DEFAULT_PAGE_SIZE = 25;
const EMPTY_ROWS: DynamicTableRow[] = [];
const CELL_UPDATE_ANIMATION_MS = 1_200;
const ROW_ADDED_ANIMATION_MS = 2_000;
const ROW_REMOVED_ANIMATION_MS = 650;
const ACTIONS_COLUMN_KEY = "__actions__";
const COLUMN_PREFERENCES_STORAGE_KEY_PREFIX = "jazz.inspector.dataExplorer.columnPreferences";

interface GridColumn {
  id: string;
  accessorKey: string;
  header: string;
  enableSorting: boolean;
  hiddenByDefault?: true;
}

const PROVENANCE_GRID_COLUMNS: readonly GridColumn[] = [
  {
    id: "$createdAt",
    accessorKey: "$createdAt",
    header: "$createdAt",
    enableSorting: true,
  },
  {
    id: "$createdBy",
    accessorKey: "$createdBy",
    header: "$createdBy",
    enableSorting: true,
    hiddenByDefault: true,
  },
  {
    id: "$updatedAt",
    accessorKey: "$updatedAt",
    header: "$updatedAt",
    enableSorting: true,
  },
  {
    id: "$updatedBy",
    accessorKey: "$updatedBy",
    header: "$updatedBy",
    enableSorting: true,
    hiddenByDefault: true,
  },
];

type RowChangeState = "added" | "removed";

interface AnimatedGridRow {
  row: DynamicTableRow;
  rowChangeState?: RowChangeState;
  changedCellIds?: Record<string, true>;
}

interface QueuedCellEdit {
  text: string;
  isNull: boolean;
}

type QueuedRowEdits = Record<string, QueuedCellEdit>;

interface StagedInsert {
  id: string;
  edits: QueuedRowEdits;
}

export interface TableMutationState {
  queuedEdits: Record<string, QueuedRowEdits>;
  stagedInserts: StagedInsert[];
  isQueuedSavePending: boolean;
  queuedSaveError: string | null;
  queuedDeletes: Set<string>;
  pendingScrollToRowId: string | null;
  nextEntryRevision: number;
  queuedEditRevisions: Record<string, Record<string, number>>;
  stagedInsertRevisions: Record<string, number>;
  queuedDeleteRevisions: Record<string, number>;
}

interface SubmittedTableSave {
  submittedQueuedEdits: Record<string, QueuedRowEdits>;
  submittedStagedInserts: StagedInsert[];
  submittedQueuedDeletes: Set<string>;
  submittedQueuedEditRevisions: Record<string, Record<string, number>>;
  submittedStagedInsertRevisions: Record<string, number>;
  submittedQueuedDeleteRevisions: Record<string, number>;
}

export interface PendingTableSave extends SubmittedTableSave {
  writeResult: WriteResult<void>;
}

function createTableMutationState(): TableMutationState {
  return {
    queuedEdits: {},
    stagedInserts: [],
    isQueuedSavePending: false,
    queuedSaveError: null,
    queuedDeletes: new Set(),
    pendingScrollToRowId: null,
    nextEntryRevision: 0,
    queuedEditRevisions: {},
    stagedInsertRevisions: {},
    queuedDeleteRevisions: {},
  };
}

function queuedCellEditsEqual(a: QueuedCellEdit | undefined, b: QueuedCellEdit | undefined) {
  return a?.text === b?.text && a?.isNull === b?.isNull;
}

function queuedRowEditsEqual(a: QueuedRowEdits, b: QueuedRowEdits) {
  const aColumns = Object.keys(a);
  return (
    aColumns.length === Object.keys(b).length &&
    aColumns.every((key) => queuedCellEditsEqual(a[key], b[key]))
  );
}

function settleSubmittedTableSave(
  previous: TableMutationState,
  settledSave: SubmittedTableSave,
): TableMutationState {
  const nextQueuedEdits: Record<string, QueuedRowEdits> = {};
  const nextQueuedEditRevisions: Record<string, Record<string, number>> = {};
  for (const [rowId, rowEdits] of Object.entries(previous.queuedEdits)) {
    const submittedRowEdits = settledSave.submittedQueuedEdits[rowId];
    const remainingRowEdits: QueuedRowEdits = {};
    const remainingRowRevisions: Record<string, number> = {};
    for (const [columnId, queuedEdit] of Object.entries(rowEdits)) {
      const currentRevision = previous.queuedEditRevisions[rowId]?.[columnId];
      const wasSettled =
        queuedCellEditsEqual(queuedEdit, submittedRowEdits?.[columnId]) &&
        currentRevision === settledSave.submittedQueuedEditRevisions[rowId]?.[columnId];
      if (!wasSettled) {
        remainingRowEdits[columnId] = queuedEdit;
        if (currentRevision !== undefined) {
          remainingRowRevisions[columnId] = currentRevision;
        }
      }
    }
    if (Object.keys(remainingRowEdits).length > 0) {
      nextQueuedEdits[rowId] = remainingRowEdits;
      nextQueuedEditRevisions[rowId] = remainingRowRevisions;
    }
  }

  const submittedInsertById = new Map(
    settledSave.submittedStagedInserts.map((insert) => [insert.id, insert]),
  );
  const nextStagedInserts = previous.stagedInserts.filter((insert) => {
    const submitted = submittedInsertById.get(insert.id);
    return (
      !submitted ||
      !queuedRowEditsEqual(insert.edits, submitted.edits) ||
      previous.stagedInsertRevisions[insert.id] !==
        settledSave.submittedStagedInsertRevisions[insert.id]
    );
  });
  const nextStagedInsertRevisions: Record<string, number> = {};
  for (const insert of nextStagedInserts) {
    const revision = previous.stagedInsertRevisions[insert.id];
    if (revision !== undefined) {
      nextStagedInsertRevisions[insert.id] = revision;
    }
  }

  const nextQueuedDeletes = new Set(previous.queuedDeletes);
  const nextQueuedDeleteRevisions = { ...previous.queuedDeleteRevisions };
  for (const rowId of settledSave.submittedQueuedDeletes) {
    if (
      previous.queuedDeleteRevisions[rowId] === settledSave.submittedQueuedDeleteRevisions[rowId]
    ) {
      nextQueuedDeletes.delete(rowId);
      delete nextQueuedDeleteRevisions[rowId];
    }
  }

  return {
    ...previous,
    queuedEdits: nextQueuedEdits,
    queuedEditRevisions: nextQueuedEditRevisions,
    stagedInserts: nextStagedInserts,
    stagedInsertRevisions: nextStagedInsertRevisions,
    queuedDeletes: nextQueuedDeletes,
    queuedDeleteRevisions: nextQueuedDeleteRevisions,
  };
}

interface EditableGridRow extends AnimatedGridRow {
  row: DynamicTableRow;
  sourceRow: DynamicTableRow;
  queuedEdits?: QueuedRowEdits;
  isStagedInsert?: boolean;
  stagedInsertId?: string;
}

function isColumnSortable(columnType: ColumnType): boolean {
  switch (columnType.type) {
    case "Integer":
    case "BigInt":
    case "Double":
    case "Boolean":
    case "Text":
    case "Enum":
    case "Timestamp":
    case "Uuid":
      return true;
    default:
      return false;
  }
}

function getGridRowId(row: DynamicTableRow): string {
  return String(row.id);
}

function clearAnimationTimeouts(timeouts: Map<string, ReturnType<typeof setTimeout>>): void {
  for (const timeout of timeouts.values()) {
    clearTimeout(timeout);
  }
  timeouts.clear();
}

function mergeChangedCellIds(
  existingCellIds: Record<string, true> | undefined,
  nextCellIds: string[],
): Record<string, true> | undefined {
  if (!existingCellIds && nextCellIds.length === 0) {
    return undefined;
  }

  const mergedCellIds = { ...existingCellIds };
  for (const cellId of nextCellIds) {
    mergedCellIds[cellId] = true;
  }

  return Object.keys(mergedCellIds).length > 0 ? mergedCellIds : undefined;
}

function getChangedCellIds(
  previousRow: DynamicTableRow,
  nextRow: DynamicTableRow,
  gridColumns: GridColumn[],
): string[] {
  const changedCellIds: string[] = [];

  for (const column of gridColumns) {
    const previousValue = formatCellValue(previousRow[column.accessorKey]);
    const nextValue = formatCellValue(nextRow[column.accessorKey]);
    if (previousValue !== nextValue) {
      changedCellIds.push(column.id);
    }
  }

  return changedCellIds;
}

function createAnimatedGridRow(row: DynamicTableRow): AnimatedGridRow {
  return { row };
}

function isDisplayFriendlyColumn(column: ColumnDescriptor): boolean {
  if (column.name === "id" || column.references) {
    return false;
  }

  switch (column.column_type.type) {
    case "Text":
    case "Enum":
    case "Timestamp":
    case "Integer":
    case "BigInt":
    case "Double":
    case "Boolean":
      return true;
    default:
      return false;
  }
}

function getRelationDisplayColumn(
  schema: Record<string, { columns: ColumnDescriptor[] }>,
  table: string,
) {
  const tableColumns = schema[table]?.columns ?? [];

  for (const columnName of RELATION_LABEL_COLUMN_PRIORITY) {
    const matchingColumn = tableColumns.find(
      (column) => column.name === columnName && isDisplayFriendlyColumn(column),
    );
    if (matchingColumn) {
      return matchingColumn;
    }
  }

  const firstTextColumn = tableColumns.find(
    (column) => column.column_type.type === "Text" && isDisplayFriendlyColumn(column),
  );
  if (firstTextColumn) {
    return firstTextColumn;
  }

  return tableColumns.find(isDisplayFriendlyColumn);
}

function createQueuedCellEdit(column: ColumnDescriptor, value: unknown): QueuedCellEdit {
  return {
    text: formatMutationFieldValue(value),
    isNull: column.nullable && (value === null || value === undefined),
  };
}

function areQueuedCellEditsEqual(left: QueuedCellEdit, right: QueuedCellEdit): boolean {
  return left.text === right.text && left.isNull === right.isNull;
}

function parseQueuedEditValue(column: ColumnDescriptor, edit: QueuedCellEdit): unknown {
  if (edit.isNull) {
    if (!column.nullable) {
      throw new Error("This column is not nullable.");
    }
    return null;
  }

  return parseMutationFieldValue(column.column_type, edit.text);
}

function parseQueuedEditForColumn(column: ColumnDescriptor, edit: QueuedCellEdit): unknown {
  try {
    return parseQueuedEditValue(column, edit);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Invalid value.";
    throw new Error(`${column.name}: ${message}`);
  }
}

function hasColumnDefault(column: ColumnDescriptor): boolean {
  return Object.prototype.hasOwnProperty.call(column, "default");
}

function unwrapDefaultValue(defaultValue: Value, columnType: ColumnType): unknown {
  switch (defaultValue.type) {
    case "Null":
      return null;
    case "Integer":
    case "BigInt":
    case "Double":
    case "Boolean":
    case "Text":
    case "Timestamp":
    case "Uuid":
      return columnType.type === "Json"
        ? JSON.parse(String(defaultValue.value))
        : defaultValue.value;
    case "Bytea":
      return new Uint8Array(defaultValue.value);
    case "Array": {
      if (columnType.type !== "Array") {
        throw new Error("Array default does not match column type.");
      }

      return defaultValue.value.map((innerValue) =>
        unwrapDefaultValue(innerValue, columnType.element),
      );
    }
    case "Row":
      // Row-valued defaults are not valid for schema columns.
      return "";
  }
}

function getInitialStagedInsertCellValue(column: ColumnDescriptor): unknown {
  if (column.default !== undefined) {
    return unwrapDefaultValue(column.default, column.column_type);
  }

  if (column.nullable) {
    return null;
  }

  return undefined;
}

function createInitialStagedInsertEdits(schemaColumns: ColumnDescriptor[]): QueuedRowEdits {
  const edits: QueuedRowEdits = {};

  for (const column of schemaColumns) {
    if (hasColumnDefault(column) || getFieldReadOnlyReason(column) !== null) {
      continue;
    }

    if (column.nullable) {
      edits[column.name] = {
        text: "",
        isNull: true,
      };
    } else if (column.column_type.type === "Boolean") {
      edits[column.name] = {
        text: "false",
        isNull: false,
      };
    }
  }

  return edits;
}

function createStagedInsert(schemaColumns: ColumnDescriptor[]): StagedInsert {
  return {
    id: crypto.randomUUID(),
    edits: createInitialStagedInsertEdits(schemaColumns),
  };
}

function buildQueuedInsertValues(
  schemaColumns: ColumnDescriptor[],
  queuedInsertEdits: QueuedRowEdits,
): Record<string, unknown> {
  const values: Record<string, unknown> = {};

  for (const column of schemaColumns) {
    if (getFieldReadOnlyReason(column) !== null) {
      continue;
    }

    const edit = queuedInsertEdits[column.name];
    if (!edit) {
      continue;
    }

    values[column.name] = parseQueuedEditForColumn(column, edit);
  }

  return values;
}

function applyQueuedEditsToRow(
  row: DynamicTableRow,
  queuedEdits?: QueuedRowEdits,
): DynamicTableRow {
  if (!queuedEdits || Object.keys(queuedEdits).length === 0) {
    return row;
  }

  const nextRow = { ...row };
  for (const [columnId, queuedEdit] of Object.entries(queuedEdits)) {
    nextRow[columnId] = queuedEdit.isNull ? null : queuedEdit.text;
  }

  return nextRow;
}

function removeQueuedCellEdit(
  queuedEdits: Record<string, QueuedRowEdits>,
  rowId: string,
  columnId: string,
): Record<string, QueuedRowEdits> {
  const rowEdits = queuedEdits[rowId];
  if (!rowEdits?.[columnId]) {
    return queuedEdits;
  }

  const nextRowEdits = { ...rowEdits };
  delete nextRowEdits[columnId];

  if (Object.keys(nextRowEdits).length === 0) {
    const nextQueuedEdits = { ...queuedEdits };
    delete nextQueuedEdits[rowId];
    return nextQueuedEdits;
  }

  return {
    ...queuedEdits,
    [rowId]: nextRowEdits,
  };
}

function useAnimatedGridRows(
  rows: DynamicTableRow[],
  gridColumns: GridColumn[],
  animationScopeKey: string,
): AnimatedGridRow[] {
  const [renderedRows, setRenderedRows] = useState<AnimatedGridRow[]>(() =>
    rows.map(createAnimatedGridRow),
  );
  const previousRowsRef = useRef(rows);
  const previousScopeKeyRef = useRef(animationScopeKey);
  const hasEstablishedScopeBaselineRef = useRef(rows.length > 0);
  const cellAnimationTimeoutsRef = useRef(new Map<string, ReturnType<typeof setTimeout>>());
  const rowAnimationTimeoutsRef = useRef(new Map<string, ReturnType<typeof setTimeout>>());

  useEffect(() => {
    return () => {
      clearAnimationTimeouts(cellAnimationTimeoutsRef.current);
      clearAnimationTimeouts(rowAnimationTimeoutsRef.current);
    };
  }, []);

  useEffect(() => {
    if (previousScopeKeyRef.current !== animationScopeKey) {
      previousScopeKeyRef.current = animationScopeKey;
      previousRowsRef.current = rows;
      hasEstablishedScopeBaselineRef.current = rows.length > 0;
      clearAnimationTimeouts(cellAnimationTimeoutsRef.current);
      clearAnimationTimeouts(rowAnimationTimeoutsRef.current);
      setRenderedRows(rows.map(createAnimatedGridRow));
      return;
    }

    const previousRows = previousRowsRef.current;
    previousRowsRef.current = rows;

    if (!hasEstablishedScopeBaselineRef.current) {
      if (rows.length === 0) {
        setRenderedRows([]);
        return;
      }

      hasEstablishedScopeBaselineRef.current = true;
      setRenderedRows(rows.map(createAnimatedGridRow));
      return;
    }

    const previousRowById = new Map(previousRows.map((row) => [getGridRowId(row), row]));
    const nextRowById = new Map(rows.map((row) => [getGridRowId(row), row]));
    const addedRowIds = new Set(
      rows.map(getGridRowId).filter((rowId) => !previousRowById.has(rowId)),
    );
    const removedRows = previousRows.filter((row) => !nextRowById.has(getGridRowId(row)));
    const changedCellIdsByRowId = new Map<string, string[]>();

    for (const row of rows) {
      const rowId = getGridRowId(row);
      const previousRow = previousRowById.get(rowId);
      if (!previousRow) {
        continue;
      }
      const changedCellIds = getChangedCellIds(previousRow, row, gridColumns);
      if (changedCellIds.length > 0) {
        changedCellIdsByRowId.set(rowId, changedCellIds);
      }
    }

    setRenderedRows((currentRenderedRows) => {
      const currentRenderedRowById = new Map(
        currentRenderedRows.map((entry) => [getGridRowId(entry.row), entry]),
      );
      const nextRenderedRows: AnimatedGridRow[] = rows.map((row) => {
        const rowId = getGridRowId(row);
        const currentRenderedRow = currentRenderedRowById.get(rowId);
        const rowChangeState: RowChangeState | undefined =
          addedRowIds.has(rowId) || currentRenderedRow?.rowChangeState === "added"
            ? "added"
            : undefined;

        return {
          row,
          rowChangeState,
          changedCellIds: mergeChangedCellIds(
            currentRenderedRow?.changedCellIds,
            changedCellIdsByRowId.get(rowId) ?? [],
          ),
        };
      });

      const ghostInsertions = new Map<string, { entry: AnimatedGridRow; index: number }>();

      for (const [index, entry] of currentRenderedRows.entries()) {
        const rowId = getGridRowId(entry.row);
        if (entry.rowChangeState === "removed" && !nextRowById.has(rowId)) {
          ghostInsertions.set(rowId, { entry, index });
        }
      }

      for (const removedRow of removedRows) {
        const rowId = getGridRowId(removedRow);
        ghostInsertions.set(rowId, {
          entry: { row: removedRow, rowChangeState: "removed" },
          index: previousRows.findIndex((row) => getGridRowId(row) === rowId),
        });
      }

      for (const { entry, index } of [...ghostInsertions.values()].sort((left, right) => {
        return left.index - right.index;
      })) {
        const rowId = getGridRowId(entry.row);
        if (nextRenderedRows.some((renderedRow) => getGridRowId(renderedRow.row) === rowId)) {
          continue;
        }

        nextRenderedRows.splice(Math.max(0, Math.min(index, nextRenderedRows.length)), 0, entry);
      }

      return nextRenderedRows;
    });

    for (const rowId of addedRowIds) {
      const existingTimeout = rowAnimationTimeoutsRef.current.get(rowId);
      if (existingTimeout) {
        clearTimeout(existingTimeout);
      }
      rowAnimationTimeoutsRef.current.set(
        rowId,
        setTimeout(() => {
          rowAnimationTimeoutsRef.current.delete(rowId);
          setRenderedRows((currentRenderedRows) =>
            currentRenderedRows.map((entry) =>
              getGridRowId(entry.row) === rowId && entry.rowChangeState === "added"
                ? { ...entry, rowChangeState: undefined }
                : entry,
            ),
          );
        }, ROW_ADDED_ANIMATION_MS),
      );
    }

    for (const removedRow of removedRows) {
      const rowId = getGridRowId(removedRow);
      const existingTimeout = rowAnimationTimeoutsRef.current.get(rowId);
      if (existingTimeout) {
        clearTimeout(existingTimeout);
      }
      rowAnimationTimeoutsRef.current.set(
        rowId,
        setTimeout(() => {
          rowAnimationTimeoutsRef.current.delete(rowId);
          setRenderedRows((currentRenderedRows) =>
            currentRenderedRows.filter((entry) => getGridRowId(entry.row) !== rowId),
          );
        }, ROW_REMOVED_ANIMATION_MS),
      );
    }

    for (const [rowId, changedCellIds] of changedCellIdsByRowId) {
      for (const cellId of changedCellIds) {
        const timeoutKey = `${rowId}:${cellId}`;
        const existingTimeout = cellAnimationTimeoutsRef.current.get(timeoutKey);
        if (existingTimeout) {
          clearTimeout(existingTimeout);
        }
        cellAnimationTimeoutsRef.current.set(
          timeoutKey,
          setTimeout(() => {
            cellAnimationTimeoutsRef.current.delete(timeoutKey);
            setRenderedRows((currentRenderedRows) =>
              currentRenderedRows.map((entry) => {
                if (getGridRowId(entry.row) !== rowId || !entry.changedCellIds?.[cellId]) {
                  return entry;
                }

                const nextChangedCellIds = { ...entry.changedCellIds };
                delete nextChangedCellIds[cellId];

                return {
                  ...entry,
                  changedCellIds:
                    Object.keys(nextChangedCellIds).length > 0 ? nextChangedCellIds : undefined,
                };
              }),
            );
          }, CELL_UPDATE_ANIMATION_MS),
        );
      }
    }
  }, [animationScopeKey, gridColumns, rows]);

  return renderedRows;
}

export function TableDataGrid() {
  const { table } = useParams();

  if (!table) {
    return <Navigate to="/data-explorer" replace />;
  }

  const { wasmSchema: schema, runtime } = useDevtoolsContext();
  const db = useDb();
  const [searchParams, setSearchParams] = useSearchParams();

  const sorting = useMemo<readonly SortColumn[]>(() => {
    const col = searchParams.get("sort");
    const dir = searchParams.get("dir");
    if (col) {
      return [{ columnKey: col, direction: dir === "DESC" ? "DESC" : "ASC" }];
    }
    return [{ columnKey: "id", direction: "ASC" }];
  }, [searchParams]);

  const pageSize = useMemo(() => {
    const raw = searchParams.get("pageSize");
    if (raw) {
      const n = Number(raw);
      if (PAGE_SIZE_OPTIONS.includes(n as (typeof PAGE_SIZE_OPTIONS)[number])) return n;
    }
    return DEFAULT_PAGE_SIZE;
  }, [searchParams]);

  const pageIndex = useMemo(() => {
    const raw = searchParams.get("page");
    if (raw) {
      const n = Number(raw);
      if (Number.isInteger(n) && n >= 0) return n;
    }
    return 0;
  }, [searchParams]);

  const filters = useMemo<TableFilterClause[]>(() => {
    const raw = searchParams.get("filters");
    if (raw) {
      try {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) return parsed;
      } catch {
        // ignore malformed
      }
    }
    return [];
  }, [searchParams]);

  const setPageSize = (next: number) => {
    setSearchParams(
      (prev) => {
        const p = new URLSearchParams(prev);
        if (next !== DEFAULT_PAGE_SIZE) {
          p.set("pageSize", String(next));
        } else {
          p.delete("pageSize");
        }
        p.delete("page");
        return p;
      },
      { replace: true },
    );
  };

  const setPageIndex = (next: number | ((current: number) => number)) => {
    setSearchParams(
      (prev) => {
        const p = new URLSearchParams(prev);
        const currentPage = Number(p.get("page") ?? 0);
        const resolved = typeof next === "function" ? next(currentPage) : next;
        if (resolved > 0) {
          p.set("page", String(resolved));
        } else {
          p.delete("page");
        }
        return p;
      },
      { replace: true },
    );
  };

  const setFilters = (next: TableFilterClause[]) => {
    setSearchParams(
      (prev) => {
        const p = new URLSearchParams(prev);
        if (next.length > 0) {
          p.set("filters", JSON.stringify(next));
        } else {
          p.delete("filters");
        }
        p.delete("page");
        return p;
      },
      { replace: true },
    );
  };
  const explorerContext = useOutletContext<{
    mutationStateByTable: Record<string, TableMutationState>;
    setMutationStateByTable: Dispatch<SetStateAction<Record<string, TableMutationState>>>;
    pendingSaveByTable: ReadonlyMap<string, PendingTableSave>;
    setPendingSaveByTable: Dispatch<SetStateAction<Map<string, PendingTableSave>>>;
    beginSave: (table: string) => symbol | null;
    finishSave: (table: string, token: symbol) => boolean;
  } | null>();
  const [fallbackMutationStateByTable, setFallbackMutationStateByTable] = useState<
    Record<string, TableMutationState>
  >({});
  const fallbackSaveTokens = useRef(new Map<string, symbol>());
  const [fallbackPendingSaveByTable, setFallbackPendingSaveByTable] = useState(
    () => new Map<string, PendingTableSave>(),
  );
  const mutationStateByTable =
    explorerContext?.mutationStateByTable ?? fallbackMutationStateByTable;
  const setMutationStateByTable =
    explorerContext?.setMutationStateByTable ?? setFallbackMutationStateByTable;
  const pendingSaveByTable = explorerContext?.pendingSaveByTable ?? fallbackPendingSaveByTable;
  const setPendingSaveByTable =
    explorerContext?.setPendingSaveByTable ?? setFallbackPendingSaveByTable;
  const beginSave =
    explorerContext?.beginSave ??
    ((targetTable: string) => {
      if (fallbackSaveTokens.current.has(targetTable)) return null;
      const token = Symbol(targetTable);
      fallbackSaveTokens.current.set(targetTable, token);
      return token;
    });
  const finishSave =
    explorerContext?.finishSave ??
    ((targetTable: string, token: symbol) => {
      if (fallbackSaveTokens.current.get(targetTable) !== token) return false;
      fallbackSaveTokens.current.delete(targetTable);
      return true;
    });
  const [selectedRowIds, setSelectedRowIds] = useState<Set<string>>(new Set());
  const mutationState = mutationStateByTable[table] ?? createTableMutationState();
  const updateCurrentTableMutationState = (update: SetStateAction<TableMutationState>) => {
    setMutationStateByTable((current) => {
      const previous = current[table] ?? createTableMutationState();
      const next = typeof update === "function" ? update(previous) : update;
      return { ...current, [table]: next };
    });
  };
  const queuedEdits = mutationState.queuedEdits;
  const stagedInserts = mutationState.stagedInserts;
  const isQueuedSavePending = mutationState.isQueuedSavePending;
  const queuedSaveError = mutationState.queuedSaveError;
  const queuedDeletes = mutationState.queuedDeletes;
  const pendingScrollToRowId = mutationState.pendingScrollToRowId;
  const setQueuedEdits: Dispatch<SetStateAction<Record<string, QueuedRowEdits>>> = (update) =>
    updateCurrentTableMutationState((current) => {
      const nextEdits = typeof update === "function" ? update(current.queuedEdits) : update;
      let nextRevision = current.nextEntryRevision;
      const nextRevisions: Record<string, Record<string, number>> = {};
      for (const [rowId, rowEdits] of Object.entries(nextEdits)) {
        const rowRevisions: Record<string, number> = {};
        for (const [columnId, value] of Object.entries(rowEdits)) {
          const unchanged = queuedCellEditsEqual(value, current.queuedEdits[rowId]?.[columnId]);
          rowRevisions[columnId] = unchanged
            ? (current.queuedEditRevisions[rowId]?.[columnId] ?? ++nextRevision)
            : ++nextRevision;
        }
        if (Object.keys(rowRevisions).length > 0) nextRevisions[rowId] = rowRevisions;
      }
      return {
        ...current,
        queuedEdits: nextEdits,
        queuedEditRevisions: nextRevisions,
        nextEntryRevision: nextRevision,
      };
    });
  const setStagedInserts: Dispatch<SetStateAction<StagedInsert[]>> = (update) =>
    updateCurrentTableMutationState((current) => {
      const nextInserts = typeof update === "function" ? update(current.stagedInserts) : update;
      const currentById = new Map(current.stagedInserts.map((insert) => [insert.id, insert]));
      let nextRevision = current.nextEntryRevision;
      const nextRevisions: Record<string, number> = {};
      for (const insert of nextInserts) {
        nextRevisions[insert.id] = queuedRowEditsEqual(
          insert.edits,
          currentById.get(insert.id)?.edits ?? {},
        )
          ? (current.stagedInsertRevisions[insert.id] ?? ++nextRevision)
          : ++nextRevision;
      }
      return {
        ...current,
        stagedInserts: nextInserts,
        stagedInsertRevisions: nextRevisions,
        nextEntryRevision: nextRevision,
      };
    });
  const setQueuedSaveError: Dispatch<SetStateAction<string | null>> = (update) =>
    updateCurrentTableMutationState((current) => ({
      ...current,
      queuedSaveError: typeof update === "function" ? update(current.queuedSaveError) : update,
    }));
  const setQueuedDeletes: Dispatch<SetStateAction<Set<string>>> = (update) =>
    updateCurrentTableMutationState((current) => {
      const nextDeletes = typeof update === "function" ? update(current.queuedDeletes) : update;
      let nextRevision = current.nextEntryRevision;
      const nextRevisions: Record<string, number> = {};
      for (const rowId of nextDeletes) {
        nextRevisions[rowId] = current.queuedDeletes.has(rowId)
          ? (current.queuedDeleteRevisions[rowId] ?? ++nextRevision)
          : ++nextRevision;
      }
      return {
        ...current,
        queuedDeletes: nextDeletes,
        queuedDeleteRevisions: nextRevisions,
        nextEntryRevision: nextRevision,
      };
    });
  const setPendingScrollToRowId: Dispatch<SetStateAction<string | null>> = (update) =>
    updateCurrentTableMutationState((current) => ({
      ...current,
      pendingScrollToRowId:
        typeof update === "function" ? update(current.pendingScrollToRowId) : update,
    }));
  const [isColumnCustomizationOpen, setIsColumnCustomizationOpen] = useState(false);
  const columnPreferencesStorageKey = `${COLUMN_PREFERENCES_STORAGE_KEY_PREFIX}.${table}`;
  const [storedColumnPreferences, setStoredColumnPreferences] = useLocalStorageState<
    ColumnPreference[]
  >(columnPreferencesStorageKey, [], { isValid: isColumnPreferences });
  const schemaColumns = schema[table]?.columns ?? [];
  const schemaColumnById = useMemo(
    () => new Map(schemaColumns.map((column) => [column.name, column])),
    [schemaColumns],
  );
  const activeSort = sorting[0] ?? { columnKey: "id", direction: "ASC" };
  const sortColumn = activeSort.columnKey;
  const sortDirection = activeSort.direction === "DESC" ? "desc" : "asc";
  const queryOffset = pageIndex * pageSize;
  const queryLimit = pageSize + 1;
  const queryBuilder = useMemo(() => {
    let builder = new GenericQueryBuilder(table, schema).select(
      "*",
      ...PROVENANCE_GRID_COLUMNS.map((column) => column.id),
    );
    for (const filter of filters) {
      if (filter.operator === "eq") {
        builder = builder.where({ [filter.column]: filter.value });
      } else {
        builder = builder.where({
          [filter.column]: {
            [filter.operator]: filter.value,
          },
        });
      }
    }
    return builder.orderBy(sortColumn, sortDirection).limit(queryLimit).offset(queryOffset);
  }, [table, schema, filters, sortColumn, sortDirection, queryLimit, queryOffset]);
  const builtQuery = useMemo(() => queryBuilder._build(), [queryBuilder]);
  // Standalone always has a server; the overlay may be fully offline (its
  // whole point is to show the host's local — possibly unsynced — data), so it
  // must not wait for an edge ack or force a server round-trip on reads.
  const mutationDurabilityTier = runtime === "standalone" ? "edge" : "local";
  const queryOptions = useMemo(
    () =>
      ({
        propagation: runtime === "standalone" ? "full" : "local-only",
        visibility: "hidden_from_live_query_list",
      }) as const,
    [runtime],
  );
  const queryResult = useAll<DynamicTableRow>(queryBuilder, queryOptions);
  // show a grid skeleton while the first result is in flight.
  const isInitialLoading = queryResult.isLoading;
  const queryError = queryResult.error;
  const rows = queryResult.data ?? EMPTY_ROWS;

  const allGridColumns = useMemo<GridColumn[]>(
    () => [
      {
        id: "id",
        accessorKey: "id",
        header: "ID",
        enableSorting: true,
      },
      ...schemaColumns.map(
        (column): GridColumn => ({
          id: column.name,
          accessorKey: column.name,
          header: column.name,
          enableSorting: isColumnSortable(column.column_type),
        }),
      ),
      ...PROVENANCE_GRID_COLUMNS,
    ],
    [schemaColumns],
  );
  const columnPreferences = useMemo(
    () => reconcileColumnPreferences(allGridColumns, storedColumnPreferences),
    [allGridColumns, storedColumnPreferences],
  );
  const gridColumns = useMemo(() => {
    const gridColumnById = new Map(allGridColumns.map((column) => [column.id, column]));
    return columnPreferences.flatMap((preference) => {
      const column = gridColumnById.get(preference.id);
      return preference.visible && column ? [column] : [];
    });
  }, [allGridColumns, columnPreferences]);
  const hasNextPage = rows.length > pageSize;
  const visibleRows = hasNextPage ? rows.slice(0, pageSize) : rows;
  const queuedEditCount = useMemo(() => {
    return Object.values(queuedEdits).reduce(
      (total, rowEdits) => total + Object.keys(rowEdits).length,
      0,
    );
  }, [queuedEdits]);
  const queuedEditedRowCount = useMemo(() => Object.keys(queuedEdits).length, [queuedEdits]);
  const hasQueuedEdits = queuedEditCount > 0;
  const stagedInsertCount = stagedInserts.length;
  const hasStagedInserts = stagedInsertCount > 0;
  const hasQueuedChanges = hasQueuedEdits || queuedDeletes.size > 0 || hasStagedInserts;
  const hasUnresolvedSave = pendingSaveByTable.has(table);
  const isAnyMutationPending = isQueuedSavePending;
  const gridAnimationScopeKey = useMemo(
    () => `${table}:${builtQuery}:${gridColumns.map((column) => column.id).join("|")}`,
    [builtQuery, gridColumns, table],
  );
  const selectableRowIds = useMemo(() => {
    const rowIds = new Set<string>();
    for (const row of visibleRows) {
      rowIds.add(getGridRowId(row));
    }
    for (const stagedInsert of stagedInserts) {
      rowIds.add(stagedInsert.id);
    }
    return rowIds;
  }, [stagedInserts, visibleRows]);
  const selectedVisibleRowIds = useMemo(() => {
    const rowIds = new Set<string>();
    for (const rowId of selectedRowIds) {
      if (selectableRowIds.has(rowId)) {
        rowIds.add(rowId);
      }
    }
    return rowIds;
  }, [selectableRowIds, selectedRowIds]);
  const tableProxy = useMemo(
    () =>
      ({
        _table: table,
        _schema: schema,
        _rowType: undefined,
        _initType: undefined,
      }) as unknown as TableProxy<DynamicTableRow, Record<string, unknown>>,
    [table, schema],
  );
  const startRow = pageIndex * pageSize;
  const endRow = startRow + visibleRows.length;

  useEffect(() => {
    setSelectedRowIds(new Set());
  }, [gridAnimationScopeKey]);

  const handleSortColumnsChange = (nextSortColumns: SortColumn[]): void => {
    const nextSort =
      nextSortColumns.length === 0
        ? [{ columnKey: "id", direction: "ASC" as const }]
        : [nextSortColumns.at(-1)!];

    setSearchParams(
      (prev) => {
        const p = new URLSearchParams(prev);
        const col = nextSort[0];
        if (col && (col.columnKey !== "id" || col.direction !== "ASC")) {
          p.set("sort", col.columnKey);
          p.set("dir", col.direction);
        } else {
          p.delete("sort");
          p.delete("dir");
        }
        p.delete("page");
        return p;
      },
      { replace: true },
    );
  };
  const handleDiscardQueuedEdits = (): void => {
    if (pendingSaveByTable.has(table)) return;
    setQueuedEdits({});
    setStagedInserts([]);
    setQueuedDeletes(new Set());
    setQueuedSaveError(null);
  };
  const handleQueueSelectedDeletes = (): void => {
    if (selectedVisibleRowIds.size === 0) {
      return;
    }

    setQueuedSaveError(null);

    const selectedStagedInsertIds = new Set(
      stagedInserts
        .filter((stagedInsert) => selectedVisibleRowIds.has(stagedInsert.id))
        .map((stagedInsert) => stagedInsert.id),
    );
    if (selectedStagedInsertIds.size > 0) {
      setStagedInserts((currentStagedInserts) =>
        currentStagedInserts.filter(
          (stagedInsert) => !selectedStagedInsertIds.has(stagedInsert.id),
        ),
      );
    }

    const selectedRealRowIds = visibleRows
      .map((row) => getGridRowId(row))
      .filter((rowId) => selectedVisibleRowIds.has(rowId));
    if (selectedRealRowIds.length > 0) {
      setQueuedDeletes((currentQueuedDeletes) => {
        const nextQueuedDeletes = new Set(currentQueuedDeletes);
        for (const rowId of selectedRealRowIds) {
          nextQueuedDeletes.add(rowId);
        }
        return nextQueuedDeletes;
      });
    }

    setSelectedRowIds(new Set());
  };
  const handleSaveQueuedEdits = async (): Promise<void> => {
    const mutationTable = table;
    let pendingSave = pendingSaveByTable.get(mutationTable);
    if (!hasQueuedChanges && !pendingSave) {
      return;
    }

    const saveToken = beginSave(mutationTable);
    if (!saveToken) return;
    try {
      setMutationStateByTable((current) => ({
        ...current,
        [mutationTable]: {
          ...(current[mutationTable] ?? createTableMutationState()),
          isQueuedSavePending: true,
          queuedSaveError: null,
        },
      }));

      if (!pendingSave) {
        const submittedQueuedEdits = queuedEdits;
        const submittedStagedInserts = stagedInserts;
        const submittedQueuedDeletes = queuedDeletes;
        const submittedQueuedEditRevisions = mutationState.queuedEditRevisions;
        const submittedStagedInsertRevisions = mutationState.stagedInsertRevisions;
        const submittedQueuedDeleteRevisions = mutationState.queuedDeleteRevisions;
        const rowUpdates = Object.entries(submittedQueuedEdits)
          .filter(([rowId]) => !submittedQueuedDeletes.has(rowId))
          .map(([rowId, rowEdits]) => {
            const updates: Record<string, unknown> = {};
            for (const [columnId, queuedEdit] of Object.entries(rowEdits)) {
              const schemaColumn = schemaColumnById.get(columnId);
              if (!schemaColumn || getFieldReadOnlyReason(schemaColumn) !== null) {
                continue;
              }
              updates[columnId] = parseQueuedEditForColumn(schemaColumn, queuedEdit);
            }
            return { rowId, updates };
          })
          .filter(({ updates }) => Object.keys(updates).length > 0);
        const inserts = submittedStagedInserts.map((stagedInsert) => ({
          id: stagedInsert.id,
          values: buildQueuedInsertValues(schemaColumns, stagedInsert.edits),
        }));

        const submittedSave: SubmittedTableSave = {
          submittedQueuedEdits,
          submittedStagedInserts,
          submittedQueuedDeletes,
          submittedQueuedEditRevisions,
          submittedStagedInsertRevisions,
          submittedQueuedDeleteRevisions,
        };
        if (rowUpdates.length === 0 && submittedQueuedDeletes.size === 0 && inserts.length === 0) {
          setMutationStateByTable((current) => {
            const previous = current[mutationTable] ?? createTableMutationState();
            return {
              ...current,
              [mutationTable]: settleSubmittedTableSave(previous, submittedSave),
            };
          });
          return;
        }

        const writeResult = await db.transaction((tx) => {
          for (const { rowId, updates } of rowUpdates) {
            tx.update(tableProxy, rowId, updates);
          }
          for (const rowId of submittedQueuedDeletes) {
            tx.delete(tableProxy, rowId);
          }
          for (const insert of inserts) {
            tx.insert(tableProxy, insert.values, { id: insert.id });
          }
        });
        pendingSave = { writeResult, ...submittedSave };
        const retainedSave = pendingSave;
        setPendingSaveByTable((current) => {
          const next = new Map(current);
          next.set(mutationTable, retainedSave);
          return next;
        });
      }

      await pendingSave.writeResult.wait({ tier: mutationDurabilityTier });
      const settledSave = pendingSave;
      setPendingSaveByTable((current) => {
        if (current.get(mutationTable) !== settledSave) return current;
        const next = new Map(current);
        next.delete(mutationTable);
        return next;
      });
      setMutationStateByTable((current) => {
        const previous = current[mutationTable] ?? createTableMutationState();
        return {
          ...current,
          [mutationTable]: settleSubmittedTableSave(previous, settledSave),
        };
      });
    } catch (error) {
      if (pendingSave && error instanceof PersistedWriteRejectedError) {
        const rejectedSave = pendingSave;
        setPendingSaveByTable((current) => {
          if (current.get(mutationTable) !== rejectedSave) return current;
          const next = new Map(current);
          next.delete(mutationTable);
          return next;
        });
      }
      setMutationStateByTable((current) => ({
        ...current,
        [mutationTable]: {
          ...(current[mutationTable] ?? createTableMutationState()),
          queuedSaveError:
            error instanceof Error || (typeof Error.isError === "function" && Error.isError(error))
              ? error.message
              : "Could not persist queued cell edits.",
        },
      }));
    } finally {
      setMutationStateByTable((current) => ({
        ...current,
        [mutationTable]: {
          ...(current[mutationTable] ?? createTableMutationState()),
          isQueuedSavePending: false,
        },
      }));
      finishSave(mutationTable, saveToken);
    }
  };

  return (
    <section className={styles.container}>
      <TableFilterBuilder
        schemaColumns={schemaColumns}
        clauses={filters}
        onClausesChange={(nextFilters) => {
          setFilters(nextFilters);
        }}
        actions={
          <>
            <Tooltip label="Schema">
              <Link
                to={`/data-explorer/${table}/schema`}
                className={`${styles.secondaryButton} ${styles.iconButton}`}
                aria-label="Schema"
              >
                <CatalogIcon className={styles.buttonIcon} />
              </Link>
            </Tooltip>
            <Tooltip label="Customize columns">
              <button
                type="button"
                className={`${styles.secondaryButton} ${styles.iconButton}`}
                aria-label="Customize columns"
                onClick={() => setIsColumnCustomizationOpen(true)}
              >
                <ColumnsIcon className={styles.buttonIcon} />
              </button>
            </Tooltip>
            <Tooltip label="Insert row">
              <button
                type="button"
                className={`${styles.secondaryButton} ${styles.iconButton}`}
                aria-label="Insert row"
                onClick={() => {
                  setQueuedSaveError(null);
                  const stagedInsert = createStagedInsert(schemaColumns);
                  setStagedInserts((current) => [...current, stagedInsert]);
                  setPendingScrollToRowId(stagedInsert.id);
                }}
                disabled={isAnyMutationPending}
              >
                <PlusIcon className={styles.buttonIcon} />
              </button>
            </Tooltip>
            <Tooltip
              label={
                selectedVisibleRowIds.size === 0 ? "Select rows to delete" : "Delete selected rows"
              }
            >
              <button
                type="button"
                className={`${styles.secondaryButton} ${styles.iconButton}`}
                aria-label="Delete row(s)"
                onClick={handleQueueSelectedDeletes}
                disabled={selectedVisibleRowIds.size === 0}
              >
                <TrashIcon className={styles.buttonIcon} />
              </button>
            </Tooltip>
          </>
        }
      />
      <ColumnCustomizationModal
        open={isColumnCustomizationOpen}
        columns={allGridColumns}
        preferences={columnPreferences}
        onApply={setStoredColumnPreferences}
        onRequestClose={() => setIsColumnCustomizationOpen(false)}
      />
      <div className={styles.contentArea}>
        <div className={styles.gridFrame}>
          {queryError ? (
            <div className={styles.emptyState} role="alert">
              Could not load rows: {queryError.message}
            </div>
          ) : isInitialLoading ? (
            <GridSkeleton />
          ) : (
            <PlainTableView
              rows={visibleRows}
              gridColumns={gridColumns}
              sorting={sorting}
              schema={schema}
              queryOptions={queryOptions}
              schemaColumnById={schemaColumnById}
              queuedEdits={queuedEdits}
              stagedInserts={stagedInserts}
              selectedRowIds={selectedVisibleRowIds}
              queuedDeletes={queuedDeletes}
              pendingScrollToRowId={pendingScrollToRowId}
              animationScopeKey={gridAnimationScopeKey}
              onSortColumnsChange={handleSortColumnsChange}
              onQueuedEditsChange={setQueuedEdits}
              onStagedInsertsChange={setStagedInserts}
              onSelectedRowIdsChange={setSelectedRowIds}
              onQueuedSaveErrorChange={setQueuedSaveError}
              onQueuedDeletesChange={setQueuedDeletes}
              onPendingScrollToRowIdChange={setPendingScrollToRowId}
            />
          )}
        </div>
      </div>
      <div className={styles.bottomRail}>
        {hasQueuedChanges || queuedSaveError || hasUnresolvedSave ? (
          <div
            className={styles.queuedBanner}
            role={queuedSaveError ? "alert" : "status"}
            aria-live="polite"
          >
            <div className={styles.queuedBannerCopy}>
              <span
                className={styles.queuedBannerLabel}
                title={
                  hasUnresolvedSave
                    ? "This save may already be committed and cannot be discarded. Retry confirmation checks the retained result without submitting it again."
                    : "These changes are staged locally. Click Save changes to apply them, or Discard to drop them."
                }
              >
                {hasUnresolvedSave ? "Confirmation pending" : "Queued"}
              </span>
              {hasUnresolvedSave ? (
                <span>
                  This save may already be committed and cannot be discarded. Retry confirmation
                  checks the existing submission; edits made afterward stay queued for the next
                  save.
                </span>
              ) : null}
              {hasQueuedEdits ? (
                <span>
                  {queuedEditCount} edit{queuedEditCount === 1 ? "" : "s"} across{" "}
                  {queuedEditedRowCount} row{queuedEditedRowCount === 1 ? "" : "s"}
                </span>
              ) : null}
              {queuedDeletes.size > 0 ? (
                <span>
                  {queuedDeletes.size} row{queuedDeletes.size === 1 ? "" : "s"} will be deleted
                </span>
              ) : null}
              {hasStagedInserts ? (
                <span>
                  {stagedInsertCount} staged insert{stagedInsertCount === 1 ? "" : "s"}
                </span>
              ) : null}
              {queuedSaveError ? (
                <span className={styles.queuedBannerError}>{queuedSaveError}</span>
              ) : null}
            </div>
            <div className={styles.queuedBannerActions}>
              <button
                type="button"
                className={`${styles.secondaryButton} ${styles.queuedBannerButton}`}
                onClick={handleDiscardQueuedEdits}
                disabled={isQueuedSavePending || hasUnresolvedSave}
                title={
                  hasUnresolvedSave
                    ? "Discard is unavailable because the pending save may already be committed."
                    : undefined
                }
              >
                Discard
              </button>
              <button
                type="button"
                className={`${styles.primaryButton} ${styles.queuedBannerButton}`}
                onClick={() => {
                  void handleSaveQueuedEdits();
                }}
                disabled={isQueuedSavePending}
              >
                {isQueuedSavePending
                  ? hasUnresolvedSave
                    ? "Confirming..."
                    : "Saving..."
                  : hasUnresolvedSave
                    ? "Retry confirmation"
                    : "Save changes"}
              </button>
            </div>
          </div>
        ) : null}
        <footer className={styles.footer}>
          <div className={styles.paginationInfo}>
            {isInitialLoading
              ? "Loading…"
              : `Showing ${visibleRows.length === 0 ? 0 : startRow + 1}-${endRow}`}
          </div>
          <div className={styles.paginationControls}>
            <label className={styles.pageSizeLabel}>
              Rows per page
              <select
                className={styles.pageSizeSelect}
                value={pageSize}
                onChange={(event) => {
                  const nextPageSize = Number(event.target.value);
                  setPageSize(nextPageSize);
                }}
              >
                {PAGE_SIZE_OPTIONS.map((sizeOption) => (
                  <option key={sizeOption} value={sizeOption}>
                    {sizeOption}
                  </option>
                ))}
              </select>
            </label>
            <span className={styles.pageIndicator}>Page {pageIndex + 1}</span>
            <button
              type="button"
              className={styles.secondaryButton}
              onClick={() => {
                setPageIndex((current) => Math.max(0, current - 1));
              }}
              disabled={pageIndex === 0}
            >
              Previous
            </button>
            <button
              type="button"
              className={styles.secondaryButton}
              onClick={() => {
                setPageIndex((current) => current + 1);
              }}
              disabled={!hasNextPage}
            >
              Next
            </button>
          </div>
        </footer>
      </div>
    </section>
  );
}

/**
 * Opens an enum's value selector when the cell enters edit mode
 */
function useOpenSelectorOnEnumEdit(
  selectEditorRef: RefObject<HTMLSelectElement | null>,
  schemaColumn: ColumnDescriptor,
) {
  useLayoutEffect(() => {
    if (schemaColumn.column_type.type !== "Enum") {
      return;
    }

    const select = selectEditorRef.current;
    if (!select) {
      return;
    }

    select.focus({ preventScroll: true });
    try {
      (select as HTMLSelectElement & { showPicker?: () => void }).showPicker?.();
    } catch {
      // Browsers may reject showPicker() without transient user activation.
    }
  }, [schemaColumn.column_type.type]);
}

function QueuedCellEditor({
  row,
  onRowChange,
  onClose,
  schemaColumn,
}: RenderEditCellProps<EditableGridRow> & {
  schemaColumn: ColumnDescriptor;
}) {
  const [draft, setDraft] = useState<QueuedCellEdit>(() =>
    createQueuedCellEdit(schemaColumn, row.row[schemaColumn.name]),
  );
  const selectEditorRef = useRef<HTMLSelectElement | null>(null);

  useOpenSelectorOnEnumEdit(selectEditorRef, schemaColumn);

  const applyDraft = (nextDraft: QueuedCellEdit) => {
    setDraft(nextDraft);
    onRowChange(
      {
        ...row,
        row: {
          ...row.row,
          [schemaColumn.name]: nextDraft.isNull ? null : nextDraft.text,
        },
      },
      nextDraft.isNull,
    );
  };
  const updateText = (nextText: string) => {
    applyDraft({ text: nextText, isNull: false });
  };
  const commit = () => {
    onClose(true, false);
  };
  const setNullAndClose = () => {
    applyDraft({ text: "", isNull: true });
    onClose(true, false);
  };
  const handleEditorKeyDown = (
    event: KeyboardEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>,
  ) => {
    if (event.key === "Escape") {
      onClose(false, false);
    }
  };
  const nullAction = schemaColumn.nullable ? (
    <button
      type="button"
      className={styles.inlineNullButton}
      aria-label={`Set ${schemaColumn.name} to NULL`}
      title="Set to NULL"
      onMouseDown={(event) => {
        event.preventDefault();
      }}
      onClick={(event) => {
        event.stopPropagation();
        setNullAndClose();
      }}
    >
      <CrossIcon className={styles.inlineNullIcon} />
    </button>
  ) : null;

  if (schemaColumn.column_type.type === "Enum") {
    const selectValue = draft.isNull ? "" : draft.text;
    const predefinedValues = schemaColumn.column_type.variants;

    const selectEditor = (
      <select
        ref={selectEditorRef}
        aria-label={`Edit ${schemaColumn.name}`}
        className={styles.inlineEditorSelect}
        autoFocus
        value={selectValue}
        onChange={(event) => {
          applyDraft({ text: event.target.value, isNull: false });
        }}
        onBlur={schemaColumn.nullable ? undefined : commit}
        onKeyDown={handleEditorKeyDown}
      >
        {selectValue.length === 0 ? <option value="">Select value</option> : null}
        {!draft.isNull && draft.text.length > 0 && !predefinedValues.includes(draft.text) ? (
          <option value={draft.text}>{draft.text}</option>
        ) : null}
        {predefinedValues.map((predefinedValue) => (
          <option key={predefinedValue} value={predefinedValue}>
            {predefinedValue}
          </option>
        ))}
      </select>
    );

    if (!schemaColumn.nullable) {
      return selectEditor;
    }

    return (
      <div
        className={styles.inlineEditorStack}
        onBlur={(event) => {
          if (!event.currentTarget.contains(event.relatedTarget as Node | null)) {
            commit();
          }
        }}
      >
        {selectEditor}
        {nullAction}
      </div>
    );
  }

  if (
    schemaColumn.column_type.type === "Json" ||
    schemaColumn.column_type.type === "Array" ||
    schemaColumn.column_type.type === "Row"
  ) {
    return (
      <div
        className={styles.inlineEditorStack}
        onBlur={(event) => {
          if (!event.currentTarget.contains(event.relatedTarget as Node | null)) {
            commit();
          }
        }}
      >
        <textarea
          aria-label={`Edit ${schemaColumn.name}`}
          className={styles.inlineEditorTextarea}
          autoFocus
          value={draft.text}
          onChange={(event) => {
            updateText(event.target.value);
          }}
          onKeyDown={(event) => {
            if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
              commit();
            }
            handleEditorKeyDown(event);
          }}
        />
        {nullAction}
      </div>
    );
  }

  return (
    <div
      className={styles.inlineEditorStack}
      onBlur={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget as Node | null)) {
          commit();
        }
      }}
    >
      <input
        aria-label={`Edit ${schemaColumn.name}`}
        className={styles.inlineEditorInput}
        autoFocus
        value={draft.text}
        onChange={(event) => {
          updateText(event.target.value);
        }}
        onKeyDown={(event) => {
          if (event.key === "Enter") {
            commit();
          }
          handleEditorKeyDown(event);
        }}
      />
      {nullAction}
    </div>
  );
}

function CrossIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 16 16" aria-hidden="true" focusable="false">
      <path
        d="M4.28 3.22 8 6.94l3.72-3.72 1.06 1.06L9.06 8l3.72 3.72-1.06 1.06L8 9.06l-3.72 3.72-1.06-1.06L6.94 8 3.22 4.28l1.06-1.06Z"
        fill="currentColor"
      />
    </svg>
  );
}

function PlusIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 24 24"
      aria-hidden="true"
      focusable="false"
      fill="none"
      stroke="currentColor"
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth="2"
    >
      <path d="M12 5v14" />
      <path d="M5 12h14" />
    </svg>
  );
}

function TrashIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 24 24"
      aria-hidden="true"
      focusable="false"
      fill="none"
      stroke="currentColor"
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth="2"
    >
      <path d="M3 6h18" />
      <path d="M8 6V4h8v2" />
      <path d="m6 6 1 14h10l1-14" />
      <path d="M10 11v5" />
      <path d="M14 11v5" />
    </svg>
  );
}

function BackArrowIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 24 24"
      aria-hidden="true"
      focusable="false"
      fill="none"
      stroke="currentColor"
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth="2"
    >
      <path d="m9 14-4-4 4-4" />
      <path d="M5 10h9a5 5 0 0 1 0 10h-1" />
    </svg>
  );
}

function CatalogIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 24 24"
      aria-hidden="true"
      focusable="false"
      fill="none"
      stroke="currentColor"
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth="2"
    >
      <path d="M4 19.5V5a2 2 0 0 1 2-2h12v18H6a2 2 0 0 1-2-1.5Z" />
      <path d="M8 7h6" />
      <path d="M8 11h8" />
      <path d="M8 15h5" />
    </svg>
  );
}

function ColumnsIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 24 24"
      aria-hidden="true"
      focusable="false"
      fill="none"
      stroke="currentColor"
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth="2"
    >
      <path d="M4 6h16" />
      <path d="M4 12h16" />
      <path d="M4 18h16" />
      <path d="M8 4v4" />
      <path d="M15 10v4" />
      <path d="M11 16v4" />
    </svg>
  );
}

function BooleanCellCheckbox({
  checked,
  indeterminate,
  label,
  onToggle,
}: {
  checked: boolean;
  indeterminate: boolean;
  label: string;
  onToggle: (checked: boolean) => void;
}) {
  const checkboxRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (!checkboxRef.current) {
      return;
    }

    checkboxRef.current.indeterminate = indeterminate;
  }, [indeterminate]);

  return (
    <input
      ref={checkboxRef}
      type="checkbox"
      className={styles.booleanCellCheckbox}
      aria-label={label}
      checked={checked}
      onMouseDown={(event) => {
        event.stopPropagation();
      }}
      onClick={(event) => {
        event.stopPropagation();
      }}
      onDoubleClick={(event) => {
        event.stopPropagation();
      }}
      onChange={(event) => {
        event.stopPropagation();
        onToggle(event.target.checked);
      }}
    />
  );
}

function NullCellMarker() {
  return (
    <div className={`${styles.cellContent} ${styles.nullCellMarker}`} title={NULL_CELL_MARKER}>
      {NULL_CELL_MARKER}
    </div>
  );
}

function UnsetCellMarker() {
  return (
    <i
      className={`${styles.cellContent} ${styles.nullCellMarker} ${styles.unsetCellMarker}`}
      title="Unset — this field will be omitted from the insert"
      data-cell-value-state="unset"
    >
      {NULL_CELL_MARKER}
    </i>
  );
}

function RelationCell({
  schema,
  relationTable,
  relationId,
  queryOptions,
}: {
  schema: Record<string, { columns: ColumnDescriptor[] }>;
  relationTable: string;
  relationId: string;
  queryOptions: { propagation: "full" | "local-only"; visibility: "hidden_from_live_query_list" };
}) {
  const queryBuilder = useMemo(
    () => new GenericQueryBuilder(relationTable, schema).where({ id: relationId }).limit(1),
    [relationId, relationTable, schema],
  );
  const { data: relationRows = EMPTY_ROWS } = useAll<DynamicTableRow>(queryBuilder, queryOptions);
  const relationRow = relationRows[0];
  const displayColumn = useMemo(
    () => getRelationDisplayColumn(schema, relationTable),
    [relationTable, schema],
  );
  const displayValue =
    relationRow && displayColumn
      ? formatCellValue(relationRow[displayColumn.name])
      : formatCellValue(relationId);
  const href = buildRelationFilterHref(relationTable, relationId);

  return (
    <div className={styles.relationCell} title={`${relationTable}.${relationId}`}>
      <span className={styles.cellContent}>{displayValue}</span>
      <Link
        to={href}
        className={styles.relationLink}
        aria-label={`Open ${displayValue} in ${relationTable}`}
        onClick={(event) => {
          event.stopPropagation();
        }}
        onMouseDown={(event) => {
          event.stopPropagation();
        }}
      >
        <svg
          className={styles.relationLinkIcon}
          viewBox="0 0 16 16"
          aria-hidden="true"
          focusable="false"
        >
          <path
            d="M6 3h7v7h-1.5V5.56l-6.97 6.97-1.06-1.06 6.97-6.97H6V3zm-2 2H2.5v8h8V12H4V5z"
            fill="currentColor"
          />
        </svg>
      </Link>
    </div>
  );
}

// Stand-in for the grid while the first query result is in flight. Rendered in
// place of the DataGrid (not as its no-rows fallback) so it owns the full body
// width and keeps a header strip — no layout jump when the real grid arrives.
// Width jitter is deterministic (keyed off row index) so the rows read as
// organic data without depending on Math.random.
const SKELETON_ROW_COUNT = 16;
const SKELETON_VALUE_WIDTHS = ["54%", "38%", "66%", "45%", "59%", "33%", "71%", "48%"];

function GridSkeleton() {
  return (
    <div className={styles.skeleton} role="status" aria-live="polite">
      <span className={styles.visuallyHidden}>Loading rows…</span>
      <div className={styles.skeletonHeader} aria-hidden="true">
        <span className={`${styles.skeletonHeaderBar} ${styles.skeletonColId}`} />
        <span className={styles.skeletonHeaderBar} style={{ width: "30%" }} />
        <span className={styles.skeletonHeaderBar} style={{ width: "18%" }} />
      </div>
      <div className={styles.skeletonBody}>
        {Array.from({ length: SKELETON_ROW_COUNT }, (_, rowIndex) => (
          <div key={rowIndex} className={styles.skeletonRow} aria-hidden="true">
            <span className={`${styles.skeletonBar} ${styles.skeletonColId}`} />
            <span
              className={styles.skeletonBar}
              style={{ width: SKELETON_VALUE_WIDTHS[rowIndex % SKELETON_VALUE_WIDTHS.length] }}
            />
            <span className={`${styles.skeletonBar} ${styles.skeletonColFlag}`} />
          </div>
        ))}
      </div>
    </div>
  );
}

function PlainTableView({
  rows,
  gridColumns,
  sorting,
  schema,
  queryOptions,
  schemaColumnById,
  queuedEdits,
  stagedInserts,
  selectedRowIds,
  queuedDeletes,
  pendingScrollToRowId,
  animationScopeKey,
  onSortColumnsChange,
  onQueuedEditsChange,
  onStagedInsertsChange,
  onSelectedRowIdsChange,
  onQueuedSaveErrorChange,
  onQueuedDeletesChange,
  onPendingScrollToRowIdChange,
}: {
  rows: DynamicTableRow[];
  gridColumns: GridColumn[];
  sorting: readonly SortColumn[];
  schema: Record<string, { columns: ColumnDescriptor[] }>;
  queryOptions: { propagation: "full" | "local-only"; visibility: "hidden_from_live_query_list" };
  schemaColumnById: Map<string, ColumnDescriptor>;
  queuedEdits: Record<string, QueuedRowEdits>;
  stagedInserts: StagedInsert[];
  selectedRowIds: Set<string>;
  queuedDeletes: Set<string>;
  pendingScrollToRowId: string | null;
  animationScopeKey: string;
  onSortColumnsChange: (sortColumns: SortColumn[]) => void;
  onQueuedEditsChange: Dispatch<SetStateAction<Record<string, QueuedRowEdits>>>;
  onStagedInsertsChange: Dispatch<SetStateAction<StagedInsert[]>>;
  onSelectedRowIdsChange: Dispatch<SetStateAction<Set<string>>>;
  onQueuedSaveErrorChange: (value: string | null) => void;
  onQueuedDeletesChange: Dispatch<SetStateAction<Set<string>>>;
  onPendingScrollToRowIdChange: (value: string | null) => void;
}) {
  const selectionAnchorRowIdRef = useRef<string | null>(null);
  const dataGridRef = useRef<DataGridHandle | null>(null);
  const animatedRows = useAnimatedGridRows(rows, gridColumns, animationScopeKey);
  const editableRows = useMemo<EditableGridRow[]>(() => {
    const realRows = animatedRows.map((entry) => {
      const rowId = getGridRowId(entry.row);
      const rowQueuedEdits = queuedEdits[rowId];

      return {
        ...entry,
        sourceRow: entry.row,
        row: applyQueuedEditsToRow(entry.row, rowQueuedEdits),
        queuedEdits: rowQueuedEdits,
      };
    });

    if (stagedInserts.length === 0) {
      return realRows;
    }

    return [
      ...realRows,
      ...stagedInserts.map((stagedInsert) => {
        const stagedSourceRow: DynamicTableRow = { id: stagedInsert.id };
        for (const column of gridColumns) {
          const schemaColumn = schemaColumnById.get(column.id);
          if (schemaColumn) {
            stagedSourceRow[column.accessorKey] = getInitialStagedInsertCellValue(schemaColumn);
          }
        }

        return {
          row: applyQueuedEditsToRow(stagedSourceRow, stagedInsert.edits),
          sourceRow: stagedSourceRow,
          queuedEdits: stagedInsert.edits,
          isStagedInsert: true,
          stagedInsertId: stagedInsert.id,
        };
      }),
    ];
  }, [animatedRows, gridColumns, queuedEdits, schemaColumnById, stagedInserts]);

  useEffect(() => {
    selectionAnchorRowIdRef.current = null;
  }, [animationScopeKey]);

  useLayoutEffect(() => {
    if (!pendingScrollToRowId) {
      return;
    }

    const rowIndex = editableRows.findIndex(
      (row) => getGridRowId(row.sourceRow) === pendingScrollToRowId,
    );
    if (rowIndex === -1) {
      return;
    }

    dataGridRef.current?.scrollToCell({ rowIdx: rowIndex });
    onPendingScrollToRowIdChange(null);
  }, [editableRows, onPendingScrollToRowIdChange, pendingScrollToRowId]);

  const rowClass = (row: EditableGridRow): string | undefined => {
    const rowId = getGridRowId(row.sourceRow);
    const rowClasses: string[] = [];

    if (row.isStagedInsert) {
      rowClasses.push(styles.rowStagedInsert);
    } else if (row.rowChangeState === "added") {
      rowClasses.push(styles.rowAdded);
    } else if (row.rowChangeState === "removed") {
      rowClasses.push(styles.rowRemoved);
    } else if (queuedDeletes.has(rowId)) {
      rowClasses.push(styles.rowQueuedDelete);
    }

    if (selectedRowIds.has(rowId)) {
      rowClasses.push(styles.rowSelected);
    }

    return rowClasses.length > 0 ? rowClasses.join(" ") : undefined;
  };
  const queueCellEdit = (
    row: EditableGridRow,
    column: ColumnDescriptor,
    nextEdit: QueuedCellEdit,
  ): void => {
    onQueuedSaveErrorChange(null);

    if (row.isStagedInsert) {
      const stagedInsertId = row.stagedInsertId;
      if (!stagedInsertId) {
        return;
      }

      onStagedInsertsChange((currentStagedInserts) => {
        return currentStagedInserts.map((stagedInsert) => {
          if (stagedInsert.id !== stagedInsertId) {
            return stagedInsert;
          }

          return {
            ...stagedInsert,
            edits: {
              ...stagedInsert.edits,
              [column.name]: nextEdit,
            },
          };
        });
      });
      return;
    }

    const rowId = getGridRowId(row.sourceRow);
    const sourceEdit = createQueuedCellEdit(column, row.sourceRow[column.name]);

    onQueuedEditsChange((currentQueuedEdits) => {
      if (areQueuedCellEditsEqual(nextEdit, sourceEdit)) {
        return removeQueuedCellEdit(currentQueuedEdits, rowId, column.name);
      }

      return {
        ...currentQueuedEdits,
        [rowId]: {
          ...currentQueuedEdits[rowId],
          [column.name]: nextEdit,
        },
      };
    });
  };
  const toggleQueuedDelete = (rowId: string): void => {
    onQueuedSaveErrorChange(null);
    onQueuedDeletesChange((current) => {
      const next = new Set(current);
      if (next.has(rowId)) {
        next.delete(rowId);
      } else {
        next.add(rowId);
      }
      return next;
    });
  };
  const renderers = useMemo<Renderers<EditableGridRow, unknown>>(
    () => ({
      renderCell(key, props) {
        const columnKey = String(props.column.key);
        return (
          <Cell
            key={key}
            {...props}
            data-cell-change-state={props.row.changedCellIds?.[columnKey] ? "updated" : undefined}
          />
        );
      },
      renderRow(key, props) {
        return <Row key={key} {...props} data-row-change-state={props.row.rowChangeState} />;
      },
      noRowsFallback: <div className={styles.emptyState}>No rows</div>,
    }),
    [],
  );
  const columns = useMemo<readonly Column<EditableGridRow>[]>(() => {
    const dataColumns = gridColumns.map((column): Column<EditableGridRow> => {
      const isIdColumn = column.id === "id";
      const schemaColumn = schemaColumnById.get(column.id);
      const isEditable =
        !isIdColumn &&
        schemaColumn &&
        schemaColumn.column_type.type !== "Boolean" &&
        getFieldReadOnlyReason(schemaColumn) === null;

      return {
        key: column.id,
        name: column.header,
        sortable: column.enableSorting,
        resizable: true,
        editable: isEditable,
        width: isIdColumn ? "minmax(148px, 1fr)" : "minmax(120px, 1fr)",
        minWidth: isIdColumn ? 148 : 120,
        headerCellClass: column.enableSorting ? styles.sortableHeaderCell : styles.gridHeaderCell,
        cellClass: (row) =>
          row.changedCellIds?.[column.id]
            ? `${styles.dataGridCell} ${styles.cellUpdated}`
            : styles.dataGridCell,
        renderCell: ({ row }) => {
          if (isIdColumn && row.isStagedInsert) {
            return (
              <span
                className={styles.stagedInsertBadge}
                title="Staged row — not saved yet. Click Save changes to insert it."
              >
                staged
              </span>
            );
          }

          const rawValue = row.row[column.accessorKey];

          if (row.isStagedInsert && rawValue === undefined) {
            return <UnsetCellMarker />;
          }

          if (rawValue === null) {
            return <NullCellMarker />;
          }

          const value = formatCellValue(rawValue);

          if (schemaColumn?.column_type.type === "Boolean") {
            const rowId = getGridRowId(row.sourceRow);
            const rowLabel = row.isStagedInsert ? "staged insert" : rowId;
            return (
              <div className={styles.booleanCell}>
                <BooleanCellCheckbox
                  label={`Toggle ${column.accessorKey} for ${rowLabel}`}
                  checked={rawValue === true || rawValue === "true"}
                  indeterminate={
                    schemaColumn.nullable && (rawValue === null || rawValue === undefined)
                  }
                  onToggle={(checked) => {
                    queueCellEdit(row, schemaColumn, {
                      text: checked ? "true" : "false",
                      isNull: false,
                    });
                  }}
                />
                {schemaColumn.nullable ? (
                  <button
                    type="button"
                    className={styles.inlineNullButton}
                    aria-label={`Set ${column.accessorKey} to NULL for ${rowLabel}`}
                    title="Set to NULL"
                    onMouseDown={(event) => {
                      event.stopPropagation();
                    }}
                    onClick={(event) => {
                      event.stopPropagation();
                      queueCellEdit(row, schemaColumn, {
                        text: "",
                        isNull: true,
                      });
                    }}
                  >
                    <CrossIcon className={styles.inlineNullIcon} />
                  </button>
                ) : null}
              </div>
            );
          }

          if (
            schemaColumn?.references &&
            typeof rawValue === "string" &&
            rawValue.trim().length > 0
          ) {
            return (
              <RelationCell
                schema={schema}
                relationTable={schemaColumn.references}
                relationId={rawValue}
                queryOptions={queryOptions}
              />
            );
          }

          return (
            <div className={styles.cellContent} title={value}>
              {value}
            </div>
          );
        },
        renderEditCell:
          schemaColumn && isEditable
            ? (props) => <QueuedCellEditor {...props} schemaColumn={schemaColumn} />
            : undefined,
        // Live query delivery and row-change animations can replace an otherwise
        // unchanged row object. Queued editors own their draft until commit, so
        // those external identity changes must not close the editor.
        editorOptions: {
          closeOnExternalRowChange: false,
        },
      };
    });

    const actionsColumn: Column<EditableGridRow> = {
      key: ACTIONS_COLUMN_KEY,
      name: "",
      sortable: false,
      resizable: false,
      width: 48,
      minWidth: 44,
      maxWidth: 56,
      headerCellClass: styles.actionsHeaderCell,
      cellClass: styles.actionsGridCell,
      renderCell: ({ row }) => {
        if (row.isStagedInsert) {
          return (
            <div className={styles.actionsCellContent}>
              <button
                type="button"
                className={styles.actionButton}
                aria-label="Cancel staged insert"
                title="Cancel"
                onMouseDown={(event) => {
                  event.stopPropagation();
                }}
                onClick={(event) => {
                  event.stopPropagation();
                  onQueuedSaveErrorChange(null);
                  onStagedInsertsChange((currentStagedInserts) =>
                    currentStagedInserts.filter(
                      (stagedInsert) => stagedInsert.id !== row.stagedInsertId,
                    ),
                  );
                }}
              >
                <CrossIcon className={styles.buttonIcon} />
              </button>
            </div>
          );
        }

        const rowId = getGridRowId(row.sourceRow);
        const isQueuedDelete = queuedDeletes.has(rowId);

        return (
          <div className={styles.actionsCellContent}>
            <button
              type="button"
              className={isQueuedDelete ? styles.actionButton : styles.dangerActionButton}
              aria-label={isQueuedDelete ? `Undo delete ${rowId}` : `Delete ${rowId}`}
              title={isQueuedDelete ? "Undo" : "Delete row"}
              onMouseDown={(event) => {
                event.stopPropagation();
              }}
              onClick={(event) => {
                event.stopPropagation();
                toggleQueuedDelete(rowId);
              }}
            >
              {isQueuedDelete ? (
                <BackArrowIcon className={styles.buttonIcon} />
              ) : (
                <TrashIcon className={styles.buttonIcon} />
              )}
            </button>
          </div>
        );
      },
    };

    return [...dataColumns, actionsColumn];
  }, [
    gridColumns,
    onStagedInsertsChange,
    onQueuedSaveErrorChange,
    queueCellEdit,
    queuedDeletes,
    queryOptions,
    schema,
    schemaColumnById,
    toggleQueuedDelete,
  ]);
  const handleRowsChange = (
    nextRows: EditableGridRow[],
    data: RowsChangeData<EditableGridRow>,
  ): void => {
    const columnId = String(data.column.key);
    const schemaColumn = schemaColumnById.get(columnId);

    if (!schemaColumn) {
      return;
    }

    onQueuedSaveErrorChange(null);

    for (const rowIndex of data.indexes) {
      const nextRow = nextRows[rowIndex];
      if (!nextRow) {
        continue;
      }

      queueCellEdit(
        nextRow,
        schemaColumn,
        createQueuedCellEdit(schemaColumn, nextRow.row[columnId]),
      );
    }
  };
  const selectOnlyRow = (rowId: string): void => {
    onSelectedRowIdsChange((currentSelectedRowIds) => {
      // Avoid changing selection if selected row didn't actually change.
      // This prevents unnecessary re-renders that sometimes prevented cells
      // from entering edit mode on double-click
      if (currentSelectedRowIds.size === 1 && currentSelectedRowIds.has(rowId)) {
        return currentSelectedRowIds;
      }

      return new Set([rowId]);
    });
  };
  const selectRowRange = (
    row: EditableGridRow,
    rowIndex: number,
    isRangeSelection: boolean,
  ): void => {
    const rowId = getGridRowId(row.sourceRow);

    if (!isRangeSelection) {
      selectionAnchorRowIdRef.current = rowId;
      selectOnlyRow(rowId);
      return;
    }

    const anchorRowId = selectionAnchorRowIdRef.current ?? rowId;
    const anchorRowIndex = editableRows.findIndex(
      (editableRow) => getGridRowId(editableRow.sourceRow) === anchorRowId,
    );
    if (anchorRowIndex === -1) {
      selectionAnchorRowIdRef.current = rowId;
      selectOnlyRow(rowId);
      return;
    }

    const startRowIndex = Math.min(anchorRowIndex, rowIndex);
    const endRowIndex = Math.max(anchorRowIndex, rowIndex);
    const nextSelectedRowIds = new Set<string>();
    for (const selectedRow of editableRows.slice(startRowIndex, endRowIndex + 1)) {
      nextSelectedRowIds.add(getGridRowId(selectedRow.sourceRow));
    }

    onSelectedRowIdsChange(nextSelectedRowIds);
  };
  const columnLayoutKey = JSON.stringify(gridColumns.map((column) => column.id));

  return (
    <DataGrid
      key={columnLayoutKey}
      ref={dataGridRef}
      className={`${styles.dataGrid} rdg-dark`}
      columns={columns}
      rows={editableRows}
      rowKeyGetter={(row) => getGridRowId(row.sourceRow)}
      selectedRows={selectedRowIds}
      sortColumns={sorting}
      onSortColumnsChange={onSortColumnsChange}
      onRowsChange={handleRowsChange}
      onCellMouseDown={(_args, event) => {
        if (event.shiftKey) {
          event.preventDefault();
        }
      }}
      onCellClick={(args, event) => {
        selectRowRange(args.row, args.rowIdx, event.shiftKey);
      }}
      onCellKeyDown={(args, event) => {
        if (args.mode === "EDIT") {
          return;
        }

        if (event.key === "Backspace" || event.key === "Delete") {
          const rowId = args.row ? getGridRowId(args.row.sourceRow) : null;
          if (!rowId) {
            return;
          }

          event.preventGridDefault();
          if (args.row?.isStagedInsert) {
            onQueuedSaveErrorChange(null);
            const stagedInsertId = args.row.stagedInsertId;
            if (stagedInsertId) {
              onStagedInsertsChange((currentStagedInserts) =>
                currentStagedInserts.filter((stagedInsert) => stagedInsert.id !== stagedInsertId),
              );
            }
          } else {
            toggleQueuedDelete(rowId);
          }
        }
      }}
      onCellDoubleClick={(args, event) => {
        const schemaColumn = schemaColumnById.get(String(args.column.key));
        const rowId = args.row ? getGridRowId(args.row.sourceRow) : null;
        if (
          !schemaColumn ||
          getFieldReadOnlyReason(schemaColumn) !== null ||
          (rowId !== null && queuedDeletes.has(rowId))
        ) {
          event.preventGridDefault();
          return;
        }

        if (schemaColumn.column_type.type === "Boolean") {
          const rawValue = args.row?.row[schemaColumn.name];
          if (rawValue === undefined || (schemaColumn.nullable && rawValue === null)) {
            queueCellEdit(args.row, schemaColumn, {
              text: "false",
              isNull: false,
            });
          }
          event.preventGridDefault();
          return;
        }

        args.selectCell(true);
        event.preventGridDefault();
      }}
      rowClass={rowClass}
      renderers={renderers}
      rowHeight={30}
      headerRowHeight={32}
      enableVirtualization={false}
    />
  );
}
