import type { ColumnDescriptor } from "jazz-tools";
import { useMemo, useState } from "react";
import {
  buildMutationFormFields,
  formatMutationFieldValue,
  type MutationFormMode,
  parseMutationFieldValue,
} from "./row-mutation-form.js";
import styles from "./RowMutationSidebar.module.css";

interface FieldState {
  text: string;
  isNull: boolean;
}

interface RowMutationSidebarProps {
  mode: MutationFormMode;
  tableName: string;
  schemaColumns: ColumnDescriptor[];
  targetRowId: string | null;
  rowValues: Record<string, unknown> | null;
  onCancel: () => void;
  onSave: (updates: Record<string, unknown>) => void | Promise<void>;
}

function modeLabel(mode: MutationFormMode): string {
  return mode === "edit" ? "Edit row" : "Insert row";
}

function saveLabel(mode: MutationFormMode): string {
  return mode === "edit" ? "Save" : "Insert";
}

function getInitialFieldState(
  value: unknown,
  mode: MutationFormMode,
  column: ColumnDescriptor,
): FieldState {
  if (mode === "insert") {
    return {
      text: formatMutationFieldValue(value),
      isNull: column.nullable,
    };
  }

  return {
    text: formatMutationFieldValue(value),
    isNull: value === null || value === undefined,
  };
}

function getFieldState(
  fields: Record<string, FieldState>,
  rowValues: Record<string, unknown>,
  mode: MutationFormMode,
  column: ColumnDescriptor,
): FieldState {
  return fields[column.name] ?? getInitialFieldState(rowValues[column.name], mode, column);
}

function createInitialFields(
  rowValues: Record<string, unknown> | null,
  mode: MutationFormMode,
  schemaColumns: ColumnDescriptor[],
): Record<string, FieldState> {
  if (!rowValues) {
    return {};
  }

  const nextFields: Record<string, FieldState> = {};
  for (const column of schemaColumns) {
    nextFields[column.name] = getInitialFieldState(rowValues[column.name], mode, column);
  }
  return nextFields;
}

export function RowMutationSidebar({
  mode,
  tableName,
  schemaColumns,
  targetRowId,
  rowValues,
  onCancel,
  onSave,
}: RowMutationSidebarProps) {
  const [fields, setFields] = useState<Record<string, FieldState>>(() =>
    createInitialFields(rowValues, mode, schemaColumns),
  );
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [saveError, setSaveError] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);

  const formFields = useMemo(() => buildMutationFormFields(schemaColumns), [schemaColumns]);

  if (!rowValues) {
    return null;
  }

  const clearFieldError = (columnName: string) => {
    setErrors((current) => ({ ...current, [columnName]: "" }));
  };

  const updateFieldState = (
    column: ColumnDescriptor,
    update: (currentField: FieldState) => FieldState,
  ) => {
    setFields((current) => ({
      ...current,
      [column.name]: update(getFieldState(current, rowValues, mode, column)),
    }));
    clearFieldError(column.name);
  };

  return (
    <aside className={styles.sidebar} aria-label={`${modeLabel(mode)} panel`}>
      <form
        className={styles.form}
        onSubmit={async (event) => {
          event.preventDefault();

          const nextErrors: Record<string, string> = {};
          const updates: Record<string, unknown> = {};

          for (const field of formFields) {
            const { column, readOnlyReason } = field;
            if (readOnlyReason) continue;
            const fieldState = getFieldState(fields, rowValues, mode, column);

            if (fieldState.isNull) {
              if (!column.nullable) {
                nextErrors[column.name] = "This column is not nullable.";
              } else {
                updates[column.name] = null;
              }
              continue;
            }

            try {
              updates[column.name] = parseMutationFieldValue(column.column_type, fieldState.text);
            } catch (parseError) {
              nextErrors[column.name] =
                parseError instanceof Error ? parseError.message : "Invalid value.";
            }
          }

          setErrors(nextErrors);
          if (Object.keys(nextErrors).length > 0) return;

          try {
            setIsSaving(true);
            setSaveError(null);
            await onSave(updates);
          } catch (error) {
            setSaveError(error instanceof Error ? error.message : "Could not persist row changes.");
          } finally {
            setIsSaving(false);
          }
        }}
      >
        <header className={styles.header}>
          <h3 className={styles.title}>{modeLabel(mode)}</h3>
          <p className={styles.meta}>
            {tableName} · {mode === "insert" ? "new row" : targetRowId}
          </p>
        </header>

        <div className={styles.fields}>
          <label className={styles.field}>
            <span className={styles.label}>id</span>
            <input
              className={styles.input}
              value={mode === "insert" ? "auto-generated" : (targetRowId ?? "")}
              readOnly
            />
          </label>

          {formFields.map(({ column, readOnlyReason }) => {
            const fieldState = getFieldState(fields, rowValues, mode, column);
            const fieldError = errors[column.name];
            const isReadOnly = readOnlyReason !== null;
            const value = fieldState.text;

            return (
              <div key={column.name} className={styles.fieldWrap}>
                <label className={styles.field}>
                  <span className={styles.label}>{column.name}</span>
                  {column.column_type.type === "Enum" && !isReadOnly ? (
                    <select
                      className={styles.select}
                      value={value}
                      disabled={fieldState.isNull}
                      onChange={(event) =>
                        updateFieldState(column, (currentField) => ({
                          ...currentField,
                          text: event.target.value,
                        }))
                      }
                    >
                      {value.length === 0 ? <option value="">Select value</option> : null}
                      {value.length > 0 && !column.column_type.variants.includes(value) ? (
                        <option value={value}>{value}</option>
                      ) : null}
                      {column.column_type.variants.map((variant) => (
                        <option key={variant} value={variant}>
                          {variant}
                        </option>
                      ))}
                    </select>
                  ) : column.column_type.type === "Boolean" && !isReadOnly ? (
                    <select
                      className={styles.select}
                      value={value}
                      disabled={fieldState.isNull}
                      onChange={(event) =>
                        updateFieldState(column, (currentField) => ({
                          ...currentField,
                          text: event.target.value,
                        }))
                      }
                    >
                      <option value="">Select value</option>
                      <option value="true">true</option>
                      <option value="false">false</option>
                    </select>
                  ) : column.column_type.type === "Json" ||
                    column.column_type.type === "Array" ||
                    column.column_type.type === "Row" ? (
                    <textarea
                      className={styles.textarea}
                      value={value}
                      readOnly={isReadOnly}
                      disabled={fieldState.isNull}
                      onChange={
                        isReadOnly
                          ? undefined
                          : (event) =>
                              updateFieldState(column, (currentField) => ({
                                ...currentField,
                                text: event.target.value,
                              }))
                      }
                    />
                  ) : (
                    <input
                      className={styles.input}
                      value={value}
                      readOnly={isReadOnly}
                      disabled={fieldState.isNull}
                      onChange={
                        isReadOnly
                          ? undefined
                          : (event) =>
                              updateFieldState(column, (currentField) => ({
                                ...currentField,
                                text: event.target.value,
                              }))
                      }
                    />
                  )}
                </label>

                {!isReadOnly && column.nullable ? (
                  <label className={styles.nullable}>
                    <input
                      type="checkbox"
                      checked={fieldState.isNull}
                      onChange={(event) =>
                        updateFieldState(column, (currentField) => ({
                          ...currentField,
                          isNull: event.target.checked,
                        }))
                      }
                    />
                    Set NULL
                  </label>
                ) : null}

                {readOnlyReason === "binary" ? (
                  <p className={styles.hint}>Read-only: binary field</p>
                ) : null}
                {fieldError ? <p className={styles.error}>{fieldError}</p> : null}
              </div>
            );
          })}
        </div>

        {saveError ? <p className={styles.error}>{saveError}</p> : null}

        <footer className={styles.actions}>
          <button type="submit" className={styles.primaryButton} disabled={isSaving}>
            {isSaving ? "Saving..." : saveLabel(mode)}
          </button>
          <button
            type="button"
            className={styles.secondaryButton}
            onClick={onCancel}
            disabled={isSaving}
          >
            Cancel
          </button>
        </footer>
      </form>
    </aside>
  );
}
