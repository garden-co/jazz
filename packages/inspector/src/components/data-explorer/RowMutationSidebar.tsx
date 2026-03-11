import type { ColumnDescriptor, DynamicTableRow } from "jazz-tools";
import { useEffect, useMemo, useState } from "react";
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
  targetRow: DynamicTableRow | null;
  onCancel: () => void;
  onSave: (updates: Record<string, unknown>) => void | Promise<void>;
}

function modeLabel(mode: MutationFormMode): string {
  return mode === "edit" ? "Edit row" : "Insert row";
}

function saveLabel(mode: MutationFormMode): string {
  return mode === "edit" ? "Save" : "Insert";
}

function getInitialFieldState(value: unknown): FieldState {
  return {
    text: formatMutationFieldValue(value),
    isNull: value === null || value === undefined,
  };
}

export function RowMutationSidebar({
  mode,
  tableName,
  schemaColumns,
  targetRow,
  onCancel,
  onSave,
}: RowMutationSidebarProps) {
  const [fields, setFields] = useState<Record<string, FieldState>>({});
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [saveError, setSaveError] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);

  const formFields = useMemo(() => buildMutationFormFields(schemaColumns), [schemaColumns]);

  useEffect(() => {
    if (!targetRow) {
      setFields({});
      setErrors({});
      setSaveError(null);
      setIsSaving(false);
      return;
    }
    const nextFields: Record<string, FieldState> = {};
    for (const column of schemaColumns) {
      nextFields[column.name] = getInitialFieldState(targetRow[column.name]);
    }
    setFields(nextFields);
    setErrors({});
    setSaveError(null);
    setIsSaving(false);
  }, [targetRow, schemaColumns]);

  if (!targetRow) {
    return null;
  }

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
            const fieldState = fields[column.name] ?? getInitialFieldState(targetRow[column.name]);

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
            {tableName} · {targetRow.id}
          </p>
        </header>

        <div className={styles.fields}>
          <label className={styles.field}>
            <span className={styles.label}>id</span>
            <input className={styles.input} value={targetRow.id} readOnly />
          </label>

          {formFields.map(({ column, readOnlyReason }) => {
            const fieldState = fields[column.name] ?? getInitialFieldState(targetRow[column.name]);
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
                      onChange={(event) => {
                        const nextValue = event.target.value;
                        setFields((current) => ({
                          ...current,
                          [column.name]: { ...fieldState, text: nextValue },
                        }));
                        setErrors((current) => ({ ...current, [column.name]: "" }));
                      }}
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
                      onChange={(event) => {
                        const nextValue = event.target.value;
                        setFields((current) => ({
                          ...current,
                          [column.name]: { ...fieldState, text: nextValue },
                        }));
                        setErrors((current) => ({ ...current, [column.name]: "" }));
                      }}
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
                      onChange={(event) => {
                        const nextValue = event.target.value;
                        setFields((current) => ({
                          ...current,
                          [column.name]: { ...fieldState, text: nextValue },
                        }));
                        setErrors((current) => ({ ...current, [column.name]: "" }));
                      }}
                    />
                  ) : (
                    <input
                      className={styles.input}
                      value={value}
                      readOnly={isReadOnly}
                      disabled={fieldState.isNull}
                      onChange={(event) => {
                        const nextValue = event.target.value;
                        setFields((current) => ({
                          ...current,
                          [column.name]: { ...fieldState, text: nextValue },
                        }));
                        setErrors((current) => ({ ...current, [column.name]: "" }));
                      }}
                    />
                  )}
                </label>

                {!isReadOnly && column.nullable ? (
                  <label className={styles.nullable}>
                    <input
                      type="checkbox"
                      checked={fieldState.isNull}
                      onChange={(event) => {
                        const checked = event.target.checked;
                        setFields((current) => ({
                          ...current,
                          [column.name]: { ...fieldState, isNull: checked },
                        }));
                        setErrors((current) => ({ ...current, [column.name]: "" }));
                      }}
                    />
                    Set NULL
                  </label>
                ) : null}

                {readOnlyReason === "binary" ? (
                  <p className={styles.hint}>Read-only: binary field</p>
                ) : null}
                {readOnlyReason === "foreign-key" ? (
                  <p className={styles.hint}>Read-only: foreign key field</p>
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
