import { useEffect, useRef, useState, type DragEvent, type MouseEvent } from "react";
import styles from "./ColumnCustomizationModal.module.css";

export interface CustomizableColumn {
  id: string;
  header: string;
  hiddenByDefault?: true;
}

export interface ColumnPreference {
  id: string;
  visible: boolean;
}

function isColumnPreference(value: unknown): value is ColumnPreference {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Partial<ColumnPreference>;
  return typeof candidate.id === "string" && typeof candidate.visible === "boolean";
}

export function isColumnPreferences(value: unknown): value is ColumnPreference[] {
  return Array.isArray(value) && value.every(isColumnPreference);
}

export function reconcileColumnPreferences(
  columns: readonly CustomizableColumn[],
  storedPreferences: readonly ColumnPreference[] | undefined,
): ColumnPreference[] {
  const availableColumnIds = new Set(columns.map((column) => column.id));
  const seenColumnIds = new Set<string>();
  const preferences: ColumnPreference[] = [];

  for (const preference of storedPreferences ?? []) {
    if (!availableColumnIds.has(preference.id) || seenColumnIds.has(preference.id)) {
      continue;
    }

    seenColumnIds.add(preference.id);
    preferences.push(preference);
  }

  for (const column of columns) {
    if (!seenColumnIds.has(column.id)) {
      preferences.push({ id: column.id, visible: !column.hiddenByDefault });
    }
  }

  return preferences;
}

function movePreference(
  preferences: readonly ColumnPreference[],
  columnId: string,
  nextIndex: number,
): ColumnPreference[] {
  const currentIndex = preferences.findIndex((preference) => preference.id === columnId);
  if (currentIndex === -1 || currentIndex === nextIndex) {
    return [...preferences];
  }

  const nextPreferences = [...preferences];
  const [preference] = nextPreferences.splice(currentIndex, 1);
  if (!preference) {
    return nextPreferences;
  }

  nextPreferences.splice(Math.max(0, Math.min(nextIndex, nextPreferences.length)), 0, preference);
  return nextPreferences;
}

interface ColumnCustomizationModalProps {
  open: boolean;
  columns: readonly CustomizableColumn[];
  preferences: readonly ColumnPreference[];
  onApply: (preferences: ColumnPreference[]) => void;
  onRequestClose: () => void;
}

export function ColumnCustomizationModal({
  open,
  columns,
  preferences,
  onApply,
  onRequestClose,
}: ColumnCustomizationModalProps) {
  const dialogRef = useRef<HTMLDialogElement | null>(null);
  const [draftPreferences, setDraftPreferences] = useState<ColumnPreference[]>(() => [
    ...preferences,
  ]);
  const [draggedColumnId, setDraggedColumnId] = useState<string | null>(null);
  const [dropTargetColumnId, setDropTargetColumnId] = useState<string | null>(null);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) {
      return;
    }

    if (open && !dialog.open) {
      dialog.showModal();
    } else if (!open && dialog.open) {
      dialog.close();
    }
  }, [open]);

  useEffect(() => {
    if (open) {
      setDraftPreferences([...preferences]);
      setDraggedColumnId(null);
      setDropTargetColumnId(null);
    }
  }, [open, preferences]);

  const columnById = new Map(columns.map((column) => [column.id, column]));

  const moveColumnBy = (columnId: string, offset: number) => {
    setDraftPreferences((currentPreferences) => {
      const currentIndex = currentPreferences.findIndex((preference) => preference.id === columnId);
      if (currentIndex === -1) {
        return currentPreferences;
      }
      return movePreference(currentPreferences, columnId, currentIndex + offset);
    });
  };

  const handleDrop = (event: DragEvent<HTMLLIElement>, targetColumnId: string) => {
    event.preventDefault();
    if (!draggedColumnId || draggedColumnId === targetColumnId) {
      setDropTargetColumnId(null);
      return;
    }

    setDraftPreferences((currentPreferences) => {
      const targetIndex = currentPreferences.findIndex(
        (preference) => preference.id === targetColumnId,
      );
      return targetIndex === -1
        ? currentPreferences
        : movePreference(currentPreferences, draggedColumnId, targetIndex);
    });
    setDraggedColumnId(null);
    setDropTargetColumnId(null);
  };

  const handleBackdropClick = (event: MouseEvent<HTMLDialogElement>) => {
    if (event.target === event.currentTarget) {
      onRequestClose();
    }
  };

  return (
    <dialog
      ref={dialogRef}
      className={styles.dialog}
      aria-labelledby="column-customization-title"
      aria-describedby="column-customization-description"
      onCancel={(event) => {
        event.preventDefault();
        onRequestClose();
      }}
      onClose={onRequestClose}
      onClick={handleBackdropClick}
    >
      <div className={styles.panel}>
        <header className={styles.header}>
          <div>
            <h2 id="column-customization-title" className={styles.title}>
              Customize columns
            </h2>
            <p id="column-customization-description" className={styles.description}>
              Choose which columns are visible and drag them to change their order.
            </p>
          </div>
          <button
            type="button"
            className={styles.closeButton}
            aria-label="Close column customization"
            onClick={onRequestClose}
          >
            <CrossIcon />
          </button>
        </header>

        <div className={styles.listHeader} aria-hidden="true">
          <span />
          <span className={styles.orderHeader}>#</span>
          <span>Column</span>
          <span className={styles.visibilityHeader}>Visible</span>
          <span />
        </div>
        <ol className={styles.columnList} aria-label="Column order">
          {draftPreferences.map((preference, index) => {
            const column = columnById.get(preference.id);
            if (!column) {
              return null;
            }

            return (
              <li
                key={preference.id}
                className={`${styles.columnRow} ${
                  dropTargetColumnId === preference.id ? styles.dropTarget : ""
                }`}
                draggable
                onDragStart={(event) => {
                  setDraggedColumnId(preference.id);
                  event.dataTransfer.effectAllowed = "move";
                  event.dataTransfer.setData("text/plain", preference.id);
                }}
                onDragOver={(event) => {
                  event.preventDefault();
                  event.dataTransfer.dropEffect = "move";
                  setDropTargetColumnId(preference.id);
                }}
                onDragLeave={() => {
                  setDropTargetColumnId((currentTarget) =>
                    currentTarget === preference.id ? null : currentTarget,
                  );
                }}
                onDrop={(event) => handleDrop(event, preference.id)}
                onDragEnd={() => {
                  setDraggedColumnId(null);
                  setDropTargetColumnId(null);
                }}
              >
                <span className={styles.dragHandle} aria-hidden="true">
                  <DragHandleIcon />
                </span>
                <span className={styles.order}>{index + 1}</span>
                <span className={styles.columnName}>{column.header}</span>
                <label className={styles.visibilityControl}>
                  <input
                    type="checkbox"
                    checked={preference.visible}
                    aria-label={`Show ${column.header}`}
                    onChange={(event) => {
                      const visible = event.target.checked;
                      setDraftPreferences((currentPreferences) =>
                        currentPreferences.map((currentPreference) =>
                          currentPreference.id === preference.id
                            ? { ...currentPreference, visible }
                            : currentPreference,
                        ),
                      );
                    }}
                  />
                </label>
                <span className={styles.moveButtons}>
                  <button
                    type="button"
                    className={styles.moveButton}
                    aria-label={`Move ${column.header} up`}
                    disabled={index === 0}
                    onClick={() => moveColumnBy(preference.id, -1)}
                  >
                    <ChevronIcon direction="up" />
                  </button>
                  <button
                    type="button"
                    className={styles.moveButton}
                    aria-label={`Move ${column.header} down`}
                    disabled={index === draftPreferences.length - 1}
                    onClick={() => moveColumnBy(preference.id, 1)}
                  >
                    <ChevronIcon direction="down" />
                  </button>
                </span>
              </li>
            );
          })}
        </ol>

        <footer className={styles.footer}>
          <button
            type="button"
            className={styles.resetButton}
            onClick={() => {
              setDraftPreferences(
                columns.map((column) => ({
                  id: column.id,
                  visible: !column.hiddenByDefault,
                })),
              );
            }}
          >
            Reset
          </button>
          <span className={styles.footerActions}>
            <button type="button" className={styles.secondaryButton} onClick={onRequestClose}>
              Cancel
            </button>
            <button
              type="button"
              className={styles.primaryButton}
              onClick={() => {
                onApply(draftPreferences);
                onRequestClose();
              }}
            >
              Apply
            </button>
          </span>
        </footer>
      </div>
    </dialog>
  );
}

function CrossIcon() {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false">
      <path d="m3.5 3.5 9 9m0-9-9 9" />
    </svg>
  );
}

function DragHandleIcon() {
  return (
    <svg viewBox="0 0 12 18" aria-hidden="true" focusable="false">
      <circle cx="3" cy="4" r="1" />
      <circle cx="9" cy="4" r="1" />
      <circle cx="3" cy="9" r="1" />
      <circle cx="9" cy="9" r="1" />
      <circle cx="3" cy="14" r="1" />
      <circle cx="9" cy="14" r="1" />
    </svg>
  );
}

function ChevronIcon({ direction }: { direction: "up" | "down" }) {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false">
      <path d={direction === "up" ? "m4 10 4-4 4 4" : "m4 6 4 4 4-4"} />
    </svg>
  );
}
