import { JsonObject, LocalNode, RawCoMap } from "cojson";
import { PageInfo } from "./types";
import { GridView } from "./grid-view.js";
import { useState, useMemo } from "react";
import { Button, Icon, Input, Modal } from "../ui";
import { styled } from "goober";
import { restoreCoMapToTimestamp } from "../utils/history";
import { CoValueEditor } from "./co-value-editor.js";
import { isWriter } from "../utils/permissions";

export function CoMapView({
  coValue,
  data,
  node,
  onNavigate,
}: {
  coValue: RawCoMap;
  data: JsonObject;
  node: LocalNode;
  onNavigate: (pages: PageInfo[]) => void;
}) {
  return (
    <>
      <GridView
        data={data}
        onNavigate={onNavigate}
        node={node}
        coValue={coValue}
      />
      <div>
        <AddPropertyModal
          disabled={!isWriter(coValue.group.myRole())}
          coValue={coValue}
          node={node}
        />{" "}
        <RestoreSnapshotModal coValue={coValue} />
      </div>
    </>
  );
}

function AddPropertyModal({
  coValue,
  node,
  disabled,
}: {
  coValue: RawCoMap;
  node: LocalNode;
  disabled: boolean;
}) {
  const [isAddPropertyModalOpen, setIsAddPropertyModalOpen] = useState(false);
  const [propertyName, setPropertyName] = useState("");

  const openAddPropertyModal = () => {
    setIsAddPropertyModalOpen(true);
    setPropertyName("");
  };

  const handleCancel = () => {
    setIsAddPropertyModalOpen(false);
    setPropertyName("");
  };

  return (
    <>
      <Button
        title="Add Property"
        variant="secondary"
        disabled={disabled}
        onClick={openAddPropertyModal}
      >
        <Icon name="add" />
      </Button>

      <Modal
        isOpen={isAddPropertyModalOpen}
        onClose={handleCancel}
        heading="Add Property"
        showButtons={false}
      >
        <Input
          label="Property Name"
          value={propertyName}
          onChange={(e) => setPropertyName(e.target.value)}
          placeholder="Enter property name"
        />
        {propertyName && (
          <EditorContainer>
            <CoValueEditor
              node={node}
              property={propertyName}
              value={undefined}
              coValue={coValue}
              onCancel={handleCancel}
            />
          </EditorContainer>
        )}
      </Modal>
    </>
  );
}

function RestoreSnapshotModal({ coValue }: { coValue: RawCoMap }) {
  const [isRestoreModalOpen, setIsRestoreModalOpen] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState<number>(-1);
  const [removeUnknownProperties, setRemoveUnknownProperties] = useState(false);

  const timestamps = useMemo(
    () => coValue.core.verifiedTransactions.map((tx) => tx.madeAt),
    [coValue.core.verifiedTransactions.length],
  );

  const coMapAtSelectedIndex = useMemo(() => {
    if (selectedIndex === -1) return null;
    return coValue.atTime(timestamps[selectedIndex]!).toJSON() as JsonObject;
  }, [coValue, timestamps, selectedIndex]);

  const openRestoreModal = () => {
    setIsRestoreModalOpen(true);
    setSelectedIndex(timestamps.length - 1);
  };

  const handleRestore = () => {
    if (timestamps.length < 2) return;
    if (timestamps.length === 0) return;

    const selectedTimestamp = timestamps[selectedIndex];
    if (selectedTimestamp === undefined) return;

    restoreCoMapToTimestamp(
      coValue,
      selectedTimestamp,
      removeUnknownProperties,
    );

    setIsRestoreModalOpen(false);
  };

  const handleClose = () => {
    setIsRestoreModalOpen(false);
  };

  const canRestore = isWriter(coValue.group.myRole());

  return (
    <>
      <Button title="Timeline" variant="secondary" onClick={openRestoreModal}>
        <Icon name="history" />
      </Button>

      <Modal
        isOpen={isRestoreModalOpen}
        onClose={handleClose}
        heading="Timeline"
        confirmText="Restore"
        cancelText="Cancel"
        onConfirm={handleRestore}
        onCancel={handleClose}
        showButtons={timestamps.length > 1 && canRestore}
      >
        {timestamps.length > 1 && (
          <>
            <RangeContainer>
              <RangeLabel>Select Timestamp</RangeLabel>
              <RangeInput
                type="range"
                min={0}
                max={Math.max(0, timestamps.length - 1)}
                value={selectedIndex}
                onChange={(e) => setSelectedIndex(Number(e.target.value))}
                disabled={timestamps.length === 0}
              />
              <TimestampDisplay>
                {timestamps[selectedIndex] !== undefined
                  ? new Date(timestamps[selectedIndex]!).toISOString()
                  : "No timestamps available"}
              </TimestampDisplay>
            </RangeContainer>

            {canRestore && (
              <CheckboxContainer>
                <CheckboxInput
                  type="checkbox"
                  id="remove-unknown-properties"
                  checked={removeUnknownProperties}
                  onChange={(e) => setRemoveUnknownProperties(e.target.checked)}
                />
                <CheckboxLabel htmlFor="remove-unknown-properties">
                  Remove unknown properties (properties that don't exist in the
                  selected snapshot)
                </CheckboxLabel>
              </CheckboxContainer>
            )}
          </>
        )}

        {timestamps.length > 0 && timestamps[selectedIndex] !== undefined && (
          <PreviewSection>
            <PreviewLabel>State at that time:</PreviewLabel>
            <PreviewPre>
              {JSON.stringify(coMapAtSelectedIndex, null, 2)}
            </PreviewPre>
          </PreviewSection>
        )}

        {timestamps.length < 2 && (
          <div style={{ color: "var(--j-text-color)" }}>
            At least 2 timestamps are required to restore a snapshot.
          </div>
        )}
      </Modal>
    </>
  );
}

const PreviewSection = styled("div")`
  margin-top: 1.5rem;
`;

const PreviewLabel = styled("div")`
  font-weight: 500;
  margin-bottom: 0.5rem;
  color: var(--j-text-color-strong);
`;

const PreviewPre = styled("pre")`
  background-color: var(--j-foreground);
  border: 1px solid var(--j-border-color);
  border-radius: var(--j-radius-md);
  padding: 1rem;
  overflow-x: auto;
  font-size: 0.875rem;
  max-height: 400px;
  overflow-y: auto;
  color: var(--j-text-color);
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
`;

const RangeContainer = styled("div")`
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
`;

const RangeLabel = styled("label")`
  font-weight: 500;
  color: var(--j-text-color-strong);
  font-size: 0.875rem;
`;

const RangeInput = styled("input")`
  width: 100%;
  height: 0.5rem;
  border-radius: var(--j-radius-sm);
  outline: none;
  -webkit-appearance: none;
  appearance: none;
  background: var(--j-foreground);
  cursor: pointer;

  &::-webkit-slider-thumb {
    -webkit-appearance: none;
    appearance: none;
    width: 1.25rem;
    height: 1.25rem;
    border-radius: 50%;
    background: var(--j-primary-color);
    cursor: pointer;
    border: 2px solid var(--j-background);
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
  }

  &::-moz-range-thumb {
    width: 1.25rem;
    height: 1.25rem;
    border-radius: 50%;
    background: var(--j-primary-color);
    cursor: pointer;
    border: 2px solid var(--j-background);
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
  }

  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
`;

const TimestampDisplay = styled("div")`
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 0.875rem;
  color: var(--j-text-color);
  padding: 0.5rem;
  background-color: var(--j-foreground);
  border: 1px solid var(--j-border-color);
  border-radius: var(--j-radius-md);
  text-align: center;
`;

const CheckboxContainer = styled("div")`
  display: flex;
  align-items: flex-start;
  gap: 0.5rem;
  margin-top: 1rem;
`;

const CheckboxInput = styled("input")`
  width: 1rem;
  height: 1rem;
  margin-top: 0.125rem;
  cursor: pointer;
  accent-color: var(--j-primary-color);
`;

const CheckboxLabel = styled("label")`
  font-size: 0.875rem;
  color: var(--j-text-color);
  cursor: pointer;
  line-height: 1.25rem;
`;

const EditorContainer = styled("div")`
  margin-top: 1rem;
`;
