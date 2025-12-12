import { JsonObject, LocalNode, RawCoPlainText } from "cojson";
import { useState } from "react";
import { styled } from "goober";
import { type CoPlainText, co } from "jazz-tools";
import { isWriter } from "../utils/permissions.js";
import { Button } from "../ui/button.js";
import { RawDataCard } from "./raw-data-card.js";
import { Icon } from "../ui/icon.js";

export function CoPlainTextView({
  data,
  coValue,
}: {
  data: JsonObject;
  coValue: RawCoPlainText;
  node: LocalNode;
}) {
  const currentText = Object.values(data).join("");
  const [isEditing, setIsEditing] = useState(false);
  const [editValue, setEditValue] = useState("");
  const canEdit = isWriter(coValue.group.myRole());

  const handleEditClick = () => {
    setIsEditing(true);
    setEditValue(currentText);
  };

  const handleCancel = () => {
    setIsEditing(false);
    setEditValue(currentText);
  };

  const handleSave = (e: React.FormEvent) => {
    e.preventDefault();
    e.stopPropagation();

    const coPlainText = co.plainText().fromRaw(coValue);
    coPlainText.$jazz.applyDiff(editValue);

    setIsEditing(false);
  };

  if (!data) return;

  if (isEditing) {
    return (
      <>
        <EditForm onSubmit={handleSave}>
          <StyledTextarea
            value={editValue}
            onChange={(e) => setEditValue(e.target.value)}
            onClick={(e) => e.stopPropagation()}
          />
          <FormActions>
            <Button type="button" variant="secondary" onClick={handleCancel}>
              Cancel
            </Button>
            <Button type="submit" variant="primary">
              Save
            </Button>
          </FormActions>
        </EditForm>
        <RawDataCard data={data} />
      </>
    );
  }

  return (
    <>
      <p>{currentText}</p>
      <div>
        {canEdit && (
          <Button variant="secondary" onClick={handleEditClick} title="Edit">
            <Icon name="edit" />
          </Button>
        )}
      </div>
      <RawDataCard data={data} />
    </>
  );
}

const EditForm = styled("form")`
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  margin-bottom: 1rem;
`;

const StyledTextarea = styled("textarea")`
  width: 100%;
  min-height: 120px;
  border-radius: var(--j-radius-md);
  border: 1px solid var(--j-border-color);
  padding: 0.5rem 0.875rem;
  box-shadow: var(--j-shadow-sm);
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 0.875rem;
  background-color: white;
  color: var(--j-text-color-strong);
  resize: vertical;

  @media (prefers-color-scheme: dark) {
    background-color: var(--j-foreground);
  }
`;

const FormActions = styled("div")`
  display: flex;
  gap: 0.5rem;
  justify-content: flex-end;
`;
