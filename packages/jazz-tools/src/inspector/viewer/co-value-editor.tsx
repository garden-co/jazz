import { LocalNode, RawCoValue } from "cojson";
import { JsonValue } from "cojson";
import { useState } from "react";
import { styled } from "goober";
import { Button } from "../ui/button.js";
import { Select } from "../ui/select.js";

type ValueType =
  | "number"
  | "string"
  | "true"
  | "false"
  | "object"
  | "null"
  | "undefined";

export function CoValueEditor({
  node,
  property,
  value,
  coValue,
  onCancel,
}: {
  node: LocalNode;
  property: string;
  value: JsonValue | undefined;
  coValue: RawCoValue;
  onCancel: () => void;
}) {
  const getInitialType = (): ValueType => {
    if (value === null) return "null";
    if (value === undefined) return "undefined";
    if (typeof value === "number") return "number";
    if (typeof value === "string") return "string";
    if (typeof value === "boolean") return value ? "true" : "false";
    if (typeof value === "object") return "object";
    return "undefined";
  };

  const [selectedType, setSelectedType] = useState<ValueType>(getInitialType());
  const [editValue, setEditValue] = useState(
    value === undefined || value === null
      ? ""
      : typeof value === "object"
        ? JSON.stringify(value, null, 2)
        : String(value),
  );

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    e.stopPropagation();

    let newValue;
    switch (selectedType) {
      case "null":
        newValue = null;
        break;
      case "undefined":
        newValue = undefined;
        break;
      case "true":
        newValue = true;
        break;
      case "false":
        newValue = false;
        break;
      case "number":
        newValue = parseFloat(editValue);
        break;
      case "string":
        newValue = editValue;
        break;
      case "object":
        newValue = JSON.parse(editValue);
        break;
      default:
        throw new Error(`Invalid type: ${selectedType}`);
    }

    coValue.core.makeTransaction(
      [
        {
          op: "set",
          key: property,
          value: newValue,
        },
      ],
      "private",
    );

    onCancel();
  };

  const showTextarea =
    selectedType === "number" ||
    selectedType === "string" ||
    selectedType === "object";

  return (
    <EditForm onSubmit={handleSubmit}>
      <Select
        label="Type"
        value={selectedType}
        onChange={(e) => {
          setSelectedType(e.target.value as ValueType);
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <option value="number">number</option>
        <option value="string">string</option>
        <option value="true">true</option>
        <option value="false">false</option>
        <option value="object">object</option>
        <option value="null">null</option>
        <option value="undefined">undefined</option>
      </Select>
      {showTextarea && (
        <StyledTextarea
          value={editValue}
          onChange={(e) => setEditValue(e.target.value)}
          onClick={(e) => e.stopPropagation()}
        />
      )}
      <FormActions>
        <Button type="button" variant="secondary" onClick={onCancel}>
          Cancel
        </Button>
        <Button type="submit" variant="primary">
          Submit
        </Button>
      </FormActions>
    </EditForm>
  );
}

const EditForm = styled("form")`
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
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
