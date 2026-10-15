import type { PhysicalColumnId } from "../../_extern/engine.js";

export type CurrentRowBindingRole = "PhysicalColumn" | "LogicalField";

export type CurrentRowResultVisibility = "ApplicationCell" | "PublicProvenance" | "HiddenMetadata";

export function currentRowMetadataVisibility(name: string): CurrentRowResultVisibility {
  switch (name) {
    case "$createdBy":
    case "$createdAt":
    case "$updatedBy":
    case "$updatedAt":
      return "PublicProvenance";
    default:
      return "HiddenMetadata";
  }
}

export type CurrentRowPublicationField =
  | { kind: "StoredColumn"; id: PhysicalColumnId; outputName: string }
  | { kind: "ResultField"; name: string; visibility: CurrentRowResultVisibility }
  | { kind: "UnresolvedSourceCell"; outputName: string };

export function publicationPublicName(field: CurrentRowPublicationField): string | undefined {
  if (field.kind !== "ResultField") return field.outputName;
  return field.visibility === "HiddenMetadata" ? undefined : field.name;
}

export function publicationApplicationName(field: CurrentRowPublicationField): string | undefined {
  if (field.kind !== "ResultField") return field.outputName;
  return field.visibility === "ApplicationCell" ? field.name : undefined;
}

export function publicationRole(field: CurrentRowPublicationField): CurrentRowBindingRole {
  return field.kind === "ResultField" ? "LogicalField" : "PhysicalColumn";
}
