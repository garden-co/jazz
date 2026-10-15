// Durable physical identity metadata and Groove history-table lowering.
import type { EnumOccurrenceRemaps } from "../../_extern/node-physical-ingest.js";

export type PhysicalWriteTarget = "History" | "GlobalCurrent" | "AheadCurrent";

export type PhysicalWriteField =
  | { kind: "Copy"; source: number }
  | { kind: "Decode"; source: number }
  | { kind: "Enum"; source: number; column: number }
  | { kind: "CreatedAtMillis" }
  | { kind: "UpdatedAtMillis" }
  /** A current row's `updated_by`: always stored. */
  | { kind: "UpdatedBy" }
  /** A history image's `updated_by`: null when it is the author of the image's transaction. */
  | { kind: "HistoryUpdatedBy" }
  | { kind: "GlobalTime" };

export type PreparedPhysicalWritePlan = {
  storageTable: string;
  sourceTable: any;
  logicalDescriptor: any;
  physicalDescriptor: any;
  historyDescriptor: any;
  writeFields: PhysicalWriteField[];
  enumRemaps: EnumOccurrenceRemaps[];
};

export * from "./physical/catalogue.js";
export * from "./physical/names.js";
export * from "./physical/bindings.js";
export * from "./physical/enum_lookup.js";
export * from "./physical/projections.js";
export * from "./physical/storage.js";
export * from "./physical/descriptors.js";
