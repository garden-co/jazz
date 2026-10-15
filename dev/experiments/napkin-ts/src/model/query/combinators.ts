import type { Value } from "../../_extern/model-query.js";
import type { Operand, Predicate } from "./policy_and_request_contracts.js";

export const col = (name: string): Operand => ({ kind: "Column", name });
export const param = (name: string): Operand => ({ kind: "Param", name });
export const claim = (name: string): Operand => ({ kind: "Claim", name });
export const lit = (value: Value): Operand => ({ kind: "Literal", value });

export const eq = (left: Operand, right: Operand): Predicate => ({ kind: "Eq", left, right });
export const ne = (left: Operand, right: Operand): Predicate => ({ kind: "Ne", left, right });
export const allOf = (predicates: Iterable<Predicate>): Predicate => ({
  kind: "All",
  predicates: [...predicates],
});
export const anyOf = (predicates: Iterable<Predicate>): Predicate => ({
  kind: "Any",
  predicates: [...predicates],
});
export const not = (predicate: Predicate): Predicate => ({ kind: "Not", predicate });
export const inList = (left: Operand, values: Iterable<Operand>): Predicate => ({
  kind: "In",
  left,
  values: [...values],
});
export const gt = (left: Operand, right: Operand): Predicate => ({ kind: "Gt", left, right });
export const gte = (left: Operand, right: Operand): Predicate => ({ kind: "Gte", left, right });
export const lt = (left: Operand, right: Operand): Predicate => ({ kind: "Lt", left, right });
export const lte = (left: Operand, right: Operand): Predicate => ({ kind: "Lte", left, right });
export const contains = (left: Operand, right: Operand): Predicate => ({
  kind: "Contains",
  left,
  right,
});
export const isNull = (operand: Operand): Predicate => ({ kind: "IsNull", operand });
