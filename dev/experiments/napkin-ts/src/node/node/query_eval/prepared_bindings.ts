import { blake3, toHex, utf8, concatBytes } from "../../../support/index.js";
import type {
  Binding,
  AuthorSubject,
  PolicyContext,
  ClaimPath,
  Value,
  ValidatedQuery,
  ColumnType,
  ProgramClaimParam,
  PreparedQueryParam,
} from "../../../_extern/node-qe-b.js";
import {
  Err,
  QueryError,
  coercePreparedBindingValue,
  authorClaimPathKey,
  authorPolicyClaims,
  policyClaimAtPath,
  debugFmt,
} from "../../../_extern/node-qe-b.js";
import { PreparedClaimBindingMode, defaultPolicyClaimValues } from "./bindings.js";

/** BTreeMap<String, T>: plain Map or SortedMap, read-only use. */
type StrMap<T> = { readonly size: number; [Symbol.iterator](): Iterator<[string, T]> };

export { coercePreparedBindingValue };

export function queryBindingValueSignature(binding: Binding): string {
  return [...binding.values().keys()].join(",");
}

export function policyPlanCacheSignature(
  binding: Binding,
  identity: AuthorSubject,
  claimsScope: string,
): string {
  return `${queryBindingValueSignature(binding)}|subject=${debugFmt(identity)}|claims=${claimsScope}`;
}

export function queryBindingSourceShapeForPreparedParams(params: PreparedQueryParam[]): string {
  const userParams = new Map<string, ColumnType>();
  const claimParams = new Map<string, ProgramClaimParam>();
  for (const param of params) {
    if (param.source.kind === "User") userParams.set(param.name, param.ty);
    else claimParams.set(param.name, { path: param.source.path, ty: param.ty });
  }
  return queryBindingSourceShapeForParts(userParams, claimParams);
}

const sortedEntries = <V>(m: StrMap<V>) => [...m].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));

export function queryBindingSourceShapeForParts(
  paramTypes: StrMap<ColumnType>,
  claimParams: StrMap<ProgramClaimParam>,
): string {
  const parts: Uint8Array[] = [utf8("jazz-binding-source-v1")];
  const pushUsize = (n: number) => {
    const b = new Uint8Array(8);
    new DataView(b.buffer).setBigUint64(0, BigInt(n), true);
    parts.push(b);
  };
  const pushStr = (s: string) => {
    const b = utf8(s);
    pushUsize(b.length);
    parts.push(b);
  };
  pushUsize(paramTypes.size);
  for (const [name, ty] of sortedEntries(paramTypes)) {
    pushStr(name);
    pushStr(debugFmt(ty));
  }
  pushUsize(claimParams.size);
  for (const [name, claim] of sortedEntries(claimParams)) {
    pushStr(name);
    pushUsize(claim.path.length);
    for (const segment of claim.path) pushStr(segment);
    pushStr(debugFmt(claim.ty));
  }
  return `jazz-query-binding:${toHex(blake3(concatBytes(...parts)))}`;
}

export function queryBindingSourceShapeForPartsIfNeeded(
  paramTypes: StrMap<ColumnType>,
  claimParams: StrMap<ProgramClaimParam>,
): string | undefined {
  return paramTypes.size || claimParams.size
    ? queryBindingSourceShapeForParts(paramTypes, claimParams)
    : undefined;
}

export function authorizationBindingSourceShape(
  shape: ValidatedQuery,
  extraUserParams: StrMap<ColumnType>,
  claimParams: StrMap<ProgramClaimParam>,
): string | undefined {
  const paramTypes = new Map<string, ColumnType>([...shape.params(), ...extraUserParams]);
  return queryBindingSourceShapeForPartsIfNeeded(paramTypes, claimParams);
}

export function bindingValuesForPlan(
  binding: Binding,
  params: PreparedQueryParam[],
  policy: PolicyContext,
  mode: PreparedClaimBindingMode,
): Value[] {
  return params.map((param) => {
    if (param.source.kind === "User") {
      const value = binding.values().get(param.name);
      if (value === undefined) throw QueryError.MissingParam(param.name);
      return coercePreparedBindingValue(value, param.ty);
    }
    const path: ClaimPath = param.source.path;
    const value = preparedClaimValue(path, policy);
    if (value === undefined) {
      if (mode === PreparedClaimBindingMode.FailClosedAuthorizationSupport)
        throw Err.AuthorizationSupportMissingClaim(path.join("."));
      throw Err.InvalidStoredValue("claim prepared param is not bound");
    }
    return coercePreparedBindingValue(value, param.ty);
  });
}

export function preparedClaimValue(path: ClaimPath, policy: PolicyContext): Value | undefined {
  if (policy.kind === "System")
    throw Err.InvalidStoredValue("claim prepared params require an identity policy context");
  const { permissionSubject, claims } = policy;
  const name = authorClaimPathKey(path);
  if (name !== undefined) return authorPolicyClaims(permissionSubject).get(name);
  const value = policyClaimAtPath(claims, path);
  if (value !== undefined) return value;
  if (path.length === 1) return defaultPolicyClaimValues(permissionSubject).get(path[0]);
  return undefined;
}

export function coerceBindingValuesForShape(
  shape: ValidatedQuery,
  values: Map<string, Value>,
): void {
  for (const [name, value] of values) {
    const ty = shape.params().get(name);
    if (ty !== undefined) values.set(name, coercePreparedBindingValue(value, ty));
  }
}
