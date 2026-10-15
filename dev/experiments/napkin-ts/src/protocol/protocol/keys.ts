import { blake3 as nobleBlake3 } from "@noble/hashes/blake3";
import type { Reader, Writer } from "../../support/postcard.js";
import { SortedMap, cmpStr, cmpBig } from "../../support/collections.js";
import { compareBytes, utf8 } from "../../support/bytes.js";
import { uuidFromString, uuidV5 } from "../../support/hash.js";
import {
  AuthorSubject,
  NIL_UUID,
  readUuid,
  uuidBytes,
  uuidFromBytes,
  writeUuid,
  type SchemaVersionId,
} from "../../types/ids.js";
import type { GlobalTime } from "../../types/time.js";
import { ByteSink, putStr, putValue } from "./catalogue.js";
import {
  canonicalPolicyBindingClaims,
  compareBindingId,
  compareShapeId,
  compareValue,
  policyBindingDirectoryClaimsFromValue,
  policyBindingDirectoryClaimsValue,
  policyDirectoryDescriptor,
  policyDirectoryPayload,
  readBindingId,
  readBranchSelector,
  readBranchViewBase,
  readDurabilityTier,
  readQuery,
  readRelationQueryRequired,
  readShapeId,
  readSnapshotRef,
  readValue,
  writeBindingId,
  writeBranchSelector,
  writeBranchViewBase,
  writeDurabilityTier,
  writeQuery,
  writeRelationQueryRequired,
  writeShapeId,
  writeSnapshotRef,
  writeValue,
  type BindingId,
  type BranchSelector,
  type BranchViewBase,
  type DurabilityTier,
  type ShapeId,
  type SnapshotRef,
} from "../../_extern/types-protocol.js";

export const READ_VIEW_NAMESPACE = uuidFromString("1a87cf70-f8f0-5ae7-a574-1f9b5e4517f1");

// ---- read views ----

export type ReadViewKey = string; // UUID; nil is the default current view
export type ReadViewSourceSpec =
  | { kind: "Current" }
  | { kind: "BranchView"; head: BranchSelector; base?: BranchViewBase }
  | { kind: "Snapshot"; snapshot: SnapshotRef };
export interface ReadViewSpec {
  source: ReadViewSourceSpec;
}
export type BindingSource = "Ordinary" | "RelayAuthoritySession";
export interface RegisterShapeOptions {
  tier: DurabilityTier;
  readView: ReadViewSpec;
  bindingSource: BindingSource;
}

export const defaultRegisterShapeOptions = (): RegisterShapeOptions => ({
  tier: "Global",
  readView: { source: { kind: "Current" } },
  bindingSource: "Ordinary",
});
export const readViewIsDefault = (spec: ReadViewSpec) => spec.source.kind === "Current";
export const branchReadView = (head: BranchSelector, base?: BranchViewBase): ReadViewSpec => ({
  source: { kind: "BranchView", head, base },
});
export const hasDefaultReadView = (o: RegisterShapeOptions) => readViewIsDefault(o.readView);

const READ_VIEW_KEY_CODEC_MAGIC = utf8("JRVK");
const READ_VIEW_KEY_CODEC_VERSION = 2;

function canonicalRegisterShapeOptionsV2Bytes(o: RegisterShapeOptions): Uint8Array {
  const b = new ByteSink();
  b.raw(READ_VIEW_KEY_CODEC_MAGIC);
  b.u8(READ_VIEW_KEY_CODEC_VERSION);
  b.u8(o.tier === "None" ? 0 : o.tier === "Local" ? 1 : 2);
  b.u8(o.bindingSource === "Ordinary" ? 0 : 1);
  putReadViewSource(b, o.readView.source);
  return b.finish();
}

function putReadViewSource(b: ByteSink, source: ReadViewSourceSpec): void {
  switch (source.kind) {
    case "Current":
      b.u8(0);
      break;
    case "BranchView": {
      b.u8(1);
      putBranchSelector(b, source.head);
      const base = source.base;
      if (base === undefined) b.u8(0);
      else if (base.kind === "Current") {
        b.u8(1);
        putBranchSelector(b, base.branch);
      } else {
        b.u8(2);
        putBranchSelector(b, base.branch);
        putSnapshotRef(b, base.snapshot);
      }
      break;
    }
    case "Snapshot":
      b.u8(2);
      putSnapshotRef(b, source.snapshot);
      break;
  }
}

function putBranchSelector(b: ByteSink, selector: BranchSelector): void {
  b.len(selector.values.length);
  for (const [name, value] of selector.values) {
    putStr(b, name);
    b.bytes(value[0]);
  }
}

function putSnapshotRef(b: ByteSink, s: SnapshotRef): void {
  b.raw(uuidBytes(s.owner));
  b.u64(s.globalBase);
  b.u64(s.localBase);
  const dots = [...s.dots].sort(
    (x: any, y: any) => cmpBig(x.time, y.time) || cmpStr(x.node, y.node),
  );
  const unique = dots.filter(
    (d, i) => i === 0 || d.time !== dots[i - 1].time || d.node !== dots[i - 1].node,
  );
  b.len(unique.length);
  for (const dot of unique) {
    b.u64(dot.time);
    b.raw(uuidBytes(dot.node));
  }
}

export function readViewKeyFromOptions(opts: RegisterShapeOptions): ReadViewKey {
  if (
    opts.tier === "Global" &&
    opts.bindingSource === "Ordinary" &&
    readViewIsDefault(opts.readView)
  )
    return NIL_UUID;
  return uuidFromBytes(uuidV5(READ_VIEW_NAMESPACE, canonicalRegisterShapeOptionsV2Bytes(opts)));
}

const TIERS: DurabilityTier[] = ["None", "Local", "Global"];
export const registerShapeOptionsKey = (o: RegisterShapeOptions): string =>
  String.fromCharCode(...canonicalRegisterShapeOptionsV2Bytes(o));
export const compareRegisterShapeOptions = (a: RegisterShapeOptions, b: RegisterShapeOptions) =>
  TIERS.indexOf(a.tier) - TIERS.indexOf(b.tier) ||
  cmpStr(registerShapeOptionsKey(a), registerShapeOptionsKey(b));

function writeReadViewSourceSpec(w: Writer, s: ReadViewSourceSpec): void {
  switch (s.kind) {
    case "Current":
      w.variant(0);
      break;
    case "BranchView":
      writeBranchSelector(w.variant(1), s.head);
      w.option(s.base, writeBranchViewBase);
      break;
    case "Snapshot":
      writeSnapshotRef(w.variant(2), s.snapshot);
      break;
  }
}
function readReadViewSourceSpec(r: Reader): ReadViewSourceSpec {
  switch (r.variant(3, "ReadViewSourceSpec")) {
    case 0:
      return { kind: "Current" };
    case 1:
      return {
        kind: "BranchView",
        head: readBranchSelector(r),
        base: r.option(readBranchViewBase),
      };
    default:
      return { kind: "Snapshot", snapshot: readSnapshotRef(r) };
  }
}

export function writeRegisterShapeOptions(w: Writer, o: RegisterShapeOptions): void {
  writeDurabilityTier(w, o.tier);
  writeReadViewSourceSpec(w, o.readView.source);
  w.variant(o.bindingSource === "Ordinary" ? 0 : 1);
}
export function readRegisterShapeOptions(r: Reader): RegisterShapeOptions {
  return {
    tier: readDurabilityTier(r),
    readView: { source: readReadViewSourceSpec(r) },
    bindingSource: r.variant(2, "BindingSource") === 0 ? "Ordinary" : "RelayAuthoritySession",
  };
}

// ---- subscription keys ----

export interface SubscriptionKey {
  shapeId: ShapeId;
  bindingId: BindingId;
  readView: ReadViewKey;
}
/** BindingViewKey has the same fields and ordering as SubscriptionKey. */
export type BindingViewKey = SubscriptionKey;
export const bindingViewKeyNew = (
  shapeId: ShapeId,
  bindingId: BindingId,
  readView: ReadViewKey,
): BindingViewKey => ({
  shapeId,
  bindingId,
  readView,
});
export const compareSubscriptionKey = (a: SubscriptionKey, b: SubscriptionKey): number =>
  compareShapeId(a.shapeId, b.shapeId) ||
  compareBindingId(a.bindingId, b.bindingId) ||
  cmpStr(a.readView, b.readView);
export const writeSubscriptionKey = (w: Writer, k: SubscriptionKey) => {
  writeShapeId(w, k.shapeId);
  writeBindingId(w, k.bindingId);
  writeUuid(w, k.readView);
};
export const readSubscriptionKey = (r: Reader): SubscriptionKey => ({
  shapeId: readShapeId(r),
  bindingId: readBindingId(r),
  readView: readUuid(r),
});

export interface AuthorityResultKey {
  bindingView: BindingViewKey;
  policyBinding?: PolicyBindingKey;
}
export const authorityResultKeyUnscoped = (bindingView: BindingViewKey): AuthorityResultKey => ({
  bindingView,
});
export const authorityResultKeyPolicyScoped = (
  bindingView: BindingViewKey,
  policyBinding: PolicyBindingKey,
) => ({
  bindingView,
  policyBinding,
});

export interface CoverageKey {
  shapeId: ShapeId;
  bindingId: BindingId;
  opts: RegisterShapeOptions;
  policyBinding?: PolicyBindingKey;
}

// ---- policy bindings ----

/** Claims ordered by an exact canonical comparison key. */
export class CanonicalPolicyClaims {
  readonly comparisonKey: Uint8Array;
  constructor(readonly claims: SortedMap<string, any>) {
    const b = new ByteSink();
    for (const [name, value] of claims) {
      putStr(b, name);
      putValue(b, value);
    }
    this.comparisonKey = b.finish();
  }
  static compare(a: CanonicalPolicyClaims, b: CanonicalPolicyClaims): number {
    return compareBytes(a.comparisonKey, b.comparisonKey);
  }
}
const writeClaims = (w: Writer, claims: SortedMap<string, any>) =>
  w.seq([...claims], (w, [k, v]) => {
    w.str(k);
    writeValue(w, v);
  });
const readClaims = (r: Reader) =>
  new SortedMap<string, any>(
    cmpStr,
    r.seq((r) => [r.str(), readValue(r)] as [string, any]),
  );

export interface DelegatedSessionBinding {
  identity: AuthorSubject;
  claims: SortedMap<string, any>;
}
export const writeDelegatedSessionBinding = (w: Writer, d: DelegatedSessionBinding) => {
  AuthorSubject.write(w, d.identity);
  writeClaims(w, d.claims);
};
export const readDelegatedSessionBinding = (r: Reader): DelegatedSessionBinding => ({
  identity: AuthorSubject.read(r),
  claims: readClaims(r),
});

export class PolicyBindingKey {
  constructor(
    readonly identity: AuthorSubject,
    readonly canonicalClaims: CanonicalPolicyClaims,
  ) {}

  static fromDelegatedSession(session: DelegatedSessionBinding): PolicyBindingKey {
    return new PolicyBindingKey(
      session.identity,
      new CanonicalPolicyClaims(session.claims.clone()),
    );
  }
  static fromCanonicalParts(
    identity: AuthorSubject,
    claims: SortedMap<string, any>,
  ): PolicyBindingKey {
    return new PolicyBindingKey(identity, new CanonicalPolicyClaims(claims));
  }
  claims(): SortedMap<string, any> {
    return this.canonicalClaims.claims;
  }

  directoryValue(): any {
    const derived = canonicalPolicyBindingClaims(this.identity, new Map());
    const claims = this.claims().clone();
    let presence = 0;
    derived.forEach(([name, expected], bit) => {
      const have = claims.get(name);
      if (have !== undefined && compareValue(have, expected) === 0) {
        claims.delete(name);
        presence |= 1 << bit;
      }
    });
    return policyDirectoryPayload(presence, policyBindingDirectoryClaimsValue(new Map(claims)));
  }

  static fromDirectoryValue(identity: AuthorSubject, value: any): PolicyBindingKey {
    if (value?.kind !== "Record") throw new Error("policy directory payload must be record");
    const record = value.record;
    if (!record.descriptor().equals(policyDirectoryDescriptor()))
      throw new Error("policy directory descriptor mismatch");
    const presence = record.getIdx(0);
    if (presence?.kind !== "U8") throw new Error("policy directory presence must be u8");
    const claims = new SortedMap<string, any>(
      cmpStr,
      policyBindingDirectoryClaimsFromValue(record.getIdx(1)),
    );
    canonicalPolicyBindingClaims(identity, new Map()).forEach(([name, expected], bit) => {
      if (presence.value & (1 << bit) && claims.set(name, expected) !== undefined)
        throw new Error("policy directory contains a redundant identity-derived claim");
    });
    return PolicyBindingKey.fromCanonicalParts(identity, claims);
  }

  directoryDigest(): Uint8Array {
    const b = new ByteSink();
    putStr(b, this.identity.canonical());
    b.raw(this.canonicalClaims.comparisonKey);
    return nobleBlake3(b.finish(), { context: utf8("jazz.authority-policy-binding-directory.v1") });
  }

  static compare(a: PolicyBindingKey, b: PolicyBindingKey): number {
    return (
      AuthorSubject.compare(a.identity, b.identity) ||
      CanonicalPolicyClaims.compare(a.canonicalClaims, b.canonicalClaims)
    );
  }
}
export const writePolicyBindingKey = (w: Writer, k: PolicyBindingKey) => {
  AuthorSubject.write(w, k.identity);
  writeClaims(w, k.claims());
};
export const readPolicyBindingKey = (r: Reader) =>
  new PolicyBindingKey(AuthorSubject.read(r), new CanonicalPolicyClaims(readClaims(r)));

// ---- shapes ----

export type ShapeBody = { kind: "Query"; query: any } | { kind: "Relation"; query: any };
export interface ShapeAst {
  version: number;
  schemaVersion: SchemaVersionId;
  body: ShapeBody;
}
export const SHAPE_AST_VERSION = 0;
export const shapeAstNew = (query: any, schemaVersion: SchemaVersionId): ShapeAst => ({
  version: SHAPE_AST_VERSION,
  schemaVersion,
  body: { kind: "Query", query },
});
export const shapeAstNewRelation = (query: any, schemaVersion: SchemaVersionId): ShapeAst => ({
  version: SHAPE_AST_VERSION,
  schemaVersion,
  body: { kind: "Relation", query },
});
export const shapeAstQuery = (s: ShapeAst) => (s.body.kind === "Query" ? s.body.query : undefined);
export const shapeAstFromValidated = (v: any) => shapeAstNew(v.query(), v.schemaVersion());

export function writeShapeAst(w: Writer, s: ShapeAst): void {
  w.varint(s.version);
  writeUuid(w, s.schemaVersion);
  if (s.body.kind === "Query") writeQuery(w.variant(0), s.body.query);
  else writeRelationQueryRequired(w.variant(1), s.body.query);
}
export function readShapeAst(r: Reader): ShapeAst {
  const version = r.varint();
  const schemaVersion = readUuid(r);
  const body: ShapeBody =
    r.variant(2, "ShapeBody") === 0
      ? { kind: "Query", query: readQuery(r) }
      : { kind: "Relation", query: readRelationQueryRequired(r) };
  return { version, schemaVersion, body };
}

// ---- subscribe ----

export type KnownStateCompleteness = "FastCurrentMembership";
export type KnownStateDeclaration =
  | { kind: "Fast"; completeness: KnownStateCompleteness; position: GlobalTime }
  | {
      kind: "FastWithAuthorizationProgress";
      completeness: KnownStateCompleteness;
      position: GlobalTime;
      authorizationProgress: number;
    }
  | {
      kind: "Watermark";
      position: GlobalTime;
      authorizationProgress?: number;
      supportingRevision: Uint8Array;
    };

function writeKnownState(w: Writer, k: KnownStateDeclaration): void {
  switch (k.kind) {
    case "Fast":
      w.variant(0).variant(0).varint64(k.position);
      break;
    case "FastWithAuthorizationProgress":
      w.variant(1).variant(0).varint64(k.position).varint(k.authorizationProgress);
      break;
    case "Watermark":
      w.variant(3).varint64(k.position);
      w.option(k.authorizationProgress, (w, p) => w.varint(p));
      w.raw(k.supportingRevision);
      break;
  }
}
function readKnownState(r: Reader): KnownStateDeclaration {
  const completeness = (): KnownStateCompleteness => (
    r.variant(1, "KnownStateCompleteness"), "FastCurrentMembership"
  );
  switch (r.variant(4, "KnownStateDeclaration")) {
    case 0:
      return { kind: "Fast", completeness: completeness(), position: r.varint64() };
    case 1:
      return {
        kind: "FastWithAuthorizationProgress",
        completeness: completeness(),
        position: r.varint64(),
        authorizationProgress: r.varint(),
      };
    case 2:
      throw new Error("invalid KnownStateDeclaration variant 2");
    default:
      return {
        kind: "Watermark",
        position: r.varint64(),
        authorizationProgress: r.option((r) => r.varint()),
        supportingRevision: r.raw(16).slice(),
      };
  }
}

export interface Subscribe {
  shapeId: ShapeId;
  subscription: SubscriptionKey;
  values: any[];
  knownState?: KnownStateDeclaration;
  delegatedSession?: DelegatedSessionBinding;
}
export const writeSubscribe = (w: Writer, s: Subscribe) => {
  writeShapeId(w, s.shapeId);
  writeSubscriptionKey(w, s.subscription);
  w.seq(s.values, writeValue);
  w.option(s.knownState, writeKnownState);
  w.option(s.delegatedSession, writeDelegatedSessionBinding);
};
export const readSubscribe = (r: Reader): Subscribe => ({
  shapeId: readShapeId(r),
  subscription: readSubscriptionKey(r),
  values: r.seq(readValue),
  knownState: r.option(readKnownState),
  delegatedSession: r.option(readDelegatedSessionBinding),
});

export type SubscribeServerFailureCode =
  | "TableNotFound"
  | "SchemaResolution"
  | "QueryValidation"
  | "QueryLowering"
  | "PolicyEvaluation"
  | "Internal";
const FAILURE_CODES: SubscribeServerFailureCode[] = [
  "TableNotFound",
  "SchemaResolution",
  "QueryValidation",
  "QueryLowering",
  "PolicyEvaluation",
  "Internal",
];
export type SubscribeRejectReason =
  | { kind: "UnsupportedShapeCapability"; detail: string }
  | { kind: "ShapeRegistrationPendingCatalogueAdmission" }
  | { kind: "ServerFailure"; code: SubscribeServerFailureCode }
  | { kind: "InvalidAuthoritySourceClosure"; transition: string };

export function writeSubscribeRejectReason(w: Writer, r: SubscribeRejectReason): void {
  switch (r.kind) {
    case "UnsupportedShapeCapability":
      w.variant(0).str(r.detail);
      break;
    case "ShapeRegistrationPendingCatalogueAdmission":
      w.variant(1);
      break;
    case "ServerFailure":
      w.variant(2).variant(FAILURE_CODES.indexOf(r.code));
      break;
    case "InvalidAuthoritySourceClosure":
      w.variant(3).str(r.transition);
      break;
  }
}
export function readSubscribeRejectReason(r: Reader): SubscribeRejectReason {
  switch (r.variant(4, "SubscribeRejectReason")) {
    case 0:
      return { kind: "UnsupportedShapeCapability", detail: r.str() };
    case 1:
      return { kind: "ShapeRegistrationPendingCatalogueAdmission" };
    case 2:
      return {
        kind: "ServerFailure",
        code: FAILURE_CODES[r.variant(6, "SubscribeServerFailureCode")],
      };
    default:
      return { kind: "InvalidAuthoritySourceClosure", transition: r.str() };
  }
}
