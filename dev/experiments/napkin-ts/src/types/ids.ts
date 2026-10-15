import { Interner } from "../support/runtime.js";
import { uuidFromString, uuidToString } from "../support/hash.js";
import type { Reader, Writer } from "../support/postcard.js";
import { principalIsNonempty } from "./identity.js";
import { type AccountId, SYSTEM_ACCOUNT_ID, accountIsSystem } from "./account_registry.js";
import {
  OwnedRecord,
  RecordDescriptor,
  ValueType,
  Value,
  GrooveRecordError,
} from "../_extern/types-protocol.js";

/** UUID-backed identifiers are lowercase hyphenated strings (ordering matches byte order). */
export type NodeUuid = string;
export type SchemaVersionId = string;
export type GlobalPhysicalTableId = string;
export type GlobalPhysicalColumnId = string;
export type GlobalPhysicalEnumVariantId = string;
export type MigrationLensId = string;
export type SchemaLineagePublicationId = string;
export type SchemaFamilyId = string;
export type RowUuid = string;
/** Node-local integer aliases. */
export type NodeAlias = number;
export type SchemaVersionAlias = number;
export type PhysicalTableId = number;
export type PhysicalColumnId = number;

const UUID_RE = /^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$/;
export function parseUuid(s: string): string {
  const t = s.startsWith("urn:uuid:") ? s.slice(9) : s.replace(/^\{(.*)\}$/, "$1");
  if (!UUID_RE.test(t)) throw new Error(`invalid UUID: ${s}`);
  return uuidToString(uuidFromString(t.toLowerCase()));
}
export const tryParseUuid = (s: string): string | undefined => {
  try {
    return parseUuid(s);
  } catch {
    return undefined;
  }
};
export const uuidBytes = (id: string): Uint8Array => uuidFromString(id);
export const uuidFromBytes = (b: Uint8Array): string => uuidToString(b);
export const NIL_UUID = "00000000-0000-0000-0000-000000000000";
export const writeUuid = (w: Writer, id: string) => w.bytes(uuidFromString(id));
export function readUuid(r: Reader): string {
  const b = r.bytes();
  if (b.length !== 16) throw new Error(`invalid UUID length ${b.length}`);
  return uuidToString(b);
}

/** Whole-structure interned author identity with cached canonical spelling. */
export class InternedAuthor {
  rowRecord?: any;
  sessionRecord?: any;
  constructor(
    readonly account: AccountId | undefined,
    readonly issuer: string,
    readonly subject: string,
    readonly canonical: string,
  ) {}
}

const authors = new Interner<InternedAuthor>((a) =>
  JSON.stringify([a.account ?? null, a.issuer, a.subject]),
);
function internAuthor(
  account: AccountId | undefined,
  issuer: string,
  subject: string,
): InternedAuthor {
  const canonical =
    account !== undefined
      ? JSON.stringify([account, issuer, subject])
      : JSON.stringify([issuer, subject]);
  return authors.intern(new InternedAuthor(account, issuer, subject, canonical));
}

export type AuthorSubjectErrorKind =
  | "MissingIssuer"
  | "MissingSubject"
  | "ReservedIssuer"
  | "InvalidCanonical"
  | "NonCanonical"
  | "MissingAccount"
  | "ReservedAccount"
  | "InvalidSystemOrigin";

export class AuthorSubjectError extends Error {
  constructor(
    readonly kind: AuthorSubjectErrorKind,
    readonly detail?: string,
  ) {
    super(authorSubjectErrorMessage(kind, detail));
  }
}
function authorSubjectErrorMessage(kind: AuthorSubjectErrorKind, d?: string): string {
  switch (kind) {
    case "MissingIssuer":
      return "author issuer must be non-empty";
    case "MissingSubject":
      return "author subject must be non-empty";
    case "ReservedIssuer":
      return `author issuer is reserved: ${d}`;
    case "InvalidCanonical":
      return `invalid canonical author subject: ${d}`;
    case "NonCanonical":
      return "author subject is not canonically JSON encoded";
    case "MissingAccount":
      return "persisted row author requires an admitted account";
    case "ReservedAccount":
      return "author account is reserved";
    case "InvalidSystemOrigin":
      return "system row author must retain a node UUID subject";
  }
}

let identityDescriptor: any;
export function authorIdentityDescriptor(): any {
  return (identityDescriptor ??= new RecordDescriptor([
    ["issuer", ValueType.String],
    ["subject", ValueType.String],
  ]));
}

function buildAuthorRecord(
  descriptor: any,
  account: any,
  issuer: string,
  subject: string,
  what: string,
): any {
  const identity = authorIdentityDescriptor();
  const raw = identity.create([Value.String(issuer), Value.String(subject)]);
  if (raw instanceof Error) throw new Error(`${what} identity record`);
  const identityValue = Value.Record(new OwnedRecord(raw, identity));
  return new OwnedRecord(descriptor.create([account, identityValue]), descriptor);
}

function readIdentity(record: any, bad: () => AuthorSubjectError): [string, string] {
  try {
    const span = record.descriptor().fieldSpan(record.raw(), 1);
    const identity = authorIdentityDescriptor().bind(record.raw().subarray(span[0], span[1]));
    return [identity.getStr(0), identity.getStr(1)];
  } catch {
    throw bad();
  }
}

export class AuthorSubject {
  static readonly SYSTEM_ISSUER = "urn:jazz:system";
  static readonly SYSTEM_SUBJECT = "system";
  static readonly SYSTEM_CANONICAL = `["urn:jazz:system","system"]`;
  static readonly LOCAL_FIRST_ISSUER = "urn:jazz:local-first";
  static readonly STATIC_BEARER_ISSUER = "urn:jazz:static-bearer";
  static readonly ANONYMOUS_ISSUER = "urn:jazz:anonymous";
  static readonly SYSTEM = new AuthorSubject("System");
  private static systemRecord: any;

  private constructor(
    readonly kind: "System" | "SystemAt" | "Authenticated",
    readonly author?: InternedAuthor,
  ) {}

  static authenticated(issuer: string, subject: string): AuthorSubject {
    if (!principalIsNonempty(issuer)) throw new AuthorSubjectError("MissingIssuer");
    if (!principalIsNonempty(subject)) throw new AuthorSubjectError("MissingSubject");
    if (AuthorSubject.isReservedIssuer(issuer))
      throw new AuthorSubjectError("ReservedIssuer", issuer);
    return AuthorSubject.intern(issuer, subject);
  }

  static reserved(issuer: string, subject: string): AuthorSubject {
    if (!principalIsNonempty(subject)) throw new AuthorSubjectError("MissingSubject");
    if (
      issuer !== AuthorSubject.LOCAL_FIRST_ISSUER &&
      issuer !== AuthorSubject.STATIC_BEARER_ISSUER &&
      issuer !== AuthorSubject.ANONYMOUS_ISSUER
    )
      throw new AuthorSubjectError("ReservedIssuer", issuer);
    return AuthorSubject.intern(issuer, subject);
  }

  private static intern(issuer: string, subject: string): AuthorSubject {
    return new AuthorSubject("Authenticated", internAuthor(undefined, issuer, subject));
  }
  static fromAuthor(kind: "SystemAt" | "Authenticated", author: InternedAuthor): AuthorSubject {
    return new AuthorSubject(kind, author);
  }

  static systemAt(node: NodeUuid): AuthorSubject {
    return new AuthorSubject(
      "SystemAt",
      internAuthor(SYSTEM_ACCOUNT_ID, AuthorSubject.SYSTEM_ISSUER, node),
    );
  }

  private static isReservedIssuer(issuer: string): boolean {
    return (
      issuer === AuthorSubject.SYSTEM_ISSUER ||
      issuer === AuthorSubject.LOCAL_FIRST_ISSUER ||
      issuer === AuthorSubject.STATIC_BEARER_ISSUER ||
      issuer === AuthorSubject.ANONYMOUS_ISSUER
    );
  }

  withAccount(account: AccountId): AuthorSubject {
    if (this.kind !== "Authenticated") throw new Error("system is not an account principal");
    if (accountIsSystem(account)) throw new Error("system account is not user-owned");
    return new AuthorSubject(
      "Authenticated",
      internAuthor(account, this.author!.issuer, this.author!.subject),
    );
  }

  principalParts(): [string, string] {
    if (this.kind === "System") return [AuthorSubject.SYSTEM_ISSUER, AuthorSubject.SYSTEM_SUBJECT];
    return [this.author!.issuer, this.author!.subject];
  }

  accountId(): AccountId | undefined {
    return this.kind === "System" ? undefined : this.author!.account;
  }

  equals(other: AuthorSubject): boolean {
    return this.kind === other.kind && this.author === other.author;
  }

  private static descriptor: any;
  static recordDescriptor(): any {
    return (AuthorSubject.descriptor ??= new RecordDescriptor([
      ["account", ValueType.Nullable(ValueType.Uuid)],
      ["identity", ValueType.Record(authorIdentityDescriptor())],
    ]));
  }
  static valueType(): any {
    return ValueType.Record(AuthorSubject.recordDescriptor());
  }

  static metadataPath(column: string): [string, string[]] | undefined {
    const dot = column.indexOf(".");
    if (dot < 0) return undefined;
    const root = column.slice(0, dot);
    const path = column.slice(dot + 1);
    if (root !== "$createdBy" && root !== "$updatedBy") return undefined;
    switch (path) {
      case "account":
        return [root, ["account"]];
      case "identity":
        return [root, ["identity"]];
      case "identity.issuer":
        return [root, ["identity", "issuer"]];
      case "identity.subject":
        return [root, ["identity", "subject"]];
      default:
        return undefined;
    }
  }

  private static authorType: any;
  static metadataType(column: string): any | undefined {
    let ty = (AuthorSubject.authorType ??= RowAuthor.valueType());
    if (column === "$createdBy" || column === "$updatedBy") return ty;
    const parsed = AuthorSubject.metadataPath(column);
    if (!parsed) return undefined;
    for (const name of parsed[1]) {
      if (ty.kind !== "Record") return undefined;
      const idx = ty.descriptor.fieldIndex(name);
      if (idx === undefined) return undefined;
      ty = ty.descriptor.fields()[idx]?.valueType;
      if (!ty) return undefined;
    }
    return ty;
  }

  toValue(): any {
    return Value.Record(this.encodedRecord());
  }

  encodedRecord(): any {
    if (this.kind === "System") return (AuthorSubject.systemRecord ??= this.buildRecord());
    return (this.author!.sessionRecord ??= this.buildRecord());
  }

  private buildRecord(): any {
    const [issuer, subject] = this.principalParts();
    const account = this.accountId();
    return buildAuthorRecord(
      AuthorSubject.recordDescriptor(),
      Value.Nullable(account === undefined ? undefined : Value.Uuid(account)),
      issuer,
      subject,
      "author",
    );
  }

  static fromValue(value: any): AuthorSubject {
    const bad = () => new AuthorSubjectError("InvalidCanonical", "invalid author record");
    if (value?.kind !== "Record") throw bad();
    const record = value.record;
    const rowAuthor = record.descriptor().equals(RowAuthor.recordDescriptor());
    if (!rowAuthor && !record.descriptor().equals(AuthorSubject.recordDescriptor())) throw bad();
    let account: string | undefined;
    try {
      const borrowed = record.borrowed();
      account = rowAuthor ? borrowed.getUuid(0) : borrowed.getNullableUuid(0);
    } catch {
      throw bad();
    }
    const [issuer, subject] = readIdentity(record, bad);
    if (issuer === AuthorSubject.SYSTEM_ISSUER) {
      if (!rowAuthor && account === undefined && subject === AuthorSubject.SYSTEM_SUBJECT)
        return AuthorSubject.SYSTEM;
      if (account === undefined) throw new AuthorSubjectError("InvalidSystemOrigin");
      if (account !== SYSTEM_ACCOUNT_ID) throw new AuthorSubjectError("ReservedAccount");
      parseSystemOrigin(subject);
      return new AuthorSubject("SystemAt", internAuthor(account, issuer, subject));
    }
    if (account === SYSTEM_ACCOUNT_ID) throw new AuthorSubjectError("ReservedAccount");
    if (!principalIsNonempty(issuer)) throw new AuthorSubjectError("MissingIssuer");
    if (!principalIsNonempty(subject)) throw new AuthorSubjectError("MissingSubject");
    return new AuthorSubject("Authenticated", internAuthor(account, issuer, subject));
  }

  ownershipId(): string {
    return this.accountId() ?? this.canonical();
  }

  static fromCanonical(canonical: string): AuthorSubject {
    if (canonical === AuthorSubject.SYSTEM_CANONICAL) return AuthorSubject.SYSTEM;
    const [account, issuer, subject] = parseCanonicalParts(canonical);
    if (issuer === AuthorSubject.SYSTEM_ISSUER) {
      if (account === undefined) throw new AuthorSubjectError("ReservedIssuer", issuer);
      if (account !== SYSTEM_ACCOUNT_ID) throw new AuthorSubjectError("ReservedAccount");
      const author = AuthorSubject.systemAt(parseSystemOrigin(subject));
      if (author.canonical() !== canonical) throw new AuthorSubjectError("NonCanonical");
      return author;
    }
    if (account !== undefined && accountIsSystem(account))
      throw new AuthorSubjectError("ReservedAccount");
    if (!principalIsNonempty(issuer)) throw new AuthorSubjectError("MissingIssuer");
    if (!principalIsNonempty(subject)) throw new AuthorSubjectError("MissingSubject");
    const principal = AuthorSubject.intern(issuer, subject);
    const author = account === undefined ? principal : principal.withAccount(account);
    if (author.canonical() !== canonical) throw new AuthorSubjectError("NonCanonical");
    return author;
  }

  static fromUntrustedCanonical(canonical: string): AuthorSubject {
    const [account, issuer, subject] = parseCanonicalParts(canonical);
    const principal = AuthorSubject.authenticated(issuer, subject);
    if (account !== undefined && accountIsSystem(account))
      throw new AuthorSubjectError("ReservedAccount");
    const author = account === undefined ? principal : principal.withAccount(account);
    if (author.canonical() !== canonical) throw new AuthorSubjectError("NonCanonical");
    return author;
  }

  canonical(): string {
    return this.kind === "System" ? AuthorSubject.SYSTEM_CANONICAL : this.author!.canonical;
  }

  isAnonymous(): boolean {
    return this.kind === "Authenticated" && this.author!.issuer === AuthorSubject.ANONYMOUS_ISSUER;
  }

  static compare(a: AuthorSubject, b: AuthorSubject): number {
    const x = a.canonical();
    const y = b.canonical();
    return x < y ? -1 : x > y ? 1 : 0;
  }

  static write(w: Writer, a: AuthorSubject) {
    w.str(a.canonical());
  }
  static read(r: Reader): AuthorSubject {
    return AuthorSubject.fromCanonical(r.str());
  }

  /** groove RecordField impl. */
  static readRaw(bytes: Uint8Array, valueType: any): AuthorSubject {
    try {
      return AuthorSubject.fromValue(Value.readRaw(bytes, valueType));
    } catch {
      throw GrooveRecordError.NonCanonicalRecord();
    }
  }
  static readField(record: any, idx: number): AuthorSubject {
    try {
      return AuthorSubject.fromValue(record.getIdx(idx));
    } catch {
      throw GrooveRecordError.NonCanonicalRecord();
    }
  }
}

/** Durable row and transaction attribution; always carries an account. */
export class RowAuthor {
  private constructor(
    readonly kind: "SystemAt" | "Account",
    readonly author: InternedAuthor,
  ) {}

  static systemAt(node: NodeUuid): RowAuthor {
    return new RowAuthor("SystemAt", AuthorSubject.systemAt(node).author!);
  }

  static fromSession(subject: AuthorSubject, node: NodeUuid): RowAuthor {
    switch (subject.kind) {
      case "System":
        return RowAuthor.systemAt(node);
      case "SystemAt":
        throw new AuthorSubjectError("InvalidSystemOrigin");
      case "Authenticated":
        return RowAuthor.fromAuthenticated(subject.author!);
    }
  }

  static fromPersistedSubject(subject: AuthorSubject): RowAuthor {
    switch (subject.kind) {
      case "System":
        throw new AuthorSubjectError("InvalidSystemOrigin");
      case "SystemAt":
        return RowAuthor.fromSystemAt(subject.author!);
      case "Authenticated":
        return RowAuthor.fromAuthenticated(subject.author!);
    }
  }

  private static fromAuthenticated(author: InternedAuthor): RowAuthor {
    if (author.issuer === AuthorSubject.SYSTEM_ISSUER)
      throw new AuthorSubjectError("ReservedIssuer", author.issuer);
    if (author.account === undefined) throw new AuthorSubjectError("MissingAccount");
    if (accountIsSystem(author.account)) throw new AuthorSubjectError("ReservedAccount");
    return new RowAuthor("Account", author);
  }

  private static fromSystemAt(author: InternedAuthor): RowAuthor {
    if (author.account !== SYSTEM_ACCOUNT_ID) throw new AuthorSubjectError("ReservedAccount");
    if (author.issuer !== AuthorSubject.SYSTEM_ISSUER)
      throw new AuthorSubjectError("InvalidSystemOrigin");
    return new RowAuthor("SystemAt", author);
  }

  accountId(): AccountId {
    const a = this.author.account;
    if (a === undefined) throw new Error("row author construction requires an account");
    return a;
  }

  principalParts(): [string, string] {
    return [this.author.issuer, this.author.subject];
  }

  systemOrigin(): NodeUuid | undefined {
    return this.kind === "SystemAt" ? tryParseUuid(this.author.subject) : undefined;
  }

  asAuthorSubject(): AuthorSubject {
    return AuthorSubject.fromAuthor(
      this.kind === "SystemAt" ? "SystemAt" : "Authenticated",
      this.author,
    );
  }

  private static descriptor: any;
  static recordDescriptor(): any {
    return (RowAuthor.descriptor ??= new RecordDescriptor([
      ["account", ValueType.Uuid],
      ["identity", ValueType.Record(authorIdentityDescriptor())],
    ]));
  }
  static valueType(): any {
    return ValueType.Record(RowAuthor.recordDescriptor());
  }

  toValue(): any {
    return Value.Record(this.encodedRecord());
  }

  encodedRecord(): any {
    return (this.author.rowRecord ??= buildAuthorRecord(
      RowAuthor.recordDescriptor(),
      Value.Uuid(this.accountId()),
      this.author.issuer,
      this.author.subject,
      "row author",
    ));
  }

  static fromValue(value: any): RowAuthor {
    if (value?.kind !== "Record")
      throw new AuthorSubjectError("InvalidCanonical", "invalid row author record");
    return RowAuthor.fromRecord(value.record.borrowed());
  }

  static fromRecord(record: any): RowAuthor {
    const bad = () => new AuthorSubjectError("InvalidCanonical", "invalid row author record");
    if (!record.descriptor().equals(RowAuthor.recordDescriptor())) throw bad();
    let account: AccountId;
    try {
      account = record.getUuid(0);
    } catch {
      throw bad();
    }
    const [issuer, subject] = readIdentity(record, bad);
    if (!principalIsNonempty(issuer)) throw new AuthorSubjectError("MissingIssuer");
    if (!principalIsNonempty(subject)) throw new AuthorSubjectError("MissingSubject");
    if (issuer === AuthorSubject.SYSTEM_ISSUER) {
      if (account !== SYSTEM_ACCOUNT_ID) throw new AuthorSubjectError("ReservedAccount");
      parseSystemOrigin(subject);
      return new RowAuthor("SystemAt", internAuthor(account, issuer, subject));
    }
    if (accountIsSystem(account)) throw new AuthorSubjectError("ReservedAccount");
    return new RowAuthor("Account", internAuthor(account, issuer, subject));
  }

  canonical(): string {
    return this.author.canonical;
  }

  /** groove RecordField impl. */
  static readRaw(bytes: Uint8Array, valueType: any): RowAuthor {
    if (valueType?.kind !== "Record") throw GrooveRecordError.TypeMismatch(valueType);
    try {
      return RowAuthor.fromRecord(valueType.descriptor.bind(bytes));
    } catch {
      throw GrooveRecordError.NonCanonicalRecord();
    }
  }
  static readField(record: any, idx: number): RowAuthor {
    try {
      return RowAuthor.fromRecord(record.getRecord(idx));
    } catch {
      throw GrooveRecordError.NonCanonicalRecord();
    }
  }
}

/** Accept only the lowercase hyphenated UUID spelling for system origins. */
function parseSystemOrigin(subject: string): NodeUuid {
  const node = tryParseUuid(subject);
  if (node === undefined || node !== subject) throw new AuthorSubjectError("InvalidSystemOrigin");
  return node;
}

function parseCanonicalParts(canonical: string): [AccountId | undefined, string, string] {
  let parts: unknown;
  try {
    parts = JSON.parse(canonical);
  } catch (e) {
    throw new AuthorSubjectError("InvalidCanonical", String(e));
  }
  if (!Array.isArray(parts) || !parts.every((p) => typeof p === "string"))
    throw new AuthorSubjectError(
      "InvalidCanonical",
      "invalid type: expected a sequence of strings",
    );
  if (parts.length === 2) return [undefined, parts[0], parts[1]];
  if (parts.length === 3) {
    let account: string;
    try {
      account = parseUuid(parts[0]);
    } catch (e) {
      throw new AuthorSubjectError("InvalidCanonical", (e as Error).message);
    }
    return [account, parts[1], parts[2]];
  }
  throw new AuthorSubjectError("InvalidCanonical", "expected principal or account author");
}
