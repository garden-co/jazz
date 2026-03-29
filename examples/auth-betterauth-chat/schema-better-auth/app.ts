// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonValue }
  | JsonValue[];

export type PermissionIntrospectionColumn = "$canRead" | "$canEdit" | "$canDelete";
export interface PermissionIntrospectionColumns {
  $canRead: boolean | null;
  $canEdit: boolean | null;
  $canDelete: boolean | null;
}

export interface BetterAuthUser {
  id: string;
  name: string;
  email: string;
  emailVerified: boolean;
  image?: string;
  createdAt: Date;
  updatedAt: Date;
  role?: string;
  banned?: boolean;
  banReason?: string;
  banExpires?: Date;
}

export interface BetterAuthSession {
  id: string;
  expiresAt: Date;
  token: string;
  createdAt: Date;
  updatedAt: Date;
  ipAddress?: string;
  userAgent?: string;
  userId: string;
  impersonatedBy?: string;
}

export interface BetterAuthAccount {
  id: string;
  accountId: string;
  providerId: string;
  userId: string;
  accessToken?: string;
  refreshToken?: string;
  idToken?: string;
  accessTokenExpiresAt?: Date;
  refreshTokenExpiresAt?: Date;
  scope?: string;
  password?: string;
  createdAt: Date;
  updatedAt: Date;
}

export interface BetterAuthVerification {
  id: string;
  identifier: string;
  value: string;
  expiresAt: Date;
  createdAt: Date;
  updatedAt: Date;
}

export interface BetterAuthJwk {
  id: string;
  publicKey: string;
  privateKey: string;
  createdAt: Date;
  expiresAt?: Date;
}

export interface BetterAuthUserInit {
  name: string;
  email: string;
  emailVerified: boolean;
  image?: string;
  createdAt: Date;
  updatedAt: Date;
  role?: string;
  banned?: boolean;
  banReason?: string;
  banExpires?: Date;
}

export interface BetterAuthSessionInit {
  expiresAt: Date;
  token: string;
  createdAt: Date;
  updatedAt: Date;
  ipAddress?: string;
  userAgent?: string;
  userId: string;
  impersonatedBy?: string;
}

export interface BetterAuthAccountInit {
  accountId: string;
  providerId: string;
  userId: string;
  accessToken?: string;
  refreshToken?: string;
  idToken?: string;
  accessTokenExpiresAt?: Date;
  refreshTokenExpiresAt?: Date;
  scope?: string;
  password?: string;
  createdAt: Date;
  updatedAt: Date;
}

export interface BetterAuthVerificationInit {
  identifier: string;
  value: string;
  expiresAt: Date;
  createdAt: Date;
  updatedAt: Date;
}

export interface BetterAuthJwkInit {
  publicKey: string;
  privateKey: string;
  createdAt: Date;
  expiresAt?: Date;
}

export interface BetterAuthUserWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  email?: string | { eq?: string; ne?: string; contains?: string };
  emailVerified?: boolean;
  image?: string | { eq?: string; ne?: string; contains?: string };
  createdAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  updatedAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  role?: string | { eq?: string; ne?: string; contains?: string };
  banned?: boolean;
  banReason?: string | { eq?: string; ne?: string; contains?: string };
  banExpires?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface BetterAuthSessionWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  expiresAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  token?: string | { eq?: string; ne?: string; contains?: string };
  createdAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  updatedAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  ipAddress?: string | { eq?: string; ne?: string; contains?: string };
  userAgent?: string | { eq?: string; ne?: string; contains?: string };
  userId?: string | { eq?: string; ne?: string };
  impersonatedBy?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface BetterAuthAccountWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  accountId?: string | { eq?: string; ne?: string; contains?: string };
  providerId?: string | { eq?: string; ne?: string; contains?: string };
  userId?: string | { eq?: string; ne?: string };
  accessToken?: string | { eq?: string; ne?: string; contains?: string };
  refreshToken?: string | { eq?: string; ne?: string; contains?: string };
  idToken?: string | { eq?: string; ne?: string; contains?: string };
  accessTokenExpiresAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  refreshTokenExpiresAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  scope?: string | { eq?: string; ne?: string; contains?: string };
  password?: string | { eq?: string; ne?: string; contains?: string };
  createdAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  updatedAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface BetterAuthVerificationWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  identifier?: string | { eq?: string; ne?: string; contains?: string };
  value?: string | { eq?: string; ne?: string; contains?: string };
  expiresAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  createdAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  updatedAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface BetterAuthJwkWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  publicKey?: string | { eq?: string; ne?: string; contains?: string };
  privateKey?: string | { eq?: string; ne?: string; contains?: string };
  createdAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  expiresAt?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

type AnyBetterAuthUserQueryBuilder<T = any> = {
  readonly _table: "better_auth_user";
} & QueryBuilder<T>;
type AnyBetterAuthSessionQueryBuilder<T = any> = {
  readonly _table: "better_auth_session";
} & QueryBuilder<T>;
type AnyBetterAuthAccountQueryBuilder<T = any> = {
  readonly _table: "better_auth_account";
} & QueryBuilder<T>;
type AnyBetterAuthVerificationQueryBuilder<T = any> = {
  readonly _table: "better_auth_verification";
} & QueryBuilder<T>;
type AnyBetterAuthJwkQueryBuilder<T = any> = {
  readonly _table: "better_auth_jwks";
} & QueryBuilder<T>;

export interface BetterAuthUserInclude {
  better_auth_sessionViaUser?:
    | true
    | BetterAuthSessionInclude
    | AnyBetterAuthSessionQueryBuilder<any>;
  better_auth_accountViaUser?:
    | true
    | BetterAuthAccountInclude
    | AnyBetterAuthAccountQueryBuilder<any>;
}

export interface BetterAuthSessionInclude {
  user?: true | BetterAuthUserInclude | AnyBetterAuthUserQueryBuilder<any>;
}

export interface BetterAuthAccountInclude {
  user?: true | BetterAuthUserInclude | AnyBetterAuthUserQueryBuilder<any>;
}

export type BetterAuthUserIncludedRelations<
  I extends BetterAuthUserInclude = {},
  R extends boolean = false,
> = {
  [K in keyof I]-?: K extends "better_auth_sessionViaUser"
    ? NonNullable<I["better_auth_sessionViaUser"]> extends infer RelationInclude
      ? RelationInclude extends true
        ? BetterAuthSession[]
        : RelationInclude extends AnyBetterAuthSessionQueryBuilder<infer QueryRow>
          ? QueryRow[]
          : RelationInclude extends BetterAuthSessionInclude
            ? BetterAuthSessionWithIncludes<RelationInclude, false>[]
            : never
      : never
    : K extends "better_auth_accountViaUser"
      ? NonNullable<I["better_auth_accountViaUser"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? BetterAuthAccount[]
          : RelationInclude extends AnyBetterAuthAccountQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends BetterAuthAccountInclude
              ? BetterAuthAccountWithIncludes<RelationInclude, false>[]
              : never
        : never
      : never;
};

export type BetterAuthSessionIncludedRelations<
  I extends BetterAuthSessionInclude = {},
  R extends boolean = false,
> = {
  [K in keyof I]-?: K extends "user"
    ? NonNullable<I["user"]> extends infer RelationInclude
      ? RelationInclude extends true
        ? R extends true
          ? BetterAuthUser
          : BetterAuthUser | undefined
        : RelationInclude extends AnyBetterAuthUserQueryBuilder<infer QueryRow>
          ? R extends true
            ? QueryRow
            : QueryRow | undefined
          : RelationInclude extends BetterAuthUserInclude
            ? R extends true
              ? BetterAuthUserWithIncludes<RelationInclude, false>
              : BetterAuthUserWithIncludes<RelationInclude, false> | undefined
            : never
      : never
    : never;
};

export type BetterAuthAccountIncludedRelations<
  I extends BetterAuthAccountInclude = {},
  R extends boolean = false,
> = {
  [K in keyof I]-?: K extends "user"
    ? NonNullable<I["user"]> extends infer RelationInclude
      ? RelationInclude extends true
        ? R extends true
          ? BetterAuthUser
          : BetterAuthUser | undefined
        : RelationInclude extends AnyBetterAuthUserQueryBuilder<infer QueryRow>
          ? R extends true
            ? QueryRow
            : QueryRow | undefined
          : RelationInclude extends BetterAuthUserInclude
            ? R extends true
              ? BetterAuthUserWithIncludes<RelationInclude, false>
              : BetterAuthUserWithIncludes<RelationInclude, false> | undefined
            : never
      : never
    : never;
};

export interface BetterAuthUserRelations {
  better_auth_sessionViaUser: BetterAuthSession[];
  better_auth_accountViaUser: BetterAuthAccount[];
}

export interface BetterAuthSessionRelations {
  user: BetterAuthUser | undefined;
}

export interface BetterAuthAccountRelations {
  user: BetterAuthUser | undefined;
}

export type BetterAuthUserWithIncludes<
  I extends BetterAuthUserInclude = {},
  R extends boolean = false,
> = BetterAuthUser & BetterAuthUserIncludedRelations<I, R>;

export type BetterAuthSessionWithIncludes<
  I extends BetterAuthSessionInclude = {},
  R extends boolean = false,
> = BetterAuthSession & BetterAuthSessionIncludedRelations<I, R>;

export type BetterAuthAccountWithIncludes<
  I extends BetterAuthAccountInclude = {},
  R extends boolean = false,
> = BetterAuthAccount & BetterAuthAccountIncludedRelations<I, R>;

export type BetterAuthUserSelectableColumn =
  | keyof BetterAuthUser
  | PermissionIntrospectionColumn
  | "*";
export type BetterAuthUserOrderableColumn = keyof BetterAuthUser | PermissionIntrospectionColumn;

export type BetterAuthUserSelected<
  S extends BetterAuthUserSelectableColumn = keyof BetterAuthUser,
> = ("*" extends S
  ? BetterAuthUser
  : Pick<BetterAuthUser, Extract<S | "id", keyof BetterAuthUser>>) &
  Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type BetterAuthUserSelectedWithIncludes<
  I extends BetterAuthUserInclude = {},
  S extends BetterAuthUserSelectableColumn = keyof BetterAuthUser,
  R extends boolean = false,
> = BetterAuthUserSelected<S> & BetterAuthUserIncludedRelations<I, R>;

export type BetterAuthSessionSelectableColumn =
  | keyof BetterAuthSession
  | PermissionIntrospectionColumn
  | "*";
export type BetterAuthSessionOrderableColumn =
  | keyof BetterAuthSession
  | PermissionIntrospectionColumn;

export type BetterAuthSessionSelected<
  S extends BetterAuthSessionSelectableColumn = keyof BetterAuthSession,
> = ("*" extends S
  ? BetterAuthSession
  : Pick<BetterAuthSession, Extract<S | "id", keyof BetterAuthSession>>) &
  Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type BetterAuthSessionSelectedWithIncludes<
  I extends BetterAuthSessionInclude = {},
  S extends BetterAuthSessionSelectableColumn = keyof BetterAuthSession,
  R extends boolean = false,
> = BetterAuthSessionSelected<S> & BetterAuthSessionIncludedRelations<I, R>;

export type BetterAuthAccountSelectableColumn =
  | keyof BetterAuthAccount
  | PermissionIntrospectionColumn
  | "*";
export type BetterAuthAccountOrderableColumn =
  | keyof BetterAuthAccount
  | PermissionIntrospectionColumn;

export type BetterAuthAccountSelected<
  S extends BetterAuthAccountSelectableColumn = keyof BetterAuthAccount,
> = ("*" extends S
  ? BetterAuthAccount
  : Pick<BetterAuthAccount, Extract<S | "id", keyof BetterAuthAccount>>) &
  Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type BetterAuthAccountSelectedWithIncludes<
  I extends BetterAuthAccountInclude = {},
  S extends BetterAuthAccountSelectableColumn = keyof BetterAuthAccount,
  R extends boolean = false,
> = BetterAuthAccountSelected<S> & BetterAuthAccountIncludedRelations<I, R>;

export type BetterAuthVerificationSelectableColumn =
  | keyof BetterAuthVerification
  | PermissionIntrospectionColumn
  | "*";
export type BetterAuthVerificationOrderableColumn =
  | keyof BetterAuthVerification
  | PermissionIntrospectionColumn;

export type BetterAuthVerificationSelected<
  S extends BetterAuthVerificationSelectableColumn = keyof BetterAuthVerification,
> = ("*" extends S
  ? BetterAuthVerification
  : Pick<BetterAuthVerification, Extract<S | "id", keyof BetterAuthVerification>>) &
  Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type BetterAuthJwkSelectableColumn =
  | keyof BetterAuthJwk
  | PermissionIntrospectionColumn
  | "*";
export type BetterAuthJwkOrderableColumn = keyof BetterAuthJwk | PermissionIntrospectionColumn;

export type BetterAuthJwkSelected<S extends BetterAuthJwkSelectableColumn = keyof BetterAuthJwk> =
  ("*" extends S ? BetterAuthJwk : Pick<BetterAuthJwk, Extract<S | "id", keyof BetterAuthJwk>>) &
    Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export const wasmSchema: WasmSchema = {
  better_auth_user: {
    columns: [
      {
        name: "name",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "email",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "emailVerified",
        column_type: {
          type: "Boolean",
        },
        nullable: false,
      },
      {
        name: "image",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "createdAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "updatedAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "role",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "banned",
        column_type: {
          type: "Boolean",
        },
        nullable: true,
      },
      {
        name: "banReason",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "banExpires",
        column_type: {
          type: "Timestamp",
        },
        nullable: true,
      },
    ],
  },
  better_auth_session: {
    columns: [
      {
        name: "expiresAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "token",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "createdAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "updatedAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "ipAddress",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "userAgent",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "userId",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "better_auth_user",
      },
      {
        name: "impersonatedBy",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
    ],
  },
  better_auth_account: {
    columns: [
      {
        name: "accountId",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "providerId",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "userId",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "better_auth_user",
      },
      {
        name: "accessToken",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "refreshToken",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "idToken",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "accessTokenExpiresAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: true,
      },
      {
        name: "refreshTokenExpiresAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: true,
      },
      {
        name: "scope",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "password",
        column_type: {
          type: "Text",
        },
        nullable: true,
      },
      {
        name: "createdAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "updatedAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
    ],
  },
  better_auth_verification: {
    columns: [
      {
        name: "identifier",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "value",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "expiresAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "createdAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "updatedAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
    ],
  },
  better_auth_jwks: {
    columns: [
      {
        name: "publicKey",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "privateKey",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "createdAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "expiresAt",
        column_type: {
          type: "Timestamp",
        },
        nullable: true,
      },
    ],
  },
};

export class BetterAuthUserQueryBuilder<
  I extends BetterAuthUserInclude = {},
  S extends BetterAuthUserSelectableColumn = keyof BetterAuthUser,
  R extends boolean = false,
> implements QueryBuilder<BetterAuthUserSelectedWithIncludes<I, S, R>> {
  readonly _table = "better_auth_user";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: BetterAuthUserSelectedWithIncludes<I, S, R>;
  readonly _initType!: BetterAuthUserInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<BetterAuthUserInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: BetterAuthUserWhereInput): BetterAuthUserQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends BetterAuthUserSelectableColumn>(
    ...columns: [NewS, ...NewS[]]
  ): BetterAuthUserQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends BetterAuthUserInclude>(
    relations: NewI,
  ): BetterAuthUserQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): BetterAuthUserQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(
    column: BetterAuthUserOrderableColumn,
    direction: "asc" | "desc" = "asc",
  ): BetterAuthUserQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): BetterAuthUserQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): BetterAuthUserQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(
    relation: "better_auth_sessionViaUser" | "better_auth_accountViaUser",
  ): BetterAuthUserQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: BetterAuthUserWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): BetterAuthUserQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends BetterAuthUserInclude = I,
    CloneS extends BetterAuthUserSelectableColumn = S,
    CloneR extends boolean = R,
  >(): BetterAuthUserQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new BetterAuthUserQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class BetterAuthSessionQueryBuilder<
  I extends BetterAuthSessionInclude = {},
  S extends BetterAuthSessionSelectableColumn = keyof BetterAuthSession,
  R extends boolean = false,
> implements QueryBuilder<BetterAuthSessionSelectedWithIncludes<I, S, R>> {
  readonly _table = "better_auth_session";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: BetterAuthSessionSelectedWithIncludes<I, S, R>;
  readonly _initType!: BetterAuthSessionInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<BetterAuthSessionInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: BetterAuthSessionWhereInput): BetterAuthSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends BetterAuthSessionSelectableColumn>(
    ...columns: [NewS, ...NewS[]]
  ): BetterAuthSessionQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends BetterAuthSessionInclude>(
    relations: NewI,
  ): BetterAuthSessionQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): BetterAuthSessionQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(
    column: BetterAuthSessionOrderableColumn,
    direction: "asc" | "desc" = "asc",
  ): BetterAuthSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): BetterAuthSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): BetterAuthSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "user"): BetterAuthSessionQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: BetterAuthSessionWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): BetterAuthSessionQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends BetterAuthSessionInclude = I,
    CloneS extends BetterAuthSessionSelectableColumn = S,
    CloneR extends boolean = R,
  >(): BetterAuthSessionQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new BetterAuthSessionQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class BetterAuthAccountQueryBuilder<
  I extends BetterAuthAccountInclude = {},
  S extends BetterAuthAccountSelectableColumn = keyof BetterAuthAccount,
  R extends boolean = false,
> implements QueryBuilder<BetterAuthAccountSelectedWithIncludes<I, S, R>> {
  readonly _table = "better_auth_account";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: BetterAuthAccountSelectedWithIncludes<I, S, R>;
  readonly _initType!: BetterAuthAccountInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<BetterAuthAccountInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: BetterAuthAccountWhereInput): BetterAuthAccountQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends BetterAuthAccountSelectableColumn>(
    ...columns: [NewS, ...NewS[]]
  ): BetterAuthAccountQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends BetterAuthAccountInclude>(
    relations: NewI,
  ): BetterAuthAccountQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): BetterAuthAccountQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(
    column: BetterAuthAccountOrderableColumn,
    direction: "asc" | "desc" = "asc",
  ): BetterAuthAccountQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): BetterAuthAccountQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): BetterAuthAccountQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "user"): BetterAuthAccountQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: BetterAuthAccountWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): BetterAuthAccountQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends BetterAuthAccountInclude = I,
    CloneS extends BetterAuthAccountSelectableColumn = S,
    CloneR extends boolean = R,
  >(): BetterAuthAccountQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new BetterAuthAccountQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class BetterAuthVerificationQueryBuilder<
  I extends Record<string, never> = {},
  S extends BetterAuthVerificationSelectableColumn = keyof BetterAuthVerification,
  R extends boolean = false,
> implements QueryBuilder<BetterAuthVerificationSelected<S>> {
  readonly _table = "better_auth_verification";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: BetterAuthVerificationSelected<S>;
  readonly _initType!: BetterAuthVerificationInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<Record<string, never>> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: BetterAuthVerificationWhereInput): BetterAuthVerificationQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends BetterAuthVerificationSelectableColumn>(
    ...columns: [NewS, ...NewS[]]
  ): BetterAuthVerificationQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  orderBy(
    column: BetterAuthVerificationOrderableColumn,
    direction: "asc" | "desc" = "asc",
  ): BetterAuthVerificationQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): BetterAuthVerificationQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): BetterAuthVerificationQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  gather(options: {
    start: BetterAuthVerificationWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): BetterAuthVerificationQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends Record<string, never> = I,
    CloneS extends BetterAuthVerificationSelectableColumn = S,
    CloneR extends boolean = R,
  >(): BetterAuthVerificationQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new BetterAuthVerificationQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class BetterAuthJwkQueryBuilder<
  I extends Record<string, never> = {},
  S extends BetterAuthJwkSelectableColumn = keyof BetterAuthJwk,
  R extends boolean = false,
> implements QueryBuilder<BetterAuthJwkSelected<S>> {
  readonly _table = "better_auth_jwks";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: BetterAuthJwkSelected<S>;
  readonly _initType!: BetterAuthJwkInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<Record<string, never>> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: BetterAuthJwkWhereInput): BetterAuthJwkQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends BetterAuthJwkSelectableColumn>(
    ...columns: [NewS, ...NewS[]]
  ): BetterAuthJwkQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  orderBy(
    column: BetterAuthJwkOrderableColumn,
    direction: "asc" | "desc" = "asc",
  ): BetterAuthJwkQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): BetterAuthJwkQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): BetterAuthJwkQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  gather(options: {
    start: BetterAuthJwkWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): BetterAuthJwkQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends Record<string, never> = I,
    CloneS extends BetterAuthJwkSelectableColumn = S,
    CloneR extends boolean = R,
  >(): BetterAuthJwkQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new BetterAuthJwkQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export interface GeneratedApp {
  better_auth_user: BetterAuthUserQueryBuilder;
  better_auth_session: BetterAuthSessionQueryBuilder;
  better_auth_account: BetterAuthAccountQueryBuilder;
  better_auth_verification: BetterAuthVerificationQueryBuilder;
  better_auth_jwks: BetterAuthJwkQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  better_auth_user: new BetterAuthUserQueryBuilder(),
  better_auth_session: new BetterAuthSessionQueryBuilder(),
  better_auth_account: new BetterAuthAccountQueryBuilder(),
  better_auth_verification: new BetterAuthVerificationQueryBuilder(),
  better_auth_jwks: new BetterAuthJwkQueryBuilder(),
  wasmSchema,
};
