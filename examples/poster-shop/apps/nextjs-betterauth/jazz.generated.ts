// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output d3dbe380c55fea1c. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type JsonValue } from "jazz-tools";

export type CanvasInviteRole = "viewer" | "editor";
export type CanvasMemberRole = "viewer" | "editor" | "admin";
export type ShapeKind = "rect" | "ellipse" | "text" | "image";

export type Asset = {
  readonly id: string;
  canvasId: string;
  name: string;
  mimeType: string;
  byteLength: number;
  width: number;
  height: number;
  bytes: Uint8Array;
};

export type AssetInsert = {
  canvasId: string;
  name: string;
  mimeType: string;
  byteLength: number;
  width: number;
  height: number;
  bytes: Uint8Array;
};

export type BetterAuthAccount = {
  readonly id: string;
  accountId: string;
  providerId: string;
  userId: string;
  password: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthAccountInsert = {
  accountId: string;
  providerId: string;
  userId: string;
  password?: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthJwk = {
  readonly id: string;
  publicKey: string;
  privateKey: string;
  createdAt: Date;
  expiresAt: Date | null;
  alg: string | null;
  crv: string | null;
};

export type BetterAuthJwkInsert = {
  publicKey: string;
  privateKey: string;
  createdAt: Date;
  expiresAt?: Date | null;
  alg?: string | null;
  crv?: string | null;
};

export type BetterAuthSession = {
  readonly id: string;
  expiresAt: Date;
  token: string;
  createdAt: Date;
  updatedAt: Date;
  ipAddress: string | null;
  userAgent: string | null;
  userId: string;
};

export type BetterAuthSessionInsert = {
  expiresAt: Date;
  token: string;
  createdAt: Date;
  updatedAt: Date;
  ipAddress?: string | null;
  userAgent?: string | null;
  userId: string;
};

export type BetterAuthUser = {
  readonly id: string;
  name: string;
  email: string;
  emailVerified: boolean;
  image: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthUserInsert = {
  name: string;
  email: string;
  emailVerified: boolean;
  image?: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthVerification = {
  readonly id: string;
  identifier: string;
  value: string;
  expiresAt: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthVerificationInsert = {
  identifier: string;
  value: string;
  expiresAt: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type CanvasInvite = {
  readonly id: string;
  canvasId: string;
  token: string;
  role: CanvasInviteRole;
  singleUse: boolean;
};

export type CanvasInviteInsert = {
  canvasId: string;
  token: string;
  role: CanvasInviteRole;
  singleUse?: boolean;
};

export type CanvasMember = {
  readonly id: string;
  canvasId: string;
  memberAuthor: string;
  role: CanvasMemberRole;
};

export type CanvasMemberInsert = {
  canvasId: string;
  memberAuthor: string;
  role: CanvasMemberRole;
};

export type Canvas = {
  readonly id: string;
  title: string;
  width: number;
  height: number;
};

export type CanvasInsert = {
  title: string;
  width: number;
  height: number;
};

export type Checkpoint = {
  readonly id: string;
  canvasId: string;
  label: string;
  snapshot: JsonValue;
};

export type CheckpointInsert = {
  canvasId: string;
  label: string;
  snapshot: JsonValue;
};

export type Cursor = {
  readonly id: string;
  canvasId: string;
  author: string;
  x: number;
  y: number;
  color: string;
  name: string;
  active: boolean;
};

export type CursorInsert = {
  canvasId: string;
  author: string;
  x: number;
  y: number;
  color: string;
  name?: string;
  active?: boolean;
};

export type Layer = {
  readonly id: string;
  canvasId: string;
  name: string;
  zIndex: number;
  visible: boolean;
  locked: boolean;
};

export type LayerInsert = {
  canvasId: string;
  name: string;
  zIndex: number;
  visible: boolean;
  locked?: boolean;
};

export type Shape = {
  readonly id: string;
  canvasId: string;
  layerId: string;
  assetId: string | null;
  kind: ShapeKind;
  x: number;
  y: number;
  width: number;
  height: number;
  rotation: number;
  zIndex: number;
  text: string | null;
  fill: string;
};

export type ShapeInsert = {
  canvasId: string;
  layerId: string;
  assetId?: string | null;
  kind: ShapeKind;
  x: number;
  y: number;
  width: number;
  height: number;
  rotation: number;
  zIndex: number;
  text?: string | null;
  fill: string;
};

interface Types extends AppTypes {
  assets: {
    row: Asset;
    insert: AssetInsert;
    columns: {
      canvasId: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      mimeType: { kind: "string"; nullable: false; value: string };
      byteLength: { kind: "int"; nullable: false; value: number };
      width: { kind: "int"; nullable: false; value: number };
      height: { kind: "int"; nullable: false; value: number };
      bytes: { kind: "bytes"; nullable: false; value: Uint8Array };
    };
    relations: {
      canvas: { one: "canvases"; nullable: false };
      shapesViaAsset: { many: "shapes" };
    };
  };
  better_auth_account: {
    row: BetterAuthAccount;
    insert: BetterAuthAccountInsert;
    columns: {
      accountId: { kind: "string"; nullable: false; value: string };
      providerId: { kind: "string"; nullable: false; value: string };
      userId: { kind: "uuid"; nullable: false; value: string };
      password: { kind: "string"; nullable: true; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      user: { one: "better_auth_user"; nullable: false };
    };
  };
  better_auth_jwks: {
    row: BetterAuthJwk;
    insert: BetterAuthJwkInsert;
    columns: {
      publicKey: { kind: "string"; nullable: false; value: string };
      privateKey: { kind: "string"; nullable: false; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      expiresAt: { kind: "timestamp"; nullable: true; value: Date };
      alg: { kind: "string"; nullable: true; value: string };
      crv: { kind: "string"; nullable: true; value: string };
    };
    relations: {};
  };
  better_auth_session: {
    row: BetterAuthSession;
    insert: BetterAuthSessionInsert;
    columns: {
      expiresAt: { kind: "timestamp"; nullable: false; value: Date };
      token: { kind: "string"; nullable: false; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
      ipAddress: { kind: "string"; nullable: true; value: string };
      userAgent: { kind: "string"; nullable: true; value: string };
      userId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      user: { one: "better_auth_user"; nullable: false };
    };
  };
  better_auth_user: {
    row: BetterAuthUser;
    insert: BetterAuthUserInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      email: { kind: "string"; nullable: false; value: string };
      emailVerified: { kind: "boolean"; nullable: false; value: boolean };
      image: { kind: "string"; nullable: true; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      better_auth_accountViaUser: { many: "better_auth_account" };
      better_auth_sessionViaUser: { many: "better_auth_session" };
    };
  };
  better_auth_verification: {
    row: BetterAuthVerification;
    insert: BetterAuthVerificationInsert;
    columns: {
      identifier: { kind: "string"; nullable: false; value: string };
      value: { kind: "string"; nullable: false; value: string };
      expiresAt: { kind: "timestamp"; nullable: false; value: Date };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {};
  };
  canvasInvites: {
    row: CanvasInvite;
    insert: CanvasInviteInsert;
    columns: {
      canvasId: { kind: "uuid"; nullable: false; value: string };
      token: { kind: "string"; nullable: false; value: string };
      role: { kind: "enum"; nullable: false; value: CanvasInviteRole };
      singleUse: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {
      canvas: { one: "canvases"; nullable: false };
    };
  };
  canvasMembers: {
    row: CanvasMember;
    insert: CanvasMemberInsert;
    columns: {
      canvasId: { kind: "uuid"; nullable: false; value: string };
      memberAuthor: { kind: "uuid"; nullable: false; value: string };
      role: { kind: "enum"; nullable: false; value: CanvasMemberRole };
    };
    relations: {
      canvas: { one: "canvases"; nullable: false };
    };
  };
  canvases: {
    row: Canvas;
    insert: CanvasInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      width: { kind: "int"; nullable: false; value: number };
      height: { kind: "int"; nullable: false; value: number };
    };
    relations: {
      assetsViaCanvas: { many: "assets" };
      canvasInvitesViaCanvas: { many: "canvasInvites" };
      canvasMembersViaCanvas: { many: "canvasMembers" };
      checkpointsViaCanvas: { many: "checkpoints" };
      cursorsViaCanvas: { many: "cursors" };
      layersViaCanvas: { many: "layers" };
      shapesViaCanvas: { many: "shapes" };
    };
  };
  checkpoints: {
    row: Checkpoint;
    insert: CheckpointInsert;
    columns: {
      canvasId: { kind: "uuid"; nullable: false; value: string };
      label: { kind: "string"; nullable: false; value: string };
      snapshot: { kind: "json"; nullable: false; value: JsonValue };
    };
    relations: {
      canvas: { one: "canvases"; nullable: false };
    };
  };
  cursors: {
    row: Cursor;
    insert: CursorInsert;
    columns: {
      canvasId: { kind: "uuid"; nullable: false; value: string };
      author: { kind: "uuid"; nullable: false; value: string };
      x: { kind: "float"; nullable: false; value: number };
      y: { kind: "float"; nullable: false; value: number };
      color: { kind: "string"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      active: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {
      canvas: { one: "canvases"; nullable: false };
    };
  };
  layers: {
    row: Layer;
    insert: LayerInsert;
    columns: {
      canvasId: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      zIndex: { kind: "int"; nullable: false; value: number };
      visible: { kind: "boolean"; nullable: false; value: boolean };
      locked: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {
      canvas: { one: "canvases"; nullable: false };
      shapesViaLayer: { many: "shapes" };
    };
  };
  shapes: {
    row: Shape;
    insert: ShapeInsert;
    columns: {
      canvasId: { kind: "uuid"; nullable: false; value: string };
      layerId: { kind: "uuid"; nullable: false; value: string };
      assetId: { kind: "uuid"; nullable: true; value: string };
      kind: { kind: "enum"; nullable: false; value: ShapeKind };
      x: { kind: "float"; nullable: false; value: number };
      y: { kind: "float"; nullable: false; value: number };
      width: { kind: "float"; nullable: false; value: number };
      height: { kind: "float"; nullable: false; value: number };
      rotation: { kind: "float"; nullable: false; value: number };
      zIndex: { kind: "int"; nullable: false; value: number };
      text: { kind: "string"; nullable: true; value: string };
      fill: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      asset: { one: "assets"; nullable: true };
      canvas: { one: "canvases"; nullable: false };
      layer: { one: "layers"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:04fb9c6d2a7280135468f154805b58e140fdfc96f971d58be1c8bf6949b8b2cd",
  "schema": {
    "current": "c2c80e4c3ecf667af8dd57d937fd72d5b6198e85d36753987ba3047838fb9fd9",
    "schemas": [
      {
        "hash": "55a9ac8b000f23198f161d0e64ea96e10fc608de6c8edabad0925d39c472badc",
        "schema": {
          "tables": {
            "assets": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "mimeType", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "byteLength", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "width", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "height", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "bytes", "nullable": false },
              ],
              "indexed_columns": ["canvasId", "name"],
            },
            "better_auth_account": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "issuer", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "accountId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "providerId", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
                { "column_type": { "type": "Text" }, "name": "password", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_jwks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "publicKey", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "privateKey", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "alg", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "crv", "nullable": true },
              ],
            },
            "better_auth_session": {
              "columns": [
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "ipAddress", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "userAgent", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
              ],
            },
            "better_auth_user": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "emailVerified", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "image", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_verification": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "identifier", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "value", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "canvasInvites": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["viewer", "editor"] },
                  "name": "role",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Boolean" },
                  "default": { "type": "Boolean", "value": false },
                  "name": "singleUse",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["canvasId", "token"],
            },
            "canvasMembers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                { "column_type": { "type": "Uuid" }, "name": "memberAuthor", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["viewer", "editor", "admin"] },
                  "name": "role",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["canvasId", "memberAuthor"],
            },
            "canvases": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "width", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "height", "nullable": false },
              ],
            },
            "checkpoints": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                { "column_type": { "type": "Text" }, "name": "label", "nullable": false },
                { "column_type": { "type": "Json" }, "name": "snapshot", "nullable": false },
              ],
              "indexed_columns": ["canvasId"],
            },
            "cursors": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                { "column_type": { "type": "Uuid" }, "name": "author", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "x", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "y", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "color", "nullable": false },
                {
                  "column_type": { "type": "Text" },
                  "default": { "type": "Text", "value": "" },
                  "name": "name",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Boolean" },
                  "default": { "type": "Boolean", "value": true },
                  "name": "active",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["canvasId", "author"],
            },
            "layers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "zIndex", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "visible", "nullable": false },
                {
                  "column_type": { "type": "Boolean" },
                  "default": { "type": "Boolean", "value": false },
                  "name": "locked",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["canvasId", "zIndex"],
            },
            "shapes": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "layerId",
                  "nullable": false,
                  "references": "layers",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "assetId",
                  "nullable": true,
                  "references": "assets",
                },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["rect", "ellipse", "text", "image"],
                  },
                  "name": "kind",
                  "nullable": false,
                },
                { "column_type": { "type": "Double" }, "name": "x", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "y", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "width", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "height", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "rotation", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "zIndex", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "text", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "fill", "nullable": false },
              ],
              "indexed_columns": ["canvasId", "layerId", "zIndex"],
            },
          },
        },
      },
      {
        "hash": "c2c80e4c3ecf667af8dd57d937fd72d5b6198e85d36753987ba3047838fb9fd9",
        "schema": {
          "tables": {
            "assets": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "mimeType", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "byteLength", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "width", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "height", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "bytes", "nullable": false },
              ],
              "indexed_columns": ["canvasId", "name"],
            },
            "better_auth_account": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "accountId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "providerId", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
                { "column_type": { "type": "Text" }, "name": "password", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_jwks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "publicKey", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "privateKey", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "alg", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "crv", "nullable": true },
              ],
            },
            "better_auth_session": {
              "columns": [
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "ipAddress", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "userAgent", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
              ],
            },
            "better_auth_user": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "emailVerified", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "image", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_verification": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "identifier", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "value", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "canvasInvites": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["viewer", "editor"] },
                  "name": "role",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Boolean" },
                  "default": { "type": "Boolean", "value": false },
                  "name": "singleUse",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["canvasId", "token"],
            },
            "canvasMembers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                { "column_type": { "type": "Uuid" }, "name": "memberAuthor", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["viewer", "editor", "admin"] },
                  "name": "role",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["canvasId", "memberAuthor"],
            },
            "canvases": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "width", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "height", "nullable": false },
              ],
            },
            "checkpoints": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                { "column_type": { "type": "Text" }, "name": "label", "nullable": false },
                { "column_type": { "type": "Json" }, "name": "snapshot", "nullable": false },
              ],
              "indexed_columns": ["canvasId"],
            },
            "cursors": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                { "column_type": { "type": "Uuid" }, "name": "author", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "x", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "y", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "color", "nullable": false },
                {
                  "column_type": { "type": "Text" },
                  "default": { "type": "Text", "value": "" },
                  "name": "name",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Boolean" },
                  "default": { "type": "Boolean", "value": true },
                  "name": "active",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["canvasId", "author"],
            },
            "layers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "zIndex", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "visible", "nullable": false },
                {
                  "column_type": { "type": "Boolean" },
                  "default": { "type": "Boolean", "value": false },
                  "name": "locked",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["canvasId", "zIndex"],
            },
            "shapes": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "layerId",
                  "nullable": false,
                  "references": "layers",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "assetId",
                  "nullable": true,
                  "references": "assets",
                },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["rect", "ellipse", "text", "image"],
                  },
                  "name": "kind",
                  "nullable": false,
                },
                { "column_type": { "type": "Double" }, "name": "x", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "y", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "width", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "height", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "rotation", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "zIndex", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "text", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "fill", "nullable": false },
              ],
              "indexed_columns": ["canvasId", "layerId", "zIndex"],
            },
          },
        },
      },
    ],
    "migrations": [
      {
        "forward": [
          {
            "added": false,
            "operations": [
              {
                "column": "issuer",
                "column_type": { "type": "Text" },
                "type": "drop",
                "value": { "type": "Text", "value": "" },
              },
            ],
            "removed": false,
            "renamedFrom": null,
            "table": "better_auth_account",
          },
        ],
        "fromHash": "55a9ac8b000f23198f161d0e64ea96e10fc608de6c8edabad0925d39c472badc",
        "toHash": "c2c80e4c3ecf667af8dd57d937fd72d5b6198e85d36753987ba3047838fb9fd9",
      },
    ],
  },
  "relations": {
    "assets": {
      "canvas": { "one": "canvases", "column": "canvasId" },
      "shapesViaAsset": { "many": "shapes", "relation": "asset" },
    },
    "better_auth_account": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_jwks": {},
    "better_auth_session": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_user": {
      "better_auth_accountViaUser": { "many": "better_auth_account", "relation": "user" },
      "better_auth_sessionViaUser": { "many": "better_auth_session", "relation": "user" },
    },
    "better_auth_verification": {},
    "canvasInvites": { "canvas": { "one": "canvases", "column": "canvasId" } },
    "canvasMembers": { "canvas": { "one": "canvases", "column": "canvasId" } },
    "canvases": {
      "assetsViaCanvas": { "many": "assets", "relation": "canvas" },
      "canvasInvitesViaCanvas": { "many": "canvasInvites", "relation": "canvas" },
      "canvasMembersViaCanvas": { "many": "canvasMembers", "relation": "canvas" },
      "checkpointsViaCanvas": { "many": "checkpoints", "relation": "canvas" },
      "cursorsViaCanvas": { "many": "cursors", "relation": "canvas" },
      "layersViaCanvas": { "many": "layers", "relation": "canvas" },
      "shapesViaCanvas": { "many": "shapes", "relation": "canvas" },
    },
    "checkpoints": { "canvas": { "one": "canvases", "column": "canvasId" } },
    "cursors": { "canvas": { "one": "canvases", "column": "canvasId" } },
    "layers": {
      "canvas": { "one": "canvases", "column": "canvasId" },
      "shapesViaLayer": { "many": "shapes", "relation": "layer" },
    },
    "shapes": {
      "asset": { "one": "assets", "column": "assetId" },
      "canvas": { "one": "canvases", "column": "canvasId" },
      "layer": { "one": "layers", "column": "layerId" },
    },
  },
});
