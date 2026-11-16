// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type SessionMemberRole = "owner" | "editor" | "viewer";
export type TrackInstrument =
  | "kick"
  | "snare"
  | "clap"
  | "closed_hat"
  | "open_hat"
  | "tom"
  | "bass"
  | "lead";

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

export type Pattern = {
  readonly id: string;
  session_id: string;
  position: number;
  name: string;
  length: number;
};

export type PatternInsert = {
  session_id: string;
  position: number;
  name: string;
  length: number;
};

export type Presence = {
  readonly id: string;
  session_id: string;
  profile_id: string;
  cursor_step: number;
  heartbeat_at: Date;
};

export type PresenceInsert = {
  session_id: string;
  profile_id: string;
  cursor_step: number;
  heartbeat_at: Date;
};

export type Profile = {
  readonly id: string;
  author: string;
  displayName: string;
};

export type ProfileInsert = {
  author: string;
  displayName: string;
};

export type SessionMember = {
  readonly id: string;
  session_id: string;
  member_author: string;
  role: SessionMemberRole;
};

export type SessionMemberInsert = {
  session_id: string;
  member_author: string;
  role: SessionMemberRole;
};

export type Session = {
  readonly id: string;
  title: string;
  tempo_bpm: number;
};

export type SessionInsert = {
  title: string;
  tempo_bpm: number;
};

export type Step = {
  readonly id: string;
  session_id: string;
  track_id: string;
  pattern_id: string;
  position: number;
  enabled: boolean;
  velocity: number;
  probability: number;
};

export type StepInsert = {
  session_id: string;
  track_id: string;
  pattern_id: string;
  position: number;
  enabled: boolean;
  velocity: number;
  probability: number;
};

export type Track = {
  readonly id: string;
  session_id: string;
  position: number;
  name: string;
  color: string;
  instrument: TrackInstrument;
  volume: number;
  muted: boolean;
  solo: boolean;
};

export type TrackInsert = {
  session_id: string;
  position: number;
  name: string;
  color: string;
  instrument?: TrackInstrument;
  volume?: number;
  muted?: boolean;
  solo?: boolean;
};

export type TransportObservation = {
  readonly id: string;
  session_id: string;
  playing: boolean;
  bar: number;
  observed_at: Date;
  tempo_bpm: number;
  pattern_id: string | null;
};

export type TransportObservationInsert = {
  session_id: string;
  playing: boolean;
  bar: number;
  observed_at: Date;
  tempo_bpm?: number;
  pattern_id?: string | null;
};

interface Types extends AppTypes {
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
  patterns: {
    row: Pattern;
    insert: PatternInsert;
    columns: {
      session_id: { kind: "uuid"; nullable: false; value: string };
      position: { kind: "int"; nullable: false; value: number };
      name: { kind: "string"; nullable: false; value: string };
      length: { kind: "int"; nullable: false; value: number };
    };
    relations: {
      session: { one: "sessions"; nullable: false };
      stepsViaPattern: { many: "steps" };
    };
  };
  presence: {
    row: Presence;
    insert: PresenceInsert;
    columns: {
      session_id: { kind: "uuid"; nullable: false; value: string };
      profile_id: { kind: "uuid"; nullable: false; value: string };
      cursor_step: { kind: "int"; nullable: false; value: number };
      heartbeat_at: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      profile: { one: "profiles"; nullable: false };
      session: { one: "sessions"; nullable: false };
    };
  };
  profiles: {
    row: Profile;
    insert: ProfileInsert;
    columns: {
      author: { kind: "uuid"; nullable: false; value: string };
      displayName: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      presenceViaProfile: { many: "presence" };
    };
  };
  session_members: {
    row: SessionMember;
    insert: SessionMemberInsert;
    columns: {
      session_id: { kind: "uuid"; nullable: false; value: string };
      member_author: { kind: "uuid"; nullable: false; value: string };
      role: { kind: "enum"; nullable: false; value: SessionMemberRole };
    };
    relations: {
      session: { one: "sessions"; nullable: false };
    };
  };
  sessions: {
    row: Session;
    insert: SessionInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      tempo_bpm: { kind: "int"; nullable: false; value: number };
    };
    relations: {
      patternsViaSession: { many: "patterns" };
      presenceViaSession: { many: "presence" };
      session_membersViaSession: { many: "session_members" };
      stepsViaSession: { many: "steps" };
      tracksViaSession: { many: "tracks" };
      transport_observationsViaSession: { many: "transport_observations" };
    };
  };
  steps: {
    row: Step;
    insert: StepInsert;
    columns: {
      session_id: { kind: "uuid"; nullable: false; value: string };
      track_id: { kind: "uuid"; nullable: false; value: string };
      pattern_id: { kind: "uuid"; nullable: false; value: string };
      position: { kind: "int"; nullable: false; value: number };
      enabled: { kind: "boolean"; nullable: false; value: boolean };
      velocity: { kind: "int"; nullable: false; value: number };
      probability: { kind: "int"; nullable: false; value: number };
    };
    relations: {
      pattern: { one: "patterns"; nullable: false };
      session: { one: "sessions"; nullable: false };
      track: { one: "tracks"; nullable: false };
    };
  };
  tracks: {
    row: Track;
    insert: TrackInsert;
    columns: {
      session_id: { kind: "uuid"; nullable: false; value: string };
      position: { kind: "int"; nullable: false; value: number };
      name: { kind: "string"; nullable: false; value: string };
      color: { kind: "string"; nullable: false; value: string };
      instrument: { kind: "enum"; nullable: false; value: TrackInstrument };
      volume: { kind: "int"; nullable: false; value: number };
      muted: { kind: "boolean"; nullable: false; value: boolean };
      solo: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {
      session: { one: "sessions"; nullable: false };
      stepsViaTrack: { many: "steps" };
    };
  };
  transport_observations: {
    row: TransportObservation;
    insert: TransportObservationInsert;
    columns: {
      session_id: { kind: "uuid"; nullable: false; value: string };
      playing: { kind: "boolean"; nullable: false; value: boolean };
      bar: { kind: "int"; nullable: false; value: number };
      observed_at: { kind: "timestamp"; nullable: false; value: Date };
      tempo_bpm: { kind: "int"; nullable: false; value: number };
      pattern_id: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      pattern: { one: "patterns"; nullable: true };
      session: { one: "sessions"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:3c2d14a4f55cc075bcfeab2a1e91fe419f8d1268b43a1e4b823928e8d4a97d05",
  "schema": {
    "current": "c1f2524cc44f9ca8d099559c30f3b51e230d846119cfe3e92ced05962a7fa6b3",
    "schemas": [
      {
        "hash": "a0d57d731cd5c19c2810769738e14ba99c2b0cd2665e4b11e44d3e73d4502b67",
        "schema": {
          "tables": {
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
            "patterns": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "session_id",
                  "nullable": false,
                  "references": "sessions",
                },
                { "column_type": { "type": "Integer" }, "name": "position", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "length", "nullable": false },
              ],
              "indexed_columns": ["session_id", "position"],
            },
            "presence": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "session_id",
                  "nullable": false,
                  "references": "sessions",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "profile_id",
                  "nullable": false,
                  "references": "profiles",
                },
                { "column_type": { "type": "Integer" }, "name": "cursor_step", "nullable": false },
                {
                  "column_type": { "type": "Timestamp" },
                  "name": "heartbeat_at",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["session_id", "heartbeat_at"],
            },
            "profiles": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "author", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "displayName", "nullable": false },
              ],
            },
            "session_members": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "session_id",
                  "nullable": false,
                  "references": "sessions",
                },
                { "column_type": { "type": "Uuid" }, "name": "member_author", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["owner", "editor", "viewer"] },
                  "name": "role",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["session_id", "member_author"],
            },
            "sessions": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "tempo_bpm", "nullable": false },
              ],
            },
            "steps": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "session_id",
                  "nullable": false,
                  "references": "sessions",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "track_id",
                  "nullable": false,
                  "references": "tracks",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pattern_id",
                  "nullable": false,
                  "references": "patterns",
                },
                { "column_type": { "type": "Integer" }, "name": "position", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "enabled", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "velocity", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "probability", "nullable": false },
              ],
              "indexed_columns": ["track_id", "pattern_id", "position"],
            },
            "tracks": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "session_id",
                  "nullable": false,
                  "references": "sessions",
                },
                { "column_type": { "type": "Integer" }, "name": "position", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "color", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": [
                      "kick",
                      "snare",
                      "clap",
                      "closed_hat",
                      "open_hat",
                      "tom",
                      "bass",
                      "lead",
                    ],
                  },
                  "default": { "type": "Text", "value": "kick" },
                  "name": "instrument",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Integer" },
                  "default": { "type": "Integer", "value": 80 },
                  "name": "volume",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Boolean" },
                  "default": { "type": "Boolean", "value": false },
                  "name": "muted",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Boolean" },
                  "default": { "type": "Boolean", "value": false },
                  "name": "solo",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["session_id", "position"],
            },
            "transport_observations": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "session_id",
                  "nullable": false,
                  "references": "sessions",
                },
                { "column_type": { "type": "Boolean" }, "name": "playing", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "bar", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "observed_at", "nullable": false },
                {
                  "column_type": { "type": "Integer" },
                  "default": { "type": "Integer", "value": 120 },
                  "name": "tempo_bpm",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pattern_id",
                  "nullable": true,
                  "references": "patterns",
                },
              ],
              "indexed_columns": ["session_id", "observed_at"],
            },
          },
        },
      },
      {
        "hash": "c1f2524cc44f9ca8d099559c30f3b51e230d846119cfe3e92ced05962a7fa6b3",
        "schema": {
          "tables": {
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
            "patterns": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "session_id",
                  "nullable": false,
                  "references": "sessions",
                },
                { "column_type": { "type": "Integer" }, "name": "position", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "length", "nullable": false },
              ],
              "indexed_columns": ["session_id", "position"],
            },
            "presence": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "session_id",
                  "nullable": false,
                  "references": "sessions",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "profile_id",
                  "nullable": false,
                  "references": "profiles",
                },
                { "column_type": { "type": "Integer" }, "name": "cursor_step", "nullable": false },
                {
                  "column_type": { "type": "Timestamp" },
                  "name": "heartbeat_at",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["session_id", "heartbeat_at"],
            },
            "profiles": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "author", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "displayName", "nullable": false },
              ],
            },
            "session_members": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "session_id",
                  "nullable": false,
                  "references": "sessions",
                },
                { "column_type": { "type": "Uuid" }, "name": "member_author", "nullable": false },
                {
                  "column_type": { "type": "Enum", "variants": ["owner", "editor", "viewer"] },
                  "name": "role",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["session_id", "member_author"],
            },
            "sessions": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "tempo_bpm", "nullable": false },
              ],
            },
            "steps": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "session_id",
                  "nullable": false,
                  "references": "sessions",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "track_id",
                  "nullable": false,
                  "references": "tracks",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pattern_id",
                  "nullable": false,
                  "references": "patterns",
                },
                { "column_type": { "type": "Integer" }, "name": "position", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "enabled", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "velocity", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "probability", "nullable": false },
              ],
              "indexed_columns": ["track_id", "pattern_id", "position"],
            },
            "tracks": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "session_id",
                  "nullable": false,
                  "references": "sessions",
                },
                { "column_type": { "type": "Integer" }, "name": "position", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "color", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": [
                      "kick",
                      "snare",
                      "clap",
                      "closed_hat",
                      "open_hat",
                      "tom",
                      "bass",
                      "lead",
                    ],
                  },
                  "default": { "type": "Text", "value": "kick" },
                  "name": "instrument",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Integer" },
                  "default": { "type": "Integer", "value": 80 },
                  "name": "volume",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Boolean" },
                  "default": { "type": "Boolean", "value": false },
                  "name": "muted",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Boolean" },
                  "default": { "type": "Boolean", "value": false },
                  "name": "solo",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["session_id", "position"],
            },
            "transport_observations": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "session_id",
                  "nullable": false,
                  "references": "sessions",
                },
                { "column_type": { "type": "Boolean" }, "name": "playing", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "bar", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "observed_at", "nullable": false },
                {
                  "column_type": { "type": "Integer" },
                  "default": { "type": "Integer", "value": 120 },
                  "name": "tempo_bpm",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "pattern_id",
                  "nullable": true,
                  "references": "patterns",
                },
              ],
              "indexed_columns": ["session_id", "observed_at"],
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
        "fromHash": "a0d57d731cd5c19c2810769738e14ba99c2b0cd2665e4b11e44d3e73d4502b67",
        "toHash": "c1f2524cc44f9ca8d099559c30f3b51e230d846119cfe3e92ced05962a7fa6b3",
      },
    ],
  },
  "relations": {
    "better_auth_account": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_jwks": {},
    "better_auth_session": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_user": {
      "better_auth_accountViaUser": { "many": "better_auth_account", "relation": "user" },
      "better_auth_sessionViaUser": { "many": "better_auth_session", "relation": "user" },
    },
    "better_auth_verification": {},
    "patterns": {
      "session": { "one": "sessions", "column": "session_id" },
      "stepsViaPattern": { "many": "steps", "relation": "pattern" },
    },
    "presence": {
      "profile": { "one": "profiles", "column": "profile_id" },
      "session": { "one": "sessions", "column": "session_id" },
    },
    "profiles": { "presenceViaProfile": { "many": "presence", "relation": "profile" } },
    "session_members": { "session": { "one": "sessions", "column": "session_id" } },
    "sessions": {
      "patternsViaSession": { "many": "patterns", "relation": "session" },
      "presenceViaSession": { "many": "presence", "relation": "session" },
      "session_membersViaSession": { "many": "session_members", "relation": "session" },
      "stepsViaSession": { "many": "steps", "relation": "session" },
      "tracksViaSession": { "many": "tracks", "relation": "session" },
      "transport_observationsViaSession": {
        "many": "transport_observations",
        "relation": "session",
      },
    },
    "steps": {
      "pattern": { "one": "patterns", "column": "pattern_id" },
      "session": { "one": "sessions", "column": "session_id" },
      "track": { "one": "tracks", "column": "track_id" },
    },
    "tracks": {
      "session": { "one": "sessions", "column": "session_id" },
      "stepsViaTrack": { "many": "steps", "relation": "track" },
    },
    "transport_observations": {
      "pattern": { "one": "patterns", "column": "pattern_id" },
      "session": { "one": "sessions", "column": "session_id" },
    },
  },
});
