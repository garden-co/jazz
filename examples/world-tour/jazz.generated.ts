// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output c8b6591a87c2caba. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type StopStatus = "confirmed" | "tentative" | "cancelled";

export type BandInvite = {
  readonly id: string;
  bandId: string;
  code: string;
};

export type BandInviteInsert = {
  bandId: string;
  code: string;
};

export type Band = {
  readonly id: string;
  name: string;
  ownerId: string;
};

export type BandInsert = {
  name: string;
  ownerId: string;
};

export type Member = {
  readonly id: string;
  bandId: string;
  userId: string;
  name: string;
  inviteCode: string | null;
};

export type MemberInsert = {
  bandId: string;
  userId: string;
  name: string;
  inviteCode?: string | null;
};

export type StopNote = {
  readonly id: string;
  stopId: string;
  bandId: string;
  body: string;
};

export type StopNoteInsert = {
  stopId: string;
  bandId: string;
  body: string;
};

export type Stop = {
  readonly id: string;
  bandId: string;
  venueId: string;
  date: Date;
  status: StopStatus;
  publicDescription: string;
};

export type StopInsert = {
  bandId: string;
  venueId: string;
  date: Date;
  status: StopStatus;
  publicDescription: string;
};

export type Venue = {
  readonly id: string;
  name: string;
  city: string;
  country: string;
  lat: number;
  lng: number;
  capacity: number | null;
  ownerId: string;
  bandId: string | null;
};

export type VenueInsert = {
  name: string;
  city: string;
  country: string;
  lat: number;
  lng: number;
  capacity?: number | null;
  ownerId: string;
  bandId?: string | null;
};

interface Types extends AppTypes {
  bandInvites: {
    row: BandInvite;
    insert: BandInviteInsert;
    columns: {
      bandId: { kind: "uuid"; nullable: false; value: string };
      code: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      band: { one: "bands"; nullable: false };
    };
  };
  bands: {
    row: Band;
    insert: BandInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      ownerId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      invitesViaBand: { many: "bandInvites" };
      membersViaBand: { many: "members" };
      stopsViaBand: { many: "stops" };
    };
  };
  members: {
    row: Member;
    insert: MemberInsert;
    columns: {
      bandId: { kind: "uuid"; nullable: false; value: string };
      userId: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      inviteCode: { kind: "string"; nullable: true; value: string };
    };
    relations: {
      band: { one: "bands"; nullable: false };
    };
  };
  stopNotes: {
    row: StopNote;
    insert: StopNoteInsert;
    columns: {
      stopId: { kind: "uuid"; nullable: false; value: string };
      bandId: { kind: "uuid"; nullable: false; value: string };
      body: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      band: { one: "bands"; nullable: false };
      stop: { one: "stops"; nullable: false };
    };
  };
  stops: {
    row: Stop;
    insert: StopInsert;
    columns: {
      bandId: { kind: "uuid"; nullable: false; value: string };
      venueId: { kind: "uuid"; nullable: false; value: string };
      date: { kind: "timestamp"; nullable: false; value: Date };
      status: { kind: "enum"; nullable: false; value: StopStatus };
      publicDescription: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      band: { one: "bands"; nullable: false };
      notesViaStop: { many: "stopNotes" };
      venue: { one: "venues"; nullable: false };
    };
  };
  venues: {
    row: Venue;
    insert: VenueInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      city: { kind: "string"; nullable: false; value: string };
      country: { kind: "string"; nullable: false; value: string };
      lat: { kind: "float"; nullable: false; value: number };
      lng: { kind: "float"; nullable: false; value: number };
      capacity: { kind: "int"; nullable: true; value: number };
      ownerId: { kind: "uuid"; nullable: false; value: string };
      bandId: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      band: { one: "bands"; nullable: true };
      stopsViaVenue: { many: "stops" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:facd0cf15e004443c0e966e82336fa37461783495c50c9375631705c4af96c5e",
  "schema": {
    "current": "fe7f83ec735520f17a141fce778da257d48bf1a8563906de7169c3cff39ab174",
    "schemas": [
      {
        "hash": "fe7f83ec735520f17a141fce778da257d48bf1a8563906de7169c3cff39ab174",
        "schema": {
          "tables": {
            "bandInvites": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "bandId",
                  "nullable": false,
                  "references": "bands",
                },
                { "column_type": { "type": "Text" }, "name": "code", "nullable": false },
              ],
            },
            "bands": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "ownerId", "nullable": false },
              ],
            },
            "members": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "bandId",
                  "nullable": false,
                  "references": "bands",
                },
                { "column_type": { "type": "Uuid" }, "name": "userId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "inviteCode", "nullable": true },
              ],
            },
            "stopNotes": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "stopId",
                  "nullable": false,
                  "references": "stops",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "bandId",
                  "nullable": false,
                  "references": "bands",
                },
                {
                  "column_type": { "type": "Text" },
                  "merge_strategy": "Markdown",
                  "name": "body",
                  "nullable": false,
                },
              ],
            },
            "stops": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "bandId",
                  "nullable": false,
                  "references": "bands",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "venueId",
                  "nullable": false,
                  "references": "venues",
                },
                { "column_type": { "type": "Timestamp" }, "name": "date", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["confirmed", "tentative", "cancelled"],
                  },
                  "name": "status",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Text" },
                  "merge_strategy": "Markdown",
                  "name": "publicDescription",
                  "nullable": false,
                },
              ],
            },
            "venues": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "city", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "country", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "lat", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "lng", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "capacity", "nullable": true },
                { "column_type": { "type": "Uuid" }, "name": "ownerId", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "bandId",
                  "nullable": true,
                  "references": "bands",
                },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "bandInvites": { "band": { "one": "bands", "column": "bandId" } },
    "bands": {
      "invitesViaBand": { "many": "bandInvites", "relation": "band" },
      "membersViaBand": { "many": "members", "relation": "band" },
      "stopsViaBand": { "many": "stops", "relation": "band" },
    },
    "members": { "band": { "one": "bands", "column": "bandId" } },
    "stopNotes": {
      "band": { "one": "bands", "column": "bandId" },
      "stop": { "one": "stops", "column": "stopId" },
    },
    "stops": {
      "band": { "one": "bands", "column": "bandId" },
      "notesViaStop": { "many": "stopNotes", "relation": "stop" },
      "venue": { "one": "venues", "column": "venueId" },
    },
    "venues": {
      "band": { "one": "bands", "column": "bandId" },
      "stopsViaVenue": { "many": "stops", "relation": "venue" },
    },
  },
});
