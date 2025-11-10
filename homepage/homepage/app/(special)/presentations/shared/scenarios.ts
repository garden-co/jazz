import { SessionEntry } from "./coValueDiagrams/helpers";

export type Scenario = {
  header: object;
  timestamps: Date[];
  sessions: {
    [key: string]: SessionEntry[];
  };
}

const scenario1Timestamps = [
  new Date("2025-10-29T22:00:00Z"),
  new Date("2025-10-29T22:01:00Z"),
  new Date("2025-10-29T22:01:01Z"),
  new Date("2025-10-29T22:02:00Z"),
  new Date("2025-10-29T22:03:00Z"),
  new Date("2025-10-29T22:04:00Z"),
  new Date("2025-10-29T22:05:00Z"),
  new Date("2025-10-29T22:06:00Z"),
];

const header1 = {
  type: "comap",
  owner: "co_zCCymDTETFr2rv9U",
  createdAt: new Date("2025-10-29T22:00:00Z").toLocaleString(),
  uniqueness: "fc89fjwo3",
};

const scenario1Sessions = {
  alice_session_1: [
    {
      payload: { op: "set" as const, key: "color", value: "red" },
      t: scenario1Timestamps[1],
    } satisfies SessionEntry,
    {
      payload: { op: "set" as const, key: "height", value: 16 },
      t: scenario1Timestamps[2],
    } satisfies SessionEntry,
    {
      payload: { op: "set" as const, key: "height", value: 17 },
      t: scenario1Timestamps[6],
    } satisfies SessionEntry,
  ],
  bob_session_1: [
    {
      payload: { op: "set" as const, key: "color", value: "amber" },
      t: scenario1Timestamps[3],
    } satisfies SessionEntry,
    {
      payload: { op: "set" as const, key: "color", value: "grey" },
      t: scenario1Timestamps[4],
    } satisfies SessionEntry,
    {
      payload: { op: "set" as const, key: "color", value: "green" },
      t: scenario1Timestamps[7],
    } satisfies SessionEntry,
  ],
  bob_session_2: [
    {
      payload: { op: "set" as const, key: "height", value: 18 },
      t: scenario1Timestamps[5],
    },
  ],
};

export const scenario1: Scenario = {
  header: header1,
  timestamps: scenario1Timestamps,
  sessions: scenario1Sessions,
};