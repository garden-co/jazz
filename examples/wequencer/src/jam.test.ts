import { describe, test, expect, vi, beforeEach, afterEach } from "vitest";
import {
  SEED_INSTRUMENTS,
  SEEDED_STORAGE_KEY,
  missingSeeds,
  ensureInstrumentsSeeded,
} from "./jam.js";

// ---------------------------------------------------------------------------
// missingSeeds
// ---------------------------------------------------------------------------

describe("missingSeeds", () => {
  test("returns all seeds when nothing exists", () => {
    expect(missingSeeds(new Set())).toEqual([...SEED_INSTRUMENTS]);
  });

  test("returns an empty array when all seeds are present", () => {
    const all = new Set(SEED_INSTRUMENTS.map((s) => s.name));
    expect(missingSeeds(all)).toHaveLength(0);
  });

  test("returns only the seeds whose names are absent", () => {
    const present = new Set(["Kick", "Snare"]);
    const missing = missingSeeds(present);
    expect(missing).toHaveLength(SEED_INSTRUMENTS.length - 2);
    expect(missing.map((s) => s.name)).not.toContain("Kick");
    expect(missing.map((s) => s.name)).not.toContain("Snare");
  });

  test("is not affected by names that are not in SEED_INSTRUMENTS", () => {
    const unrelated = new Set(["Oboe", "Triangle"]);
    expect(missingSeeds(unrelated)).toHaveLength(SEED_INSTRUMENTS.length);
  });
});

// ---------------------------------------------------------------------------
// ensureInstrumentsSeeded
// ---------------------------------------------------------------------------

const fakeBlob = new Blob([new Uint8Array(8)]);

function makeDb(instruments: { name: string }[] = []) {
  return {
    all: vi.fn().mockResolvedValue(instruments),
    insert: vi.fn(),
    createFileFromBlob: vi.fn().mockResolvedValue({ id: "file-id" }),
  };
}

function allSeedRows() {
  return SEED_INSTRUMENTS.map((s, i) => ({
    id: String(i),
    name: s.name,
    soundFileId: `file-${i}`,
    display_order: s.display_order,
  }));
}

describe("ensureInstrumentsSeeded", () => {
  beforeEach(() => {
    localStorage.removeItem(SEEDED_STORAGE_KEY);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ blob: () => Promise.resolve(fakeBlob) }));
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  // -- offline behaviour ----------------------------------------------------

  test("subsequent visit offline: resolves immediately without touching the db", async () => {
    localStorage.setItem(SEEDED_STORAGE_KEY, "1");
    const db = makeDb();
    db.all.mockRejectedValue(new Error("offline"));
    await expect(ensureInstrumentsSeeded(db as any)).resolves.toBeUndefined();
    expect(db.all).not.toHaveBeenCalled();
    expect(db.insert).not.toHaveBeenCalled();
  });

  test("first visit offline: propagates the error from db.all", async () => {
    const db = {
      all: vi.fn().mockRejectedValue(new Error("Edge server unreachable")),
      insert: vi.fn(),
    };
    await expect(ensureInstrumentsSeeded(db as any)).rejects.toThrow("Edge server unreachable");
    expect(db.insert).not.toHaveBeenCalled();
    // Flag must NOT be set — the next attempt should retry, not skip
    expect(localStorage.getItem(SEEDED_STORAGE_KEY)).toBeNull();
  });

  // -- localStorage guard ---------------------------------------------------

  test("skips db.all entirely when the localStorage flag is already set", async () => {
    localStorage.setItem(SEEDED_STORAGE_KEY, "1");
    const db = makeDb();
    await ensureInstrumentsSeeded(db as any);
    expect(db.all).not.toHaveBeenCalled();
    expect(db.insert).not.toHaveBeenCalled();
  });

  test("sets the localStorage flag after a successful seed", async () => {
    const db = makeDb([]);
    expect(localStorage.getItem(SEEDED_STORAGE_KEY)).toBeNull();
    await ensureInstrumentsSeeded(db as any);
    expect(localStorage.getItem(SEEDED_STORAGE_KEY)).toBe("1");
  });

  test("sets the localStorage flag even when no inserts were needed", async () => {
    const db = makeDb(allSeedRows());
    await ensureInstrumentsSeeded(db as any);
    expect(localStorage.getItem(SEEDED_STORAGE_KEY)).toBe("1");
  });

  // -- insert behaviour -----------------------------------------------------

  test("inserts all 7 seeds when the db is empty", async () => {
    const db = makeDb([]);
    await ensureInstrumentsSeeded(db as any);
    expect(db.insert).toHaveBeenCalledTimes(SEED_INSTRUMENTS.length);
  });

  test("inserts nothing when all seeds are already present", async () => {
    const db = makeDb(allSeedRows());
    await ensureInstrumentsSeeded(db as any);
    expect(db.insert).not.toHaveBeenCalled();
  });

  test("inserts only the missing seeds when the db is partially populated", async () => {
    const partial = [
      { id: "1", name: "Kick", soundFileId: "file-0", display_order: 0 },
      { id: "2", name: "Snare", soundFileId: "file-1", display_order: 1 },
    ];
    const db = makeDb(partial);
    await ensureInstrumentsSeeded(db as any);
    expect(db.insert).toHaveBeenCalledTimes(SEED_INSTRUMENTS.length - 2);
    const insertedNames = db.insert.mock.calls.map((c: any[]) => c[1].name);
    expect(insertedNames).not.toContain("Kick");
    expect(insertedNames).not.toContain("Snare");
  });

  test("inserts with the correct shape", async () => {
    const db = makeDb([]);
    await ensureInstrumentsSeeded(db as any);
    for (const call of db.insert.mock.calls as any[]) {
      const data = call[1];
      expect(data).toHaveProperty("name");
      expect(data).toHaveProperty("soundFileId");
      expect(typeof data.soundFileId).toBe("string");
      expect(data).toHaveProperty("display_order");
      expect(typeof data.display_order).toBe("number");
    }
  });
});
