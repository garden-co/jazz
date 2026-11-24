import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it, vi } from "vitest";
import { storedBytes } from "../../tests/stored-bytes.js";

vi.mock(import("node:fs/promises"), async (importOriginal) => {
  const fs = await importOriginal();
  return { ...fs, readFile: vi.fn(fs.readFile) as typeof fs.readFile };
});

it("fails a storage scan when a file cannot be read", async () => {
  const dir = await mkdtemp(join(tmpdir(), "jazz-storage-scan-"));
  try {
    await writeFile(join(dir, "store"), "stored bytes");
    await expect(storedBytes(dir)).resolves.toEqual(Buffer.from("stored bytes"));
    const readError = Object.assign(new Error("cannot read the store file"), { code: "EACCES" });
    vi.mocked(readFile).mockRejectedValueOnce(readError);
    await expect(storedBytes(dir)).rejects.toBe(readError);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
