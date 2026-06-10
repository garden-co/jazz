import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import App from "./App.js";
import { encodeStorageBundle } from "./storage-bundle.js";
import { setMockEntries } from "./test/jazz-wasm-mock.js";

describe("OPFS B-tree viewer", () => {
  afterEach(() => {
    cleanup();
  });

  it("opens a bundle and renders raw entries", async () => {
    setMockEntries([
      {
        key: "raw:debug:alpha",
        keyBytes: new TextEncoder().encode("raw:debug:alpha"),
        value: new TextEncoder().encode("one"),
      },
    ]);
    const bundle = encodeStorageBundle({
      metadata: { dbName: "debug-db" },
      files: [{ path: "debug-db.opfsbtree", bytes: Uint8Array.from([1, 2, 3]) }],
    });
    const file = new File([bundle], "debug-db.jazz-opfs-bundle", {
      type: "application/vnd.jazz.opfs-btree-bundle",
    });

    render(<App />);
    fireEvent.change(screen.getByLabelText("Open storage bundle"), {
      target: { files: [file] },
    });

    expect(await screen.findByText("raw:debug:alpha")).toBeDefined();
    expect(screen.getAllByText("debug-db.opfsbtree")).toHaveLength(2);
    expect(screen.getByText("one")).toBeDefined();
    expect(screen.getAllByText("3 B")).toHaveLength(2);
  });

  it("paginates the raw entry list", async () => {
    setMockEntries(
      Array.from({ length: 101 }, (_, index) => {
        const key = `raw:debug:${String(index).padStart(3, "0")}`;
        return {
          key,
          keyBytes: new TextEncoder().encode(key),
          value: new TextEncoder().encode(`value-${index}`),
        };
      }),
    );
    const bundle = encodeStorageBundle({
      metadata: { dbName: "debug-db" },
      files: [{ path: "debug-db.opfsbtree", bytes: Uint8Array.from([1, 2, 3]) }],
    });
    const file = new File([bundle], "debug-db.jazz-opfs-bundle", {
      type: "application/vnd.jazz.opfs-btree-bundle",
    });

    render(<App />);
    fireEvent.change(screen.getByLabelText("Open storage bundle"), {
      target: { files: [file] },
    });

    expect(await screen.findByText("raw:debug:000")).toBeDefined();
    expect(screen.queryByText("raw:debug:100")).toBeNull();

    fireEvent.click(screen.getAllByText("Next")[0]!);

    expect(await screen.findByText("raw:debug:100")).toBeDefined();
    expect(screen.queryByText("raw:debug:000")).toBeNull();
  });
});
