import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { useLocalStorageState } from "./use-local-storage-state.js";

function isStoredNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

function LocalStorageNumberProbe({ storageKey }: { storageKey: string }) {
  const [value, setValue] = useLocalStorageState(storageKey, 5, {
    isValid: isStoredNumber,
  });

  return (
    <button type="button" onClick={() => setValue((currentValue) => currentValue + 1)}>
      {value}
    </button>
  );
}

describe("useLocalStorageState", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    cleanup();
  });

  it("initializes from localStorage and writes updates back", () => {
    localStorage.setItem("test.storage.number", "12");

    render(<LocalStorageNumberProbe storageKey="test.storage.number" />);

    const button = screen.getByRole("button", { name: "12" });
    fireEvent.click(button);

    expect(screen.getByRole("button", { name: "13" })).not.toBeNull();
    expect(localStorage.getItem("test.storage.number")).toBe("13");
  });

  it("uses the default value when the stored value is invalid", () => {
    localStorage.setItem("test.storage.number", "-1");

    render(<LocalStorageNumberProbe storageKey="test.storage.number" />);

    expect(screen.getByRole("button", { name: "5" })).not.toBeNull();
  });
});
