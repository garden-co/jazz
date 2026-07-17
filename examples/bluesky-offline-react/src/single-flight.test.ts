import { describe, expect, it, vi } from "vitest";
import { singleFlight } from "./single-flight.js";

describe("single-flight work", () => {
  it("shares overlapping calls and allows a later retry", async () => {
    let release!: () => void;
    const task = vi.fn(() => new Promise<void>((resolve) => { release = resolve; }));
    const run = singleFlight(task);

    const first = run();
    const overlapping = run();
    expect(overlapping).toBe(first);
    expect(task).toHaveBeenCalledOnce();

    release();
    await first;
    run();
    expect(task).toHaveBeenCalledTimes(2);
  });
});
