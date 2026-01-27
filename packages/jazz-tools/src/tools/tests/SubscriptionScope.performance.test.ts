import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { SubscriptionScope } from "../subscribe/SubscriptionScope.js";
import { setupJazzTestSync } from "../testing.js";
import { setupAccount } from "./utils.js";
import { z } from "../index.js";
import { co } from "../internal.js";
import { CoID, RawCoValue, cojsonInternals } from "cojson";

describe("SubscriptionScope performance profiling", () => {
  beforeEach(async () => {
    await setupJazzTestSync();
    SubscriptionScope.setProfilingEnabled(true);
    performance.clearMarks();
    performance.clearMeasures();
    cojsonInternals.setCoValueLoadingRetryDelay(100);
  });

  afterEach(() => {
    SubscriptionScope.setProfilingEnabled(false);
  });

  it("emits performance entries on successful load", async () => {
    const { me, meOnSecondPeer } = await setupAccount();

    const TestMap = co.map({ name: z.string() });
    const value = TestMap.create({ name: "test" }, { owner: me });

    const loaded = await TestMap.load(value.$jazz.id, {
      loadAs: meOnSecondPeer,
    });

    expect(loaded).toBeDefined();

    const measures = performance.getEntriesByType(
      "measure",
    ) as PerformanceMeasure[];
    const measure = measures.find(
      (m) =>
        (m as PerformanceMeasure & { detail?: { id?: string } }).detail?.id ===
        value.$jazz.id,
    );

    expect(measure).toBeDefined();
    const detail = (
      measure as PerformanceMeasure & { detail: Record<string, unknown> }
    ).detail;
    expect(detail.type).toBe("jazz-subscription");
    expect(detail.status).toBe("loaded");
    expect(detail.uuid).toBeDefined();
    expect(detail.source).toBeDefined();
    expect(detail.resolve).toBeDefined();
    expect(measure!.duration).toBeGreaterThan(0);
  });

  it("does not emit performance entries when profiling is disabled", async () => {
    SubscriptionScope.setProfilingEnabled(false);

    const { me, meOnSecondPeer } = await setupAccount();

    const TestMap = co.map({ name: z.string() });
    const value = TestMap.create({ name: "test" }, { owner: me });

    const loaded = await TestMap.load(value.$jazz.id, {
      loadAs: meOnSecondPeer,
    });

    expect(loaded).toBeDefined();

    // Verify no jazz-subscription entries were created
    const measures = performance.getEntriesByType(
      "measure",
    ) as PerformanceMeasure[];
    const jazzMeasures = measures.filter(
      (m) =>
        (m as PerformanceMeasure & { detail?: { type?: string } }).detail
          ?.type === "jazz-subscription",
    );
    expect(jazzMeasures.length).toBe(0);

    const marks = performance.getEntriesByType("mark") as PerformanceMark[];
    const jazzMarks = marks.filter(
      (m) =>
        (m as PerformanceMark & { detail?: { type?: string } }).detail?.type ===
        "jazz-subscription",
    );
    expect(jazzMarks.length).toBe(0);
  });

  it("emits performance entries with error status on unavailable", async () => {
    const { meOnSecondPeer } = await setupAccount();

    const TestMap = co.map({ name: z.string() });
    const fakeId = "co_zFAKEIDTHATDOESNOTEXIST123" as CoID<RawCoValue>;

    const loaded = await TestMap.load(fakeId, { loadAs: meOnSecondPeer });

    expect(loaded.$isLoaded).toBe(false);

    const measures = performance.getEntriesByType(
      "measure",
    ) as PerformanceMeasure[];
    const measure = measures.find(
      (m) =>
        (m as PerformanceMeasure & { detail?: { id?: string } }).detail?.id ===
        fakeId,
    );

    expect(measure).toBeDefined();
    const detail = (
      measure as PerformanceMeasure & { detail: Record<string, unknown> }
    ).detail;
    expect(detail.status).toBe("error");
    expect(detail.errorType).toBe("unavailable");
  });

  it("emits start and end marks with correct detail", async () => {
    const { me, meOnSecondPeer } = await setupAccount();

    const TestMap = co.map({ name: z.string() });
    const value = TestMap.create({ name: "test" }, { owner: me });

    await TestMap.load(value.$jazz.id, { loadAs: meOnSecondPeer });

    const marks = performance.getEntriesByType("mark") as PerformanceMark[];
    const startMark = marks.find(
      (m) =>
        m.name.startsWith("jazz.subscription.start:") &&
        (m as PerformanceMark & { detail?: { id?: string } }).detail?.id ===
          value.$jazz.id,
    );
    const endMark = marks.find(
      (m) =>
        m.name.startsWith("jazz.subscription.end:") &&
        (m as PerformanceMark & { detail?: { id?: string } }).detail?.id ===
          value.$jazz.id,
    );

    expect(startMark).toBeDefined();
    expect(endMark).toBeDefined();
    expect(
      (startMark as PerformanceMark & { detail: { status: string } }).detail
        .status,
    ).toBe("pending");
    expect(
      (endMark as PerformanceMark & { detail: { status: string } }).detail
        .status,
    ).toBe("loaded");
  });
});
