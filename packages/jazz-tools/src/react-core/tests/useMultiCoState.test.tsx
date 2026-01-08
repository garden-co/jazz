// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { Loaded, co, z } from "jazz-tools";
import { assertLoaded } from "jazz-tools/testing";
import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import React, { Suspense } from "react";
import { useMultiCoState, useSuspenseMultiCoState } from "../hooks.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { act, renderHook, waitFor } from "./testUtils.js";

// Silence unhandled rejection errors coming from Suspense
process.on("unhandledRejection", () => {});

beforeEach(async () => {
  cojsonInternals.setCoValueLoadingRetryDelay(20);

  await setupJazzTestSync({
    asyncPeers: true,
  });

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("useSuspenseMultiCoState", () => {
  it("should return loaded values for all subscriptions", async () => {
    const ProjectSchema = co.map({
      name: z.string(),
    });

    const project1 = ProjectSchema.create({ name: "My Project 1" });
    const project2 = ProjectSchema.create({ name: "My Project 2" });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = renderHook(
      () =>
        useSuspenseMultiCoState(ProjectSchema, [
          project1.$jazz.id,
          project2.$jazz.id,
        ]),
      {
        wrapper,
      },
    );

    await waitFor(() => result.current?.length === 2);

    const [loadedProject1, loadedProject2] = result.current;

    expect(loadedProject1).not.toBeNull();
    expect(loadedProject1).not.toBeUndefined();
    assertLoaded(loadedProject1!);
    expect(loadedProject1!.name).toBe("My Project 1");

    expect(loadedProject2).not.toBeNull();
    expect(loadedProject2).not.toBeUndefined();
    assertLoaded(loadedProject2!);
    expect(loadedProject2!.name).toBe("My Project 2");
  });

  it("should have correct return types for each entry", async () => {
    const ProjectSchema = co.map({
      name: z.string(),
      priority: z.number(),
    });

    const project1 = ProjectSchema.create({ name: "Project 1", priority: 1 });
    const project2 = ProjectSchema.create({ name: "Project 2", priority: 2 });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const ids = [project1.$jazz.id, project2.$jazz.id] as const;

    const { result } = renderHook(
      () => useSuspenseMultiCoState(ProjectSchema, ids),
      {
        wrapper,
      },
    );

    await waitFor(() => {
      expect(result.current).not.toBeNull();
      expect(result.current.length).toBe(2);
    });

    const [loadedProject1, loadedProject2] = result.current;

    // Verify types are correctly inferred
    // When all IDs are strings, result should be tuple of Loaded<S, R> without null
    expectTypeOf(result.current).toEqualTypeOf<
      [Loaded<typeof ProjectSchema>, Loaded<typeof ProjectSchema>]
    >();
  });

  it("should return null for undefined IDs", async () => {
    const ProjectSchema = co.map({
      name: z.string(),
    });

    const project = ProjectSchema.create({ name: "My Project" });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = renderHook(
      () =>
        useSuspenseMultiCoState(ProjectSchema, [project.$jazz.id, undefined]),
      {
        wrapper,
      },
    );

    await waitFor(() => {
      expect(result.current).not.toBeNull();
      expect(result.current.length).toBe(2);
    });

    const [loadedProject, nullValue] = result.current;

    expect(loadedProject).not.toBeNull();
    expect(loadedProject).not.toBeUndefined();
    assertLoaded(loadedProject!);
    expect(loadedProject!.name).toBe("My Project");
    expect(nullValue).toBe(null);
  });

  it("should re-render when any value changes", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const map1 = TestMap.create({ value: "initial1" });
    const map2 = TestMap.create({ value: "initial2" });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = renderHook(
      () => useSuspenseMultiCoState(TestMap, [map1.$jazz.id, map2.$jazz.id]),
      {
        wrapper,
      },
    );

    await waitFor(() => {
      expect(result.current).not.toBeNull();
      expect(result.current.length).toBe(2);
    });

    expect(result.current[0]).not.toBeNull();
    expect(result.current[0]).not.toBeUndefined();
    assertLoaded(result.current[0]!);
    expect(result.current[0]!.value).toBe("initial1");

    // Update one of the values
    act(() => {
      map1.$jazz.set("value", "updated1");
    });

    await waitFor(() => {
      expect(result.current[0]?.value).toBe("updated1");
    });
  });

  it("should handle empty subscription array", async () => {
    const ProjectSchema = co.map({
      name: z.string(),
    });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = renderHook(
      () => useSuspenseMultiCoState(ProjectSchema, []),
      {
        wrapper,
      },
    );

    await waitFor(() => {
      expect(result.current).not.toBeNull();
    });

    expect(result.current).toEqual([]);
  });
});

describe("useMultiCoState", () => {
  it("should return MaybeLoaded values without suspending", async () => {
    const ProjectSchema = co.map({
      name: z.string(),
    });

    const project1 = ProjectSchema.create({ name: "My Project 1" });
    const project2 = ProjectSchema.create({ name: "My Project 2" });

    const { result } = renderHook(() =>
      useMultiCoState(ProjectSchema, [project1.$jazz.id, project2.$jazz.id]),
    );

    await waitFor(() => {
      expect(result.current[0]?.$isLoaded).toBe(true);
      expect(result.current[1]?.$isLoaded).toBe(true);
    });

    const [loadedProject1, loadedProject2] = result.current;

    expect(loadedProject1).not.toBeNull();
    if (loadedProject1) {
      assertLoaded(loadedProject1);
      expect(loadedProject1.name).toBe("My Project 1");
    }

    expect(loadedProject2).not.toBeNull();
    if (loadedProject2) {
      assertLoaded(loadedProject2);
      expect(loadedProject2.name).toBe("My Project 2");
    }
  });

  it("should return null for undefined IDs", async () => {
    const ProjectSchema = co.map({
      name: z.string(),
    });

    const project = ProjectSchema.create({ name: "My Project" });

    const { result } = renderHook(() =>
      useMultiCoState(ProjectSchema, [project.$jazz.id, undefined]),
    );

    await waitFor(() => {
      expect(result.current[0]?.$isLoaded).toBe(true);
    });

    const [loadedProject, nullValue] = result.current;

    expect(loadedProject).not.toBeNull();
    if (loadedProject) {
      assertLoaded(loadedProject);
      expect(loadedProject.name).toBe("My Project");
    }
    expect(nullValue).toBe(null);
  });

  it("should re-render when any value changes", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const map1 = TestMap.create({ value: "initial1" });
    const map2 = TestMap.create({ value: "initial2" });

    const { result } = renderHook(() =>
      useMultiCoState(TestMap, [map1.$jazz.id, map2.$jazz.id]),
    );

    await waitFor(() => {
      expect(result.current[0]?.$isLoaded).toBe(true);
    });

    expect(result.current[0]).not.toBeNull();
    if (result.current[0]) {
      assertLoaded(result.current[0]);
      expect(result.current[0].value).toBe("initial1");
    }

    // Update one of the values
    act(() => {
      map1.$jazz.set("value", "updated1");
    });

    await waitFor(() => {
      const val = result.current[0];
      return val?.$isLoaded && val.value === "updated1";
    });

    expect(result.current[0]).not.toBeNull();
    if (result.current[0]) {
      assertLoaded(result.current[0]);
      expect(result.current[0].value).toBe("updated1");
    }
  });

  it("should handle empty subscription array", async () => {
    const ProjectSchema = co.map({
      name: z.string(),
    });

    const { result } = renderHook(() => useMultiCoState(ProjectSchema, []));

    await waitFor(() => {
      expect(result.current).not.toBeNull();
    });

    expect(result.current).toEqual([]);
  });
});
