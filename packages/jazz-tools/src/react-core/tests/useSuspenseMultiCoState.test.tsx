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

    const TaskSchema = co.map({
      title: z.string(),
    });

    const project = ProjectSchema.create({ name: "My Project" });
    const task = TaskSchema.create({ title: "Task 1" });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = renderHook(
      () =>
        useSuspenseMultiCoState([
          { schema: ProjectSchema, id: project.$jazz.id },
          { schema: TaskSchema, id: task.$jazz.id },
        ] as const),
      {
        wrapper,
      },
    );

    await waitFor(() => {
      expect(result.current).not.toBeNull();
      expect(result.current.length).toBe(2);
    });

    const [loadedProject, loadedTask] = result.current;

    assertLoaded(loadedProject);
    expect(loadedProject.name).toBe("My Project");

    assertLoaded(loadedTask);
    expect(loadedTask.title).toBe("Task 1");
  });

  it("should have correct return types for each entry", async () => {
    const ProjectSchema = co.map({
      name: z.string(),
    });

    const TaskSchema = co.map({
      title: z.string(),
      priority: z.number(),
    });

    const project = ProjectSchema.create({ name: "Project" });
    const task = TaskSchema.create({ title: "Task", priority: 1 });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = renderHook(
      () =>
        useSuspenseMultiCoState([
          { schema: ProjectSchema, id: project.$jazz.id },
          { schema: TaskSchema, id: task.$jazz.id },
        ] as const),
      {
        wrapper,
      },
    );

    await waitFor(() => {
      expect(result.current).not.toBeNull();
      expect(result.current.length).toBe(2);
    });

    const [loadedProject, loadedTask] = result.current;

    // Verify types are correctly inferred
    expectTypeOf(loadedProject).toEqualTypeOf<Loaded<
      typeof ProjectSchema
    > | null>();
    expectTypeOf(loadedTask).toEqualTypeOf<Loaded<typeof TaskSchema> | null>();
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
        useSuspenseMultiCoState([
          { schema: ProjectSchema, id: project.$jazz.id },
          { schema: ProjectSchema, id: undefined },
        ] as const),
      {
        wrapper,
      },
    );

    await waitFor(() => {
      expect(result.current).not.toBeNull();
      expect(result.current.length).toBe(2);
    });

    const [loadedProject, nullValue] = result.current;

    assertLoaded(loadedProject);
    expect(loadedProject.name).toBe("My Project");
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
      () =>
        useSuspenseMultiCoState([
          { schema: TestMap, id: map1.$jazz.id },
          { schema: TestMap, id: map2.$jazz.id },
        ] as const),
      {
        wrapper,
      },
    );

    await waitFor(() => {
      expect(result.current).not.toBeNull();
      expect(result.current.length).toBe(2);
    });

    assertLoaded(result.current[0]);
    expect(result.current[0].value).toBe("initial1");

    // Update one of the values
    act(() => {
      map1.$jazz.set("value", "updated1");
    });

    await waitFor(() => {
      expect(result.current[0]?.value).toBe("updated1");
    });
  });

  it("should handle empty subscription array", async () => {
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = renderHook(() => useSuspenseMultiCoState([] as const), {
      wrapper,
    });

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

    const TaskSchema = co.map({
      title: z.string(),
    });

    const project = ProjectSchema.create({ name: "My Project" });
    const task = TaskSchema.create({ title: "Task 1" });

    const { result } = renderHook(() =>
      useMultiCoState([
        { schema: ProjectSchema, id: project.$jazz.id },
        { schema: TaskSchema, id: task.$jazz.id },
      ] as const),
    );

    await waitFor(() => {
      expect(result.current[0]?.$isLoaded).toBe(true);
      expect(result.current[1]?.$isLoaded).toBe(true);
    });

    const [loadedProject, loadedTask] = result.current;

    assertLoaded(loadedProject);
    expect(loadedProject.name).toBe("My Project");

    assertLoaded(loadedTask);
    expect(loadedTask.title).toBe("Task 1");
  });

  it("should return null for undefined IDs", async () => {
    const ProjectSchema = co.map({
      name: z.string(),
    });

    const project = ProjectSchema.create({ name: "My Project" });

    const { result } = renderHook(() =>
      useMultiCoState([
        { schema: ProjectSchema, id: project.$jazz.id },
        { schema: ProjectSchema, id: undefined },
      ] as const),
    );

    await waitFor(() => {
      expect(result.current[0]?.$isLoaded).toBe(true);
    });

    const [loadedProject, nullValue] = result.current;

    assertLoaded(loadedProject);
    expect(loadedProject.name).toBe("My Project");
    expect(nullValue).toBe(null);
  });

  it("should re-render when any value changes", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const map1 = TestMap.create({ value: "initial1" });
    const map2 = TestMap.create({ value: "initial2" });

    const { result } = renderHook(() =>
      useMultiCoState([
        { schema: TestMap, id: map1.$jazz.id },
        { schema: TestMap, id: map2.$jazz.id },
      ] as const),
    );

    await waitFor(() => {
      expect(result.current[0]?.$isLoaded).toBe(true);
    });

    assertLoaded(result.current[0]);
    expect(result.current[0].value).toBe("initial1");

    // Update one of the values
    act(() => {
      map1.$jazz.set("value", "updated1");
    });

    await waitFor(() => {
      const val = result.current[0];
      return val?.$isLoaded && val.value === "updated1";
    });

    assertLoaded(result.current[0]);
    expect(result.current[0].value).toBe("updated1");
  });

  it("should handle empty subscription array", async () => {
    const { result } = renderHook(() => useMultiCoState([] as const));

    await waitFor(() => {
      expect(result.current).not.toBeNull();
    });

    expect(result.current).toEqual([]);
  });
});
