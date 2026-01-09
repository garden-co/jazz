// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { Loaded, co, z } from "jazz-tools";
import { assertLoaded } from "jazz-tools/testing";
import { assert, beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import React, { Suspense } from "react";
import { useCoStates, useSuspenseCoStates } from "../hooks.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { act, renderHook, waitFor } from "./testUtils.js";

const ProjectSchema = co.map({
  name: z.string(),
});

beforeEach(async () => {
  cojsonInternals.setCoValueLoadingRetryDelay(20);

  await setupJazzTestSync({
    asyncPeers: true,
  });

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("useSuspenseCoStates", () => {
  it("should return loaded values for all subscriptions", async () => {
    const project1 = ProjectSchema.create({ name: "My Project 1" });
    const project2 = ProjectSchema.create({ name: "My Project 2" });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = await act(async () => {
      return renderHook(
        () =>
          useSuspenseCoStates(ProjectSchema, [
            project1.$jazz.id,
            project2.$jazz.id,
          ]),
        {
          wrapper,
        },
      );
    });

    // Wait for any async operations to complete
    await waitFor(() => {
      expect(result.current).toBeDefined();
      expect(result.current.length).toBe(2);
    });

    const [loadedProject1, loadedProject2] = result.current;

    assert(loadedProject1);
    expect(loadedProject1.name).toBe("My Project 1");

    assert(loadedProject2);
    expect(loadedProject2.name).toBe("My Project 2");
  });

  it("should have correct return types for each entry", async () => {
    const project1 = ProjectSchema.create({ name: "Project 1" });
    const project2 = ProjectSchema.create({ name: "Project 2" });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const ids = [project1.$jazz.id, project2.$jazz.id] as const;

    const { result } = await act(async () => {
      return renderHook(() => useSuspenseCoStates(ProjectSchema, ids), {
        wrapper,
      });
    });

    await waitFor(() => {
      expect(result.current).toBeDefined();
      expect(result.current.length).toBe(2);
    });

    expectTypeOf(result.current).toEqualTypeOf<
      Loaded<typeof ProjectSchema>[]
    >();
  });

  it("should re-render when any value changes", async () => {
    const project1 = ProjectSchema.create({ name: "Project 1" });
    const project2 = ProjectSchema.create({ name: "Project 2" });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = await act(async () => {
      return renderHook(
        () =>
          useSuspenseCoStates(ProjectSchema, [
            project1.$jazz.id,
            project2.$jazz.id,
          ]),
        {
          wrapper,
        },
      );
    });

    await waitFor(() => {
      expect(result.current).toBeDefined();
      expect(result.current.length).toBe(2);
    });

    assert(result.current[0]);
    assert(result.current[0]);
    expect(result.current[0].name).toBe("Project 1");

    // Update one of the values
    act(() => {
      project1.$jazz.set("name", "updated1");
    });

    await waitFor(() => {
      assert(result.current[0]);
      expect(result.current[0].name).toBe("updated1");
    });
  });

  it("should handle empty subscription array", async () => {
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = await act(async () => {
      return renderHook(() => useSuspenseCoStates(ProjectSchema, []), {
        wrapper,
      });
    });

    await waitFor(() => {
      expect(result.current).toBeDefined();
    });

    expect(result.current).toEqual([]);
  });
});

describe("useCoStates", () => {
  it("should return MaybeLoaded values", async () => {
    const project1 = ProjectSchema.create({ name: "My Project 1" });
    const project2 = ProjectSchema.create({ name: "My Project 2" });

    const { result } = renderHook(() =>
      useCoStates(ProjectSchema, [project1.$jazz.id, project2.$jazz.id]),
    );

    await waitFor(() => {
      expect(result.current[0]?.$isLoaded).toBe(true);
      expect(result.current[1]?.$isLoaded).toBe(true);
    });

    const [loadedProject1, loadedProject2] = result.current;

    assert(loadedProject1);
    assert(loadedProject2);
    assertLoaded(loadedProject1);
    assertLoaded(loadedProject2);
    expect(loadedProject1.name).toBe("My Project 1");
    expect(loadedProject2.name).toBe("My Project 2");
  });

  it("should re-render when any value changes", async () => {
    const project1 = ProjectSchema.create({ name: "Project 1" });
    const project2 = ProjectSchema.create({ name: "Project 2" });

    const { result } = renderHook(() =>
      useCoStates(ProjectSchema, [project1.$jazz.id, project2.$jazz.id]),
    );

    await waitFor(() => {
      expect(result.current[0]?.$isLoaded).toBe(true);
    });

    expect(result.current[0]).not.toBeNull();
    if (result.current[0]) {
      assertLoaded(result.current[0]);
      expect(result.current[0].name).toBe("Project 1");
    }

    // Update one of the values
    act(() => {
      project1.$jazz.set("name", "updated1");
    });

    await waitFor(() => {
      const val = result.current[0];
      return val?.$isLoaded && val.name === "updated1";
    });

    assert(result.current[0]);
    expect(result.current[0].name).toBe("updated1");
  });

  it("should handle empty subscription array", async () => {
    const { result } = renderHook(() => useCoStates(ProjectSchema, []));

    await waitFor(() => {
      expect(result.current).not.toBeNull();
    });

    expect(result.current).toEqual([]);
  });

  it("should update when ids change", async () => {
    const project1 = ProjectSchema.create({ name: "My Project 1" });
    const project2 = ProjectSchema.create({ name: "My Project 2" });

    let ids: string[] = [project1.$jazz.id, project2.$jazz.id];
    const { result, rerender } = renderHook(
      ({ ids }: { ids: string[] }) => useCoStates(ProjectSchema, ids),
      {
        initialProps: { ids },
      },
    );

    await waitFor(() => {
      expect(result.current[0]?.$isLoaded).toBe(true);
      expect(result.current[1]?.$isLoaded).toBe(true);
    });

    const project3 = ProjectSchema.create({ name: "My Project 3" });
    act(() => {
      // Create a new array with updated IDs
      ids = [project2.$jazz.id, project3.$jazz.id];
      rerender({ ids });
    });

    await waitFor(() => {
      expect(result.current[0]?.$isLoaded).toBe(true);
      expect(result.current[1]?.$isLoaded).toBe(true);
    });

    assert(result.current[0]);
    assert(result.current[1]);
    assertLoaded(result.current[0]);
    assertLoaded(result.current[1]);
    expect(result.current[0].name).toBe("My Project 2");
    expect(result.current[1].name).toBe("My Project 3");
  });

  it("should not update when ids are the same", async () => {
    const project1 = ProjectSchema.create({ name: "My Project 1" });
    const project2 = ProjectSchema.create({ name: "My Project 2" });

    let ids: string[] = [project1.$jazz.id, project2.$jazz.id];
    const { result, rerender } = renderHook(
      ({ ids }: { ids: string[] }) => useCoStates(ProjectSchema, ids),
      {
        initialProps: { ids },
      },
    );

    await waitFor(() => {
      expect(result.current[0]?.$isLoaded).toBe(true);
      expect(result.current[1]?.$isLoaded).toBe(true);
    });

    const firstResult = result.current;

    act(() => {
      // Create a new array with the same IDs
      ids = [...ids];
      rerender({ ids });
    });

    // The result should be the same reference when IDs haven't changed
    expect(result.current).toBe(firstResult);
    expect(result.current[0]).toBe(firstResult[0]);
    expect(result.current[1]).toBe(firstResult[1]);
  });

  it("should remove subscriptions for removed ids", async () => {
    const project1 = ProjectSchema.create({ name: "My Project 1" });
    const project2 = ProjectSchema.create({ name: "My Project 2" });

    let ids = [project1.$jazz.id, project2.$jazz.id];
    let renderCount = 0;
    const { result, rerender } = renderHook(
      ({ ids }: { ids: string[] }) => {
        renderCount++;
        return useCoStates(ProjectSchema, ids);
      },
      {
        initialProps: { ids },
      },
    );

    await waitFor(() => {
      expect(result.current[0]?.$isLoaded).toBe(true);
      expect(result.current[1]?.$isLoaded).toBe(true);
    });

    assert(result.current[0]);
    assertLoaded(result.current[0]);
    const loadedProject1 = result.current[0];

    act(() => {
      ids.shift(); // Remove project1, keeping only project2
      rerender({ ids });
    });

    await waitFor(() => {
      expect(result.current.length).toBe(1);
      expect(result.current[0]?.$isLoaded).toBe(true);
    });
    assert(result.current[0]);
    assertLoaded(result.current[0]);
    expect(result.current[0].name).toBe("My Project 2");

    expect(renderCount).toBe(2);

    // Modify project1. The hook should NOT re-render because project1 is no longer subscribed
    act(() => {
      project1.$jazz.set("name", "Modified Project 1");
    });

    // Wait a bit to ensure any potential updates would have occurred
    await new Promise((resolve) => setTimeout(resolve, 100));

    // The hook didn't re-render
    expect(renderCount).toBe(2);

    // project2's name is still the same
    assert(result.current[0]);
    assertLoaded(result.current[0]);
    expect(result.current[0].name).toBe("My Project 2");

    // project1's subscription scope is no longer subscribed to
    const project1SubscriptionScope = loadedProject1.$jazz._subscriptionScope;
    expect(project1SubscriptionScope?.subscribers.size).toBe(0);
  });
});
