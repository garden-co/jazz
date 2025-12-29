// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { Group, Loaded, co, getJazzErrorType, z } from "jazz-tools";
import { assertLoaded, disableJazzTestSync } from "jazz-tools/testing";
import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import React, { Suspense, useRef } from "react";
import { useSuspenseCoState } from "../hooks.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import {
  act,
  createAsyncStorage,
  render,
  renderHook,
  waitFor,
} from "./testUtils.js";
import { ErrorBoundary } from "react-error-boundary";

// Hook to track render count
const useRenderCount = <T,>(hook: () => T) => {
  const renderCountRef = useRef(0);
  const result = hook();
  renderCountRef.current = renderCountRef.current + 1;
  return {
    renderCount: renderCountRef.current,
    result,
  };
};

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

function ErrorFallback(props: { error: Error }) {
  return <div>Error: {getJazzErrorType(props.error)}</div>;
}

describe("useSuspenseCoState", () => {
  it("should return loaded value without suspending when data is available", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
    });

    let suspenseTriggered = false;

    const SuspenseFallback = () => {
      suspenseTriggered = true;
      return <div>Loading...</div>;
    };

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<SuspenseFallback />}>{children}</Suspense>
    );

    const { result } = renderHook(
      () => useSuspenseCoState(TestMap, map.$jazz.id),
      {
        account,
        wrapper,
      },
    );

    // Wait for any async operations to complete
    await waitFor(() => {
      expect(result.current).toBeDefined();
    });

    // Verify Suspense was not triggered since data was immediately available
    expect(suspenseTriggered).toBe(false);

    // Verify the hook returns loaded data
    assertLoaded(result.current);
    expect(result.current.value).toBe("123");
  });

  it("should have Loaded<T> return type", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
    });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = renderHook(
      () => useSuspenseCoState(TestMap, map.$jazz.id),
      {
        account,
        wrapper,
      },
    );

    await waitFor(() => {
      expect(result.current).toBeDefined();
    });

    // Verify the return type is Loaded<typeof TestMap>
    expectTypeOf(result.current).toEqualTypeOf<Loaded<typeof TestMap>>();
  });

  it("should suspend when data is not immediately available", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const map = TestMap.create(
      {
        value: "123",
      },
      Group.create().makePublic("reader"),
    );

    const viewerAccount = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    let suspenseTriggered = false;

    const SuspenseFallback = () => {
      suspenseTriggered = true;
      return <div>Loading...</div>;
    };

    const TestComponent = () => {
      const value = useSuspenseCoState(TestMap, map.$jazz.id);
      return <div>{value.value}</div>;
    };

    const { container } = await act(async () => {
      return render(
        <Suspense fallback={<SuspenseFallback />}>
          <TestComponent />
        </Suspense>,
        {
          account: viewerAccount,
        },
      );
    });

    expect(suspenseTriggered).toBe(true);

    // Wait for data to load - the subscription should update and resolve
    await waitFor(() => {
      expect(container.textContent).toContain("123");
      expect(container.textContent).not.toContain("Loading...");
    });
  });

  it("should throw error when CoValue is unavailable", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const map = TestMap.create(
      {
        value: "123",
      },
      Group.create().makePublic("reader"),
    );

    await setupJazzTestSync();

    const viewerAccount = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const TestComponent = () => {
      const value = useSuspenseCoState(TestMap, map.$jazz.id);
      return <div>{value.value}</div>;
    };

    const { container } = await act(async () => {
      return render(
        <ErrorBoundary FallbackComponent={ErrorFallback}>
          <Suspense fallback={<div>Loading...</div>}>
            <TestComponent />
          </Suspense>
        </ErrorBoundary>,
        {
          account: viewerAccount,
        },
      );
    });

    // Verify error is displayed in error boundary
    await waitFor(
      () => {
        expect(container.textContent).toContain("Error: unavailable");
      },
      { timeout: 10_000 },
    );
  });

  it("should throw error when CoValue is deleted", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const owner = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create(
      {
        value: "123",
      },
      Group.create(owner).makePublic("reader"),
    );

    map.$jazz.raw.core.deleteCoValue();

    const TestComponent = () => {
      const value = useSuspenseCoState(TestMap, map.$jazz.id);
      return <div>{value.value}</div>;
    };

    const { container } = await act(async () => {
      return render(
        <ErrorBoundary FallbackComponent={ErrorFallback}>
          <Suspense fallback={<div>Loading...</div>}>
            <TestComponent />
          </Suspense>
        </ErrorBoundary>,
        {
          account: owner,
        },
      );
    });

    await waitFor(
      () => {
        expect(container.textContent).toContain("Error: deleted");
      },
      { timeout: 10_000 },
    );
  });

  it("should throw error when CoValue is unavailable due disabled network", async () => {
    disableJazzTestSync();

    const TestMap = co.map({
      value: z.string(),
    });

    const map = TestMap.create(
      {
        value: "123",
      },
      Group.create().makePublic("reader"),
    );

    const viewerAccount = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    viewerAccount.$jazz.localNode.setStorage(await createAsyncStorage());

    const TestComponent = () => {
      const value = useSuspenseCoState(TestMap, map.$jazz.id);
      return <div>{value.value}</div>;
    };

    const { container } = await act(async () => {
      return render(
        <ErrorBoundary FallbackComponent={ErrorFallback}>
          <Suspense fallback={<div>Loading...</div>}>
            <TestComponent />
          </Suspense>
        </ErrorBoundary>,
        {
          account: viewerAccount,
        },
      );
    });

    // Verify error is displayed in error boundary
    await waitFor(
      () => {
        expect(container.textContent).toContain("Error: unavailable");
      },
      { timeout: 10_000 },
    );
  });

  it("should throw error when CoValue is unavailable due to missing loading sources", async () => {
    disableJazzTestSync();

    const TestMap = co.map({
      value: z.string(),
    });

    const map = TestMap.create(
      {
        value: "123",
      },
      Group.create().makePublic("reader"),
    );

    const viewerAccount = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const TestComponent = () => {
      const value = useSuspenseCoState(TestMap, map.$jazz.id);
      return <div>{value.value}</div>;
    };

    const { container } = await act(async () => {
      return render(
        <ErrorBoundary FallbackComponent={ErrorFallback}>
          <Suspense fallback={<div>Loading...</div>}>
            <TestComponent />
          </Suspense>
        </ErrorBoundary>,
        {
          account: viewerAccount,
        },
      );
    });

    // Verify error is displayed in error boundary
    await waitFor(() => {
      expect(container.textContent).toContain("Error: unavailable");
    });
  });

  it("should throw error with invalid subscription ID", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const TestComponent = () => {
      const value = useSuspenseCoState(TestMap, "invalid-id");
      return <div>{value.value}</div>;
    };

    const { container } = await act(async () => {
      return render(
        <ErrorBoundary FallbackComponent={ErrorFallback}>
          <Suspense fallback={<div>Loading...</div>}>
            <TestComponent />
          </Suspense>
        </ErrorBoundary>,
      );
    });

    // Wait for error to be thrown
    await waitFor(
      () => {
        expect(container.textContent).toContain("Error: unavailable");
      },
      { timeout: 1000 },
    );
  });

  it("should throw error when CoValue is unauthorized", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    // Create CoValue owned by another account without sharing
    const map = TestMap.create(
      {
        value: "123",
      },
      Group.create(),
    );

    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const TestComponent = () => {
      const value = useSuspenseCoState(TestMap, map.$jazz.id);
      return <div>{value.value}</div>;
    };

    const { container } = await act(async () => {
      return render(
        <ErrorBoundary FallbackComponent={ErrorFallback}>
          <Suspense fallback={<div>Loading...</div>}>
            <TestComponent />
          </Suspense>
        </ErrorBoundary>,
      );
    });

    // Wait for error to be thrown (unauthorized access)
    await waitFor(() => {
      expect(container.textContent).toContain("Error: unauthorized");
    });
  });

  it("should throw error when CoValue becomes unauthorized", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const group = Group.create();
    group.addMember("everyone", "reader");

    // Create CoValue owned by another account without sharing
    const map = TestMap.create(
      {
        value: "123",
      },
      group,
    );

    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const TestComponent = () => {
      const value = useSuspenseCoState(TestMap, map.$jazz.id);
      return <div>{value.value}</div>;
    };

    const { container } = await act(async () => {
      return render(
        <ErrorBoundary FallbackComponent={ErrorFallback}>
          <Suspense fallback={<div>Loading...</div>}>
            <TestComponent />
          </Suspense>
        </ErrorBoundary>,
      );
    });
    await waitFor(() => {
      expect(container.textContent).toContain("123");
      expect(container.textContent).not.toContain("Loading...");
    });

    group.removeMember("everyone");

    // Wait for error to be thrown (unauthorized access)
    await waitFor(() => {
      expect(container.textContent).toContain("Error: unauthorized");
    });
  });

  it("should update value when CoValue changes", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
    });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = renderHook(
      () => useSuspenseCoState(TestMap, map.$jazz.id),
      {
        account,
        wrapper,
      },
    );

    // Wait for initial load
    await waitFor(() => {
      expect(result.current).toBeTruthy();
    });

    // Verify initial value is correct
    assertLoaded(result.current);
    expect(result.current.value).toBe("123");

    // Update the CoValue field
    act(() => {
      map.$jazz.set("value", "456");
    });

    // Verify the hook returns updated value
    await waitFor(() => {
      expect(result.current.value).toBe("456");
    });

    // Verify it's still loaded (no suspension occurred)
    assertLoaded(result.current);
  });

  it("should maintain loaded state during updates", async () => {
    const TestMap = co.map({
      value: z.string(),
      count: z.number(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "initial",
      count: 0,
    });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = renderHook(
      () => useSuspenseCoState(TestMap, map.$jazz.id),
      {
        account,
        wrapper,
      },
    );

    // Wait for initial load
    await waitFor(() => {
      expect(result.current).toBeDefined();
    });

    assertLoaded(result.current);
    expect(result.current.value).toBe("initial");
    expect(result.current.count).toBe(0);

    // Update multiple fields
    act(() => {
      map.$jazz.set("value", "updated");
      map.$jazz.set("count", 42);
    });

    // Verify all changes are reflected
    await waitFor(() => {
      expect(result.current.value).toBe("updated");
      expect(result.current.count).toBe(42);
    });

    // Verify still loaded (no suspension)
    assertLoaded(result.current);
  });

  it("should load nested values with resolve query", async () => {
    const TestNestedMap = co.map({
      value: z.string(),
    });

    const TestMap = co.map({
      value: z.string(),
      nested: TestNestedMap,
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
      nested: TestNestedMap.create({
        value: "456",
      }),
    });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = renderHook(
      () =>
        useSuspenseCoState(TestMap, map.$jazz.id, {
          resolve: {
            nested: true,
          },
        }),
      {
        account,
        wrapper,
      },
    );

    // Wait for both parent and nested values to load
    await waitFor(() => {
      expect(result.current).toBeDefined();
    });

    // Verify both parent and nested values are loaded
    assertLoaded(result.current);
    expect(result.current.value).toBe("123");
    assertLoaded(result.current.nested);
    expect(result.current.nested.value).toBe("456");
  });

  it("should auto-load nested values on access", async () => {
    const TestNestedMap = co.map({
      value: z.string(),
    });

    const TestMap = co.map({
      value: z.string(),
      nested: TestNestedMap,
    });

    const map = TestMap.create(
      {
        value: "123",
        nested: {
          value: "456",
        },
      },
      Group.create().makePublic("reader"),
    );

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    // Preload the CoValue to avoid that the initial load triggers a suspension
    await TestMap.load(map.$jazz.id);

    let suspenseTriggered = false;

    const SuspenseFallback = () => {
      suspenseTriggered = true;
      return <div>Loading...</div>;
    };

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<SuspenseFallback />}>{children}</Suspense>
    );

    const { result } = renderHook(
      () => useSuspenseCoState(TestMap, map.$jazz.id),
      {
        account,
        wrapper,
      },
    );

    // Wait for parent value to load
    await waitFor(() => {
      expect(result.current).toBeDefined();
    });

    // Verify parent value is loaded
    assertLoaded(result.current);
    expect(result.current.value).toBe("123");

    // Access nested value - it should load automatically
    await waitFor(() => {
      assertLoaded(result.current.nested);
      expect(result.current.nested.value).toBe("456");
    });

    // Verify Suspense was not triggered during the autoload
    expect(suspenseTriggered).toBe(false);
  });

  it("should load deeply nested structures", async () => {
    const Message = co.map({
      content: co.plainText(),
    });
    const Messages = co.list(Message);
    const Thread = co.map({
      messages: Messages,
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const thread = Thread.create({
      messages: Messages.create([
        Message.create({
          content: "Hello man!",
        }),
        Message.create({
          content: "The temperature is high today",
        }),
        Message.create({
          content: "Shall we go to the beach?",
        }),
      ]),
    });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = renderHook(
      () =>
        useSuspenseCoState(Thread, thread.$jazz.id, {
          resolve: {
            messages: {
              $each: {
                content: true,
              },
            },
          },
        }),
      {
        account,
        wrapper,
      },
    );

    // Wait for all nested levels to load
    await waitFor(() => {
      expect(result.current).toBeDefined();
    });

    // Verify all nested levels are loaded
    assertLoaded(result.current);
    expect(result.current.messages.length).toBe(3);

    // Verify each message and its content are loaded
    const message0 = result.current.messages[0];
    expect(message0).toBeDefined();
    assertLoaded(message0!);
    expect(message0!.content.toString()).toBe("Hello man!");

    const message1 = result.current.messages[1];
    expect(message1).toBeDefined();
    assertLoaded(message1!);
    expect(message1!.content.toString()).toBe("The temperature is high today");

    const message2 = result.current.messages[2];
    expect(message2).toBeDefined();
    assertLoaded(message2!);
    expect(message2!.content.toString()).toBe("Shall we go to the beach?");
  });

  it("should work with selector function", async () => {
    const TestMap = co.map({
      value: z.string(),
      count: z.number(),
      metadata: z.string(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "test",
      count: 42,
      metadata: "extra",
    });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    // Selector that transforms data - returns only value and count as an object
    const { result } = renderHook(
      () =>
        useSuspenseCoState(TestMap, map.$jazz.id, {
          select: (value) => ({
            value: value.value,
            count: value.count,
          }),
        }),
      {
        account,
        wrapper,
      },
    );

    // Wait for data to load
    await waitFor(() => {
      expect(result.current).toBeDefined();
    });

    // Verify returned value is the transformed result
    expect(result.current).toEqual({
      value: "test",
      count: 42,
    });

    // Verify metadata is not included (selector filtered it out)
    expect(result.current).not.toHaveProperty("metadata");

    // Verify return type matches selector output type
    expectTypeOf(result.current).toEqualTypeOf<{
      value: string;
      count: number;
    }>();
  });

  it("should maintain type safety with selector", async () => {
    const TestMap = co.map({
      value: z.string(),
      count: z.number(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "hello",
      count: 10,
    });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    // Selector that returns a string
    const { result } = renderHook(
      () =>
        useSuspenseCoState(TestMap, map.$jazz.id, {
          select: (value) => `${value.value}: ${value.count}`,
        }),
      {
        account,
        wrapper,
      },
    );

    // Wait for data to load
    await waitFor(() => {
      expect(result.current).toBeDefined();
    });

    // Verify returned value is the transformed result
    expect(result.current).toBe("hello: 10");

    // Verify return type matches selector output type (string)
    expectTypeOf(result.current).toEqualTypeOf<string>();
  });

  it("should update selector result when CoValue changes", async () => {
    const TestMap = co.map({
      value: z.string(),
      count: z.number(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "initial",
      count: 0,
    });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    // Selector that combines value and count
    const { result } = renderHook(
      () =>
        useSuspenseCoState(TestMap, map.$jazz.id, {
          select: (value) => `${value.value}-${value.count}`,
        }),
      {
        account,
        wrapper,
      },
    );

    // Wait for initial load
    await waitFor(() => {
      expect(result.current).toBeDefined();
    });

    expect(result.current).toBe("initial-0");

    // Update the CoValue
    act(() => {
      map.$jazz.set("value", "updated");
      map.$jazz.set("count", 100);
    });

    // Verify selector result updates
    await waitFor(() => {
      expect(result.current).toBe("updated-100");
    });
  });

  it("should respect custom equality function", async () => {
    const TestMap = co.map({
      count: z.number(),
      metadata: z.string(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      count: 0,
      metadata: "initial",
    });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    // Custom equality function that compares count only
    const { result } = renderHook(
      () =>
        useRenderCount(() =>
          useSuspenseCoState(TestMap, map.$jazz.id, {
            select: (value) => ({
              count: value.count,
              metadata: value.metadata,
            }),
            equalityFn: (a, b) => a.count === b.count,
          }),
        ),
      {
        account,
        wrapper,
      },
    );

    // Wait for initial load
    await waitFor(() => {
      expect(result.current.result).toBeDefined();
    });

    // Verify initial render
    expect(result.current.renderCount).toBe(1);
    expect(result.current.result.count).toBe(0);
    expect(result.current.result.metadata).toBe("initial");

    const initialRenderCount = result.current.renderCount;

    // Update metadata field (equality should return true - count unchanged)
    act(() => {
      map.$jazz.set("metadata", "updated");
    });

    // Wait a bit to ensure no re-render occurred
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Verify no re-render occurred (equality function returned true)
    expect(result.current.renderCount).toBe(initialRenderCount);
    // Note: The result might still show old metadata since no re-render occurred
    // But the underlying data has changed

    // Update count field (equality should return false - count changed)
    act(() => {
      map.$jazz.set("count", 42);
    });

    // Verify re-render occurred
    await waitFor(() => {
      expect(result.current.renderCount).toBe(initialRenderCount + 1);
      expect(result.current.result.count).toBe(42);
    });
  });

  it("should prevent re-renders when equality returns true", async () => {
    const TestMap = co.map({
      value: z.string(),
      count: z.number(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "test",
      count: 10,
    });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    // Equality function that always returns true (prevents all re-renders)
    const { result } = renderHook(
      () =>
        useRenderCount(() =>
          useSuspenseCoState(TestMap, map.$jazz.id, {
            select: (value) => ({
              value: value.value,
              count: value.count,
            }),
            equalityFn: () => true,
          }),
        ),
      {
        account,
        wrapper,
      },
    );

    // Wait for initial load
    await waitFor(() => {
      expect(result.current.result).toBeDefined();
    });

    const initialRenderCount = result.current.renderCount;
    const initialValue = result.current.result.value;
    const initialCount = result.current.result.count;

    // Update both fields multiple times
    for (let i = 1; i <= 10; i++) {
      act(() => {
        map.$jazz.set("value", `updated-${i}`);
        map.$jazz.set("count", 10 + i);
      });
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // Verify no re-renders occurred (equality always returns true)
    expect(result.current.renderCount).toBe(initialRenderCount);
    // Result should still show initial values since no re-render occurred
    expect(result.current.result.value).toBe(initialValue);
    expect(result.current.result.count).toBe(initialCount);
  });

  it("should trigger re-render when equality returns false", async () => {
    const TestMap = co.map({
      value: z.string(),
      count: z.number(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "initial",
      count: 0,
    });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    // Equality function that compares both value and count
    const { result } = renderHook(
      () =>
        useRenderCount(() =>
          useSuspenseCoState(TestMap, map.$jazz.id, {
            select: (value) => ({
              value: value.value,
              count: value.count,
            }),
            equalityFn: (a, b) => a.value === b.value && a.count === b.count,
          }),
        ),
      {
        account,
        wrapper,
      },
    );

    // Wait for initial load
    await waitFor(() => {
      expect(result.current.result).toBeDefined();
    });

    const initialRenderCount = result.current.renderCount;

    // Update count field (equality should return false)
    act(() => {
      map.$jazz.set("count", 100);
    });

    // Verify re-render occurred
    await waitFor(() => {
      expect(result.current.renderCount).toBe(initialRenderCount + 1);
      expect(result.current.result.count).toBe(100);
    });
  });

  it("should work with branches - create, edit, and merge", async () => {
    const Person = co.map({
      name: z.string(),
      age: z.number(),
      email: z.string(),
    });

    const group = Group.create();
    group.addMember("everyone", "writer");

    const originalPerson = Person.create(
      {
        name: "John Doe",
        age: 30,
        email: "john@example.com",
      },
      group,
    );

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    // Render useSuspenseCoState twice: once for branch, once for main
    const { result } = await act(async () => {
      return renderHook(
        () => {
          const branch = useSuspenseCoState(Person, originalPerson.$jazz.id, {
            unstable_branch: { name: "feature-branch" },
          });

          const main = useSuspenseCoState(Person, originalPerson.$jazz.id);

          return { branch, main };
        },
        {
          account,
          wrapper,
        },
      );
    });

    // Wait for both to load
    await waitFor(() => {
      expect(result.current).not.toBeNull();
      expect(result.current.branch).toBeDefined();
      expect(result.current.main).toBeDefined();
    });

    // Verify both return loaded data
    assertLoaded(result.current.branch);
    assertLoaded(result.current.main);

    expect(result.current.branch.name).toBe("John Doe");
    expect(result.current.branch.age).toBe(30);
    expect(result.current.branch.email).toBe("john@example.com");

    expect(result.current.main.name).toBe("John Doe");
    expect(result.current.main.age).toBe(30);
    expect(result.current.main.email).toBe("john@example.com");

    // Use act() to modify branch CoValue
    act(() => {
      result.current.branch.$jazz.applyDiff({
        name: "John Smith",
        age: 31,
        email: "john.smith@example.com",
      });
    });

    // Wait for updates
    await waitFor(() => {
      expect(result.current.branch.name).toBe("John Smith");
    });

    // Verify branch has changes
    expect(result.current.branch.name).toBe("John Smith");
    expect(result.current.branch.age).toBe(31);
    expect(result.current.branch.email).toBe("john.smith@example.com");

    // Verify main is unchanged
    expect(result.current.main.name).toBe("John Doe");
    expect(result.current.main.age).toBe(30);
    expect(result.current.main.email).toBe("john@example.com");

    // Merge branch
    await act(async () => {
      await result.current.branch.$jazz.unstable_merge();
    });

    // Wait for merge to propagate
    await waitFor(() => {
      expect(result.current.main.name).toBe("John Smith");
    });

    // Verify main now has the changes
    expect(result.current.main.name).toBe("John Smith");
    expect(result.current.main.age).toBe(31);
    expect(result.current.main.email).toBe("john.smith@example.com");
  });

  it("should preload value when provided", async () => {
    disableJazzTestSync();

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      email: z.string(),
    });

    const group = Group.create();
    group.addMember("everyone", "writer");

    const originalPerson = Person.create(
      {
        name: "John Doe",
        age: 30,
        email: "john@example.com",
      },
      group,
    );

    // Create a test account (different from creator)
    const bob = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    // Export the CoValue
    const exportedPerson = originalPerson.$jazz.export();

    // Track render count
    let renderCount = 0;
    let suspenseTriggered = false;

    const SuspenseFallback = () => {
      suspenseTriggered = true;
      return <div>Loading...</div>;
    };

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<SuspenseFallback />}>{children}</Suspense>
    );

    // Render useSuspenseCoState with preloaded data
    const { result } = renderHook(
      () => {
        renderCount++;
        return useSuspenseCoState(Person, originalPerson.$jazz.id, {
          preloaded: exportedPerson,
        });
      },
      {
        account: bob,
        wrapper,
      },
    );

    // Wait for any async operations
    await waitFor(() => {
      expect(result.current).toBeDefined();
    });

    // Verify only one render occurred
    expect(renderCount).toBe(1);

    // Verify Suspense was not triggered (preloaded data enables immediate rendering)
    expect(suspenseTriggered).toBe(false);

    // Verify data is immediately accessible
    assertLoaded(result.current);
    expect(result.current.name).toBe("John Doe");
    expect(result.current.age).toBe(30);
    expect(result.current.email).toBe("john@example.com");
  });
});
