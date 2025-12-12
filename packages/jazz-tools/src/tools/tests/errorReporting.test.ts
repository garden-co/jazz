import { beforeEach, describe, expect, test, vi } from "vitest";
import { z } from "../index.js";
import { setCustomErrorReporter } from "../config.js";
import { CoValueLoadingState, co } from "../internal.js";
import { createJazzTestAccount, linkAccounts } from "../testing.js";

const TestMap = co.map({
  value: z.string(),
});

describe("Custom error reporter", () => {
  beforeEach(() => {
    // Clean up error reporter before each test
    setCustomErrorReporter(undefined);
  });

  test("with custom error reporter enabled, console.error is not called", async () => {
    const bob = await createJazzTestAccount({
      creationProps: { name: "Bob" },
    });

    const alice = await createJazzTestAccount({
      creationProps: { name: "Alice" },
    });

    linkAccounts(bob, alice);

    await alice.$jazz.waitForAllCoValuesSync();

    const onlyBob = bob;
    const group = co.group().create(bob);

    group.addMember(alice, "reader");

    let capturedError: Error | undefined;
    let capturedProps: { jazzError: any } | undefined;

    setCustomErrorReporter((error, props) => {
      capturedError = error;
      capturedProps = props;
    });

    const consoleErrorSpy = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});

    const map = TestMap.create({ value: "hello" }, onlyBob);

    const mapOnAlice = await TestMap.load(map.$jazz.id, { loadAs: alice });

    // Access the value to trigger error logging
    expect(mapOnAlice.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );

    expect(consoleErrorSpy).not.toHaveBeenCalled();
    expect(capturedError).toBeDefined();
    expect(capturedProps).toBeDefined();
    expect(capturedProps?.jazzError).toBeDefined();

    consoleErrorSpy.mockRestore();
  });

  test("without custom error reporter, console.error is called", async () => {
    const bob = await createJazzTestAccount({
      creationProps: { name: "Bob" },
    });

    const alice = await createJazzTestAccount({
      creationProps: { name: "Alice" },
    });

    linkAccounts(bob, alice);

    await alice.$jazz.waitForAllCoValuesSync();

    const onlyBob = bob;
    const group = co.group().create(bob);

    group.addMember(alice, "reader");

    // Ensure no custom error reporter is set
    setCustomErrorReporter(undefined);

    const consoleErrorSpy = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});

    const map = TestMap.create({ value: "hello" }, onlyBob);

    const mapOnAlice = await TestMap.load(map.$jazz.id, { loadAs: alice });

    // Access the value to trigger error logging
    expect(mapOnAlice.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );

    expect(consoleErrorSpy).toHaveBeenCalled();

    consoleErrorSpy.mockRestore();
  });
});
