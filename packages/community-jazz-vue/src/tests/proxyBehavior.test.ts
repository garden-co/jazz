// @vitest-environment happy-dom

import { Group, co, z } from "jazz-tools";
import { beforeAll, describe, expect, it } from "vitest";
import { isProxy, nextTick, toRaw } from "vue";
import { useAccount, useCoState } from "../composables.js";
import { createJazzTestAccount } from "../testing.js";
import { withJazzTestSetup } from "./testUtils.js";

const TestMap = co.map({
  content: z.string(),
  get nested() {
    return co.optional(TestMap);
  },
});

const AccountRoot = co.map({
  value: z.string(),
  testMap: co.optional(TestMap),
});

const AccountProfile = co.map({
  name: z.string(),
});

const AccountSchema = co
  .account({
    root: AccountRoot,
    profile: AccountProfile,
  })
  .withMigration((account) => {
    if (!account.root) {
      account.root = AccountRoot.create({ value: "test" }, { owner: account });
    }
    if (!account.profile) {
      // Profile must be owned by a Group, not the account itself
      const group = Group.create();
      account.profile = AccountProfile.create(
        { name: "Test User" },
        { owner: group },
      );
    }
  });

describe("Proxy Behavior Verification", () => {
  // Create a single account for all tests to avoid parallel account creation
  let sharedAccount: any;
  let sharedAccountWithSchema: any;

  beforeAll(async () => {
    try {
      sharedAccount = await createJazzTestAccount();
      sharedAccountWithSchema = await createJazzTestAccount({ AccountSchema });
    } catch (error) {
      console.warn("Failed to create test account:", error);
    }
  });

  it("should not expose Vue proxies to Jazz objects in useCoState", async () => {
    const testMap = TestMap.create(
      {
        content: "test content",
      },
      { owner: sharedAccount },
    );

    const [result] = withJazzTestSetup(() => useCoState(TestMap, testMap.id), {
      account: sharedAccount,
    });

    // The returned value should not be a Vue proxy
    expect(isProxy(result.value)).toBe(false);

    // Properties should also not be proxies
    if (result.value) {
      expect(isProxy(result.value.content)).toBe(false);
    }
  });

  it("should handle nested object access without proxy issues", async () => {
    const nestedMap = TestMap.create(
      {
        content: "nested content",
      },
      { owner: sharedAccountWithSchema },
    );

    const rootMap = AccountRoot.create(
      {
        value: "root value",
        testMap: nestedMap,
      },
      { owner: sharedAccountWithSchema },
    );

    // Update account root
    sharedAccountWithSchema.root = rootMap;

    const [accountResult] = withJazzTestSetup(
      () => useAccount(AccountSchema, { resolve: { root: { testMap: true } } }),
      { account: sharedAccountWithSchema },
    );

    await nextTick();

    // Should be able to access deeply nested properties without proxy issues
    const me = accountResult.me.value;
    expect(me).toBeDefined();
    expect(me?.root).toBeDefined();
    expect(me?.root?.testMap).toBeDefined();
    expect(me?.root?.testMap?.content).toBe("nested content");

    // None of these should be Vue proxies
    expect(isProxy(me)).toBe(false);
    expect(isProxy(me?.root)).toBe(false);
    expect(isProxy(me?.root?.testMap)).toBe(false);
  });

  it("should handle reactivity updates without breaking Jazz object integrity", async () => {
    const account = await createJazzTestAccount();

    const testMap = TestMap.create(
      {
        content: "initial content",
      },
      { owner: account },
    );

    const [result] = withJazzTestSetup(() => useCoState(TestMap, testMap.id), {
      account,
    });

    // Initial state
    expect(result.value?.content).toBe("initial content");
    expect(isProxy(result.value)).toBe(false);

    // Update the Jazz object
    testMap.content = "updated content";
    await nextTick();

    // Should reactively update
    expect(result.value?.content).toBe("updated content");

    // Should still not be a proxy after update
    expect(isProxy(result.value)).toBe(false);
  });

  it("should not require toRaw() calls in user code", async () => {
    const account = await createJazzTestAccount();

    const testMap = TestMap.create(
      {
        content: "test content",
      },
      { owner: account },
    );

    const [result] = withJazzTestSetup(() => useCoState(TestMap, testMap.id), {
      account,
    });

    // User should be able to use the object directly without toRaw()
    const jazzObject = result.value;
    expect(jazzObject).toBeDefined();

    // Should be able to call Jazz methods directly
    expect(() => {
      jazzObject?.content; // Access property
      // In a real scenario, user might call other Jazz methods here
    }).not.toThrow();

    // Should not need toRaw() to access properties
    expect(jazzObject?.content).toBe("test content");
  });

  it("should handle context manager objects without proxy issues", async () => {
    const account = await createJazzTestAccount();

    const [result] = withJazzTestSetup(() => useAccount(), {
      account,
    });

    // The account object should not be a proxy
    expect(isProxy(result.me.value)).toBe(false);

    // The agent should be accessible and usable without toRaw() calls
    expect(result.agent).toBeDefined();
    expect(typeof result.agent).toBe("object");

    // Should be able to access agent properties without proxy issues
    expect(() => {
      const agentType = result.agent._type; // Access agent property
      expect(agentType).toBeDefined();
    }).not.toThrow();

    // LogOut function should work without proxy issues
    expect(typeof result.logOut).toBe("function");
    expect(() => {
      // Should be able to call without toRaw()
      result.logOut.toString(); // Safe way to test function access
    }).not.toThrow();
  });

  it("should maintain object identity across reactive updates", async () => {
    const account = await createJazzTestAccount();

    const testMap = TestMap.create(
      {
        content: "initial",
      },
      { owner: account },
    );

    const [result] = withJazzTestSetup(() => useCoState(TestMap, testMap.id), {
      account,
    });

    const initialObject = result.value;
    const initialId = initialObject?.id;

    // Update content
    testMap.content = "updated";
    await nextTick();

    const updatedObject = result.value;

    // Object identity should be maintained (same Jazz object)
    expect(updatedObject?.id).toBe(initialId);
    expect(isProxy(updatedObject)).toBe(false);
  });

  it("should handle sign-up and project creation scenarios without refresh", async () => {
    // This test simulates the scenario mentioned in memories where proxy issues
    // recur after sign-up and project creation without refresh

    const account = await createJazzTestAccount({ AccountSchema });

    const [accountResult] = withJazzTestSetup(() => useAccount(AccountSchema), {
      account,
    });

    // Simulate project creation
    const newProject = TestMap.create(
      {
        content: "new project",
      },
      { owner: account },
    );

    const [projectResult] = withJazzTestSetup(
      () => useCoState(TestMap, newProject.id),
      {
        account,
      },
    );

    await nextTick();

    // Both account and project should work without proxy issues
    expect(isProxy(accountResult.me.value)).toBe(false);
    expect(isProxy(projectResult.value)).toBe(false);

    // Should be able to access properties without toRaw()
    expect(accountResult.me.value?.id).toBeDefined();
    expect(projectResult.value?.content).toBe("new project");
  });
});
