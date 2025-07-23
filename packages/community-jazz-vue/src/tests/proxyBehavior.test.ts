// @vitest-environment happy-dom

import { Account, CoMap, coField } from "jazz-tools";
import { describe, expect, it } from "vitest";
import { isProxy, nextTick, toRaw } from "vue";
import { useAccount, useCoState } from "../composables.js";
import { createJazzTestAccount } from "../testing.js";
import { withJazzTestSetup } from "./testUtils.js";

class TestMap extends CoMap {
  content = coField.string;
  nested = coField.ref(TestMap, { optional: true });
}

class AccountRoot extends CoMap {
  value = coField.string;
  testMap = coField.ref(TestMap, { optional: true });
}

class AccountSchema extends Account {
  root = coField.ref(AccountRoot);

  migrate() {
    if (!this._refs.root) {
      this.root = AccountRoot.create({ value: "test" }, { owner: this });
    }
  }
}

describe("Proxy Behavior Verification", () => {
  it("should not expose Vue proxies to Jazz objects in useAccount", async () => {
    const account = await createJazzTestAccount({ AccountSchema });

    const [result] = withJazzTestSetup(() => useAccount(AccountSchema), {
      account,
    });

    // The returned me should not be a Vue proxy
    expect(isProxy(result.me.value)).toBe(false);

    // Even if we access nested properties, they shouldn't be proxies
    if (result.me.value?.root) {
      expect(isProxy(result.me.value.root)).toBe(false);
    }
  });

  it("should not expose Vue proxies to Jazz objects in useCoState", async () => {
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

    // The returned value should not be a Vue proxy
    expect(isProxy(result.value)).toBe(false);

    // Properties should also not be proxies
    if (result.value) {
      expect(isProxy(result.value.content)).toBe(false);
    }
  });

  it("should handle nested object access without proxy issues", async () => {
    const account = await createJazzTestAccount({ AccountSchema });

    const nestedMap = TestMap.create(
      {
        content: "nested content",
      },
      { owner: account },
    );

    const rootMap = AccountRoot.create(
      {
        value: "root value",
        testMap: nestedMap,
      },
      { owner: account },
    );

    // Update account root
    account.root = rootMap;

    const [accountResult] = withJazzTestSetup(
      () => useAccount(AccountSchema, { resolve: { root: { testMap: true } } }),
      { account },
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
