import { AgentSecret } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { beforeEach, describe, expect, test, vi } from "vitest";
import {
  Account,
  AnonymousJazzAgent,
  AuthSecretStorage,
  Credentials,
  ID,
  InMemoryKVStore,
  KvStoreContext,
  co,
  coValueClassFromCoValueClassOrSchema,
  createAnonymousJazzContext,
  createJazzContext,
  createJazzContextForNewAccount,
  createJazzContextFromExistingCredentials,
  randomSessionProvider,
} from "../exports";
import { activeAccountContext } from "../implementation/activeAccountContext";
import {
  createJazzTestAccount,
  getPeerConnectedToTestSyncServer,
  setupJazzTestSync,
} from "../testing";
import { loadCoValueOrFail } from "./utils";
const Crypto = await WasmCrypto.create();

KvStoreContext.getInstance().initialize(new InMemoryKVStore());

describe("createContext methods", () => {
  let authSecretStorage: AuthSecretStorage;

  beforeEach(async () => {
    authSecretStorage = new AuthSecretStorage();
    authSecretStorage.clear();
    await setupJazzTestSync();
  });

  describe("createJazzContextFromExistingCredentials", () => {
    test("creates context with valid credentials", async () => {
      // Create an account first to get valid credentials
      const account = await createJazzTestAccount({
        isCurrentActiveAccount: true,
      });

      const credentials: Credentials = {
        accountID: account.$jazz.id,
        secret: account.$jazz.localNode.getCurrentAgent().agentSecret,
      };

      const context = await createJazzContextFromExistingCredentials({
        credentials,
        peers: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
        sessionProvider: randomSessionProvider,
        asActiveAccount: true,
      });

      expect(context.node).toBeDefined();
      expect(context.account).toBeDefined();
      expect(context.account.$jazz.id).toBe(credentials.accountID);
      expect(typeof context.done).toBe("function");
      expect(typeof context.logOut).toBe("function");
    });

    test("handles custom account schema", async () => {
      const CustomAccount = co
        .account({
          root: co.map({}),
          profile: co.profile(),
        })
        .withMigration(async () => {});

      const account = await createJazzTestAccount({
        isCurrentActiveAccount: true,
      });

      const credentials: Credentials = {
        accountID: account.$jazz.id,
        secret: account.$jazz.localNode.getCurrentAgent().agentSecret,
      };

      const context = await createJazzContextFromExistingCredentials({
        credentials,
        peers: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
        AccountSchema: CustomAccount,
        sessionProvider: randomSessionProvider,
        asActiveAccount: true,
      });

      expect(context.account).toBeInstanceOf(
        coValueClassFromCoValueClassOrSchema(CustomAccount),
      );
    });

    test("calls onLogOut callback when logging out", async () => {
      const account = await createJazzTestAccount({
        isCurrentActiveAccount: true,
      });

      const onLogOut = vi.fn();

      const context = await createJazzContextFromExistingCredentials({
        credentials: {
          accountID: account.$jazz.id,
          secret: account.$jazz.localNode.getCurrentAgent().agentSecret,
        },
        peers: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
        sessionProvider: randomSessionProvider,
        onLogOut,
        asActiveAccount: true,
      });

      context.logOut();
      expect(onLogOut).toHaveBeenCalled();
    });

    test("connects to provided peers", async () => {
      const account = await createJazzTestAccount({
        isCurrentActiveAccount: true,
      });

      const coMap = account.$jazz.raw.createMap();
      coMap.set("test", "test", "trusting");

      const context = await createJazzContextFromExistingCredentials({
        credentials: {
          accountID: account.$jazz.id,
          secret: account.$jazz.localNode.getCurrentAgent().agentSecret,
        },
        peers: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
        sessionProvider: randomSessionProvider,
        asActiveAccount: true,
      });

      const loadedMap = await loadCoValueOrFail(context.node, coMap.id);

      expect(loadedMap.get("test")).toBe("test");
    });

    test("sets the active account", async () => {
      const account = await createJazzTestAccount({
        isCurrentActiveAccount: true,
      });

      const context = await createJazzContextFromExistingCredentials({
        credentials: {
          accountID: account.$jazz.id,
          secret: account.$jazz.localNode.getCurrentAgent().agentSecret,
        },
        peers: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
        sessionProvider: randomSessionProvider,
        asActiveAccount: true,
      });

      expect(activeAccountContext.get()).toBe(context.account);
    });

    test("does not set the active account when asActiveAccount is false", async () => {
      const account = await createJazzTestAccount({
        isCurrentActiveAccount: false,
      });

      const context = await createJazzContextFromExistingCredentials({
        credentials: {
          accountID: account.$jazz.id,
          secret: account.$jazz.localNode.getCurrentAgent().agentSecret,
        },
        peers: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
        sessionProvider: randomSessionProvider,
        asActiveAccount: false,
      });

      expect(activeAccountContext.get()).not.toBe(context.account);
    });
  });

  describe("createJazzContextForNewAccount", () => {
    test("creates new account with provided props", async () => {
      const context = await createJazzContextForNewAccount({
        creationProps: { name: "New User" },
        peers: [],
        crypto: Crypto,
      });

      expect(context.account).toBeDefined();
      expect(context.account.profile?.name).toBe("New User");
    });

    test("uses initial agent secret when provided", async () => {
      const initialSecret = Crypto.newRandomAgentSecret();

      const context = await createJazzContextForNewAccount({
        creationProps: { name: "New User" },
        initialAgentSecret: initialSecret,
        peers: [],
        crypto: Crypto,
      });

      expect(context.node.getCurrentAgent().agentSecret).toBe(initialSecret);
    });

    test("handles custom account schema", async () => {
      const CustomAccount = co
        .account({
          root: co.map({}),
          profile: co.profile(),
        })
        .withMigration(async () => {});

      const context = await createJazzContextForNewAccount({
        creationProps: { name: "New User" },
        peers: [],
        crypto: Crypto,
        AccountSchema: CustomAccount,
      });

      expect(context.account).toBeInstanceOf(
        coValueClassFromCoValueClassOrSchema(CustomAccount),
      );
    });

    test("sets the active account to the new account", async () => {
      const context = await createJazzContextForNewAccount({
        creationProps: { name: "New User" },
        peers: [],
        crypto: Crypto,
      });
      expect(activeAccountContext.get()).toBe(context.account);
    });
  });

  describe("createAnonymousJazzContext", () => {
    test("creates anonymous context", async () => {
      const context = await createAnonymousJazzContext({
        peers: [],
        crypto: Crypto,
      });

      expect(context.agent).toBeInstanceOf(AnonymousJazzAgent);
      expect(typeof context.done).toBe("function");
      expect(typeof context.logOut).toBe("function");
    });

    test("connects to provided peers", async () => {
      const account = await createJazzTestAccount({
        isCurrentActiveAccount: true,
      });

      const coMap = account.$jazz.raw.createMap();
      coMap.set("test", "test", "trusting");

      const context = await createAnonymousJazzContext({
        peers: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
      });

      const loadedMap = await loadCoValueOrFail(context.agent.node, coMap.id);

      expect(loadedMap.get("test")).toBe("test");
    });

    test("sets the guest mode", async () => {
      await createAnonymousJazzContext({
        peers: [],
        crypto: Crypto,
      });

      expect(() => activeAccountContext.get()).toThrow(
        "Something that expects a full active account was called in guest mode.",
      );
    });
  });

  describe("createJazzContext", () => {
    test("creates new account when no credentials exist", async () => {
      const context = await createJazzContext({
        peers: [],
        crypto: Crypto,
        authSecretStorage,
        sessionProvider: randomSessionProvider,
      });

      expect(context.account).toBeDefined();
      expect(context.authSecretStorage).toBe(authSecretStorage);
    });

    test("uses existing credentials when available", async () => {
      // First create an account and store credentials
      const initialContext = await createJazzContext({
        peers: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
        authSecretStorage,
        sessionProvider: randomSessionProvider,
      });

      // Create new context with same storage
      const newContext = await createJazzContext({
        peers: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
        authSecretStorage,
        sessionProvider: randomSessionProvider,
      });

      expect(newContext.account.$jazz.id).toBe(initialContext.account.$jazz.id);
    });

    test("uses provided new account props", async () => {
      const context = await createJazzContext({
        newAccountProps: {
          creationProps: { name: "Custom User" },
        },
        peers: [],
        crypto: Crypto,
        authSecretStorage,
        sessionProvider: randomSessionProvider,
      });

      expect(context.account.profile?.name).toBe("Custom User");
    });

    test("uses initial agent secret when provided", async () => {
      const initialSecret = Crypto.newRandomAgentSecret();

      const storage = new AuthSecretStorage();

      await storage.set({
        accountID: "test" as ID<Account>,
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret: "secret123" as AgentSecret,
        provider: "passkey",
      });

      const context = await createJazzContext({
        newAccountProps: {
          secret: initialSecret,
        },
        peers: [],
        crypto: Crypto,
        authSecretStorage,
        sessionProvider: randomSessionProvider,
      });

      expect(context.node.getCurrentAgent().agentSecret).toBe(initialSecret);
      expect(await storage.get()).toEqual({
        accountID: "test" as ID<Account>,
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret: "secret123" as AgentSecret,
        provider: "passkey",
      });
    });

    test("handles custom account schema", async () => {
      const CustomAccount = co
        .account({
          root: co.map({}),
          profile: co.profile(),
        })
        .withMigration(async () => {});

      const context = await createJazzContext({
        peers: [],
        crypto: Crypto,
        authSecretStorage,
        sessionProvider: randomSessionProvider,
        AccountSchema: CustomAccount,
      });

      expect(context.account).toBeInstanceOf(
        coValueClassFromCoValueClassOrSchema(CustomAccount),
      );
    });
  });
});
