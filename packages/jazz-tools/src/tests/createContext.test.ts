import { AgentSecret, RawCoMap } from "cojson";
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
  createAnonymousJazzContext,
  createJazzContext,
  createJazzContextForNewAccount,
  createJazzContextFromExistingCredentials,
  randomSessionProvider,
  z,
  zodSchemaToCoSchema,
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
        accountID: account.id,
        secret: account._raw.core.node.getCurrentAgent().agentSecret,
      };

      const context = await createJazzContextFromExistingCredentials({
        credentials,
        peersToLoadFrom: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
        sessionProvider: randomSessionProvider,
      });

      expect(context.node).toBeDefined();
      expect(context.account).toBeDefined();
      expect(context.account.id).toBe(credentials.accountID);
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
        accountID: account.id,
        secret: account._raw.core.node.getCurrentAgent().agentSecret,
      };

      const context = await createJazzContextFromExistingCredentials({
        credentials,
        peersToLoadFrom: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
        AccountSchema: zodSchemaToCoSchema(CustomAccount),
        sessionProvider: randomSessionProvider,
      });

      expect(context.account).toBeInstanceOf(
        zodSchemaToCoSchema(CustomAccount),
      );
    });

    test("calls onLogOut callback when logging out", async () => {
      const account = await createJazzTestAccount({
        isCurrentActiveAccount: true,
      });

      const onLogOut = vi.fn();

      const context = await createJazzContextFromExistingCredentials({
        credentials: {
          accountID: account.id,
          secret: account._raw.core.node.getCurrentAgent().agentSecret,
        },
        peersToLoadFrom: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
        sessionProvider: randomSessionProvider,
        onLogOut,
      });

      context.logOut();
      expect(onLogOut).toHaveBeenCalled();
    });

    test("connects to provided peers", async () => {
      const account = await createJazzTestAccount({
        isCurrentActiveAccount: true,
      });

      const coMap = account._raw.createMap();
      coMap.set("test", "test", "trusting");

      const context = await createJazzContextFromExistingCredentials({
        credentials: {
          accountID: account.id,
          secret: account._raw.core.node.getCurrentAgent().agentSecret,
        },
        peersToLoadFrom: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
        sessionProvider: randomSessionProvider,
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
          accountID: account.id,
          secret: account._raw.core.node.getCurrentAgent().agentSecret,
        },
        peersToLoadFrom: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
        sessionProvider: randomSessionProvider,
      });

      expect(activeAccountContext.get()).toBe(context.account);
    });
  });

  describe("createJazzContextForNewAccount", () => {
    test("creates new account with provided props", async () => {
      const context = await createJazzContextForNewAccount({
        creationProps: { name: "New User" },
        peersToLoadFrom: [],
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
        peersToLoadFrom: [],
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
        peersToLoadFrom: [],
        crypto: Crypto,
        AccountSchema: zodSchemaToCoSchema(CustomAccount),
      });

      expect(context.account).toBeInstanceOf(
        zodSchemaToCoSchema(CustomAccount),
      );
    });

    test("sets the active account to the new account", async () => {
      const context = await createJazzContextForNewAccount({
        creationProps: { name: "New User" },
        peersToLoadFrom: [],
        crypto: Crypto,
      });
      expect(activeAccountContext.get()).toBe(context.account);
    });
  });

  describe("createAnonymousJazzContext", () => {
    test("creates anonymous context", async () => {
      const context = await createAnonymousJazzContext({
        peersToLoadFrom: [],
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

      const coMap = account._raw.createMap();
      coMap.set("test", "test", "trusting");

      const context = await createAnonymousJazzContext({
        peersToLoadFrom: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
      });

      const loadedMap = await loadCoValueOrFail(context.agent.node, coMap.id);

      expect(loadedMap.get("test")).toBe("test");
    });

    test("sets the guest mode", async () => {
      await createAnonymousJazzContext({
        peersToLoadFrom: [],
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
        peersToLoadFrom: [],
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
        peersToLoadFrom: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
        authSecretStorage,
        sessionProvider: randomSessionProvider,
      });

      // Create new context with same storage
      const newContext = await createJazzContext({
        peersToLoadFrom: [getPeerConnectedToTestSyncServer()],
        crypto: Crypto,
        authSecretStorage,
        sessionProvider: randomSessionProvider,
      });

      expect(newContext.account.id).toBe(initialContext.account.id);
    });

    test("uses provided new account props", async () => {
      const context = await createJazzContext({
        newAccountProps: {
          creationProps: { name: "Custom User" },
        },
        peersToLoadFrom: [],
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
        peersToLoadFrom: [],
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
        peersToLoadFrom: [],
        crypto: Crypto,
        authSecretStorage,
        sessionProvider: randomSessionProvider,
        AccountSchema: zodSchemaToCoSchema(CustomAccount),
      });

      expect(context.account).toBeInstanceOf(
        zodSchemaToCoSchema(CustomAccount),
      );
    });
  });
});
