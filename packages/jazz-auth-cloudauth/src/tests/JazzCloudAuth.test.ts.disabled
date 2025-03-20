// @vitest-environment happy-dom

import {
  Account,
  AuthSecretStorage,
  InMemoryKVStore,
  KvStoreContext,
} from "jazz-tools";
import {
  TestJSCrypto,
  TestJazzContextManager,
  createJazzTestAccount,
  createJazzTestGuest,
  setupJazzTestSync,
} from "jazz-tools/testing";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { CloudAuth, newAuthClient } from "..";

const crypto = await TestJSCrypto.create();
KvStoreContext.getInstance().initialize(new InMemoryKVStore());
const mockAuthenticate = vi.fn();
const baseUrl = "http://localhost:3000";
const keyserver = "http://localhost:6189";
const authClient = newAuthClient(baseUrl);
let auth: CloudAuth;
let account: Account;
let contextManager: TestJazzContextManager<any>;
let authSecretStorage: AuthSecretStorage;

beforeEach(async () => {
  await setupJazzTestSync();
  account = await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
  contextManager = TestJazzContextManager.fromAccountOrGuest(account);
  authSecretStorage = contextManager.getAuthSecretStorage();
  await contextManager.createContext({});
  auth = new CloudAuth(
    contextManager.authenticate,
    authSecretStorage,
    authClient,
    keyserver,
    crypto,
  );
});

describe("JazzCloudAuth", () => {
  describe("Sign-in", () => {
    it("should sign up successfully with email & password", async () => {
      let me = await Account.getMe().ensureLoaded({
        profile: {},
      });

      let credentials = await authSecretStorage.get();
      expect(credentials?.accountID).toBe(me.id);
      expect(credentials?.provider).toBe("anonymous");

      await authClient.signUp.email({
        email: "a@a.com",
        password: "12345678",
        name: "UserA",
        accountID: "",
        accountSecret: "",
      });
      let session = (await authClient.getSession()).data;
      if (session) await auth.signIn(session);
      session = (await authClient.getSession()).data;
      if (session) await auth.onUserChange(session);

      me = await Account.getMe().ensureLoaded({
        profile: {},
      });
      credentials = await authSecretStorage.get();

      expect(credentials?.accountID).toBe(me.id);
      expect(credentials?.provider).toBe("cloudauth");
      expect(me.profile.name).toBe("UserA");

      await authClient.deleteUser();
    });
    it("should login successfully with email & password", async () => {
      // Sign up with email & password
      await authClient.signUp.email({
        email: "a@a.com",
        password: "12345678",
        name: "UserA",
        accountID: "",
        accountSecret: "",
      });
      let session = (await authClient.getSession()).data;
      if (session) await auth.signIn(session);
      session = (await authClient.getSession()).data;
      if (session) await auth.onUserChange(session);

      // Logout
      await authClient.signOut();
      await contextManager.logOut();

      // Check user is logged out
      let me = await Account.getMe().ensureLoaded({
        profile: {},
      });
      let credentials = await authSecretStorage.get();
      expect(credentials?.accountID).toBe(me.id);
      expect(credentials?.provider).toBe("anonymous");

      // Login
      await authClient.signIn.email({
        email: "a@a.com",
        password: "12345678",
      });
      session = (await authClient.getSession()).data;
      if (session) await auth.logIn(session);
      session = (await authClient.getSession()).data;
      if (session) await auth.onUserChange(session);

      // Check user is logged in
      me = await Account.getMe().ensureLoaded({
        profile: {},
      });
      credentials = await authSecretStorage.get();
      expect(credentials?.accountID).toBe(me.id);
      expect(credentials?.provider).toBe("cloudauth");
      expect(me.profile.name).toBe("UserA");

      await authClient.deleteUser();
    });
  });
  describe("Key sharding", () => {
    it("should return the pre-split keys after merging", () => {
      const accountSecret = crypto.newRandomAgentSecret();
      const [signer0, signer1] = CloudAuth.splitSignerSecret(
        accountSecret,
        crypto,
      );
      const [sealer0, sealer1] = CloudAuth.splitSealerSecret(
        accountSecret,
        crypto,
      );
      const signerSecret = CloudAuth.mergeSignerSecret(
        signer0,
        signer1,
        crypto,
      );
      const sealerSecret = CloudAuth.mergeSealerSecret(
        sealer0,
        sealer1,
        crypto,
      );
      expect(signerSecret).toEqual(crypto.getAgentSignerSecret(accountSecret));
      expect(sealerSecret).toEqual(crypto.getAgentSealerSecret(accountSecret));
    });
  });
});
