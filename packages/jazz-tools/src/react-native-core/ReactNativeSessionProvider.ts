import {
  CryptoProvider,
  KvStoreContext,
  SessionID,
  SessionProvider,
} from "jazz-tools";
import { AgentID, RawAccountID } from "cojson";

export class ReactNativeSessionProvider implements SessionProvider {
  async acquireSession(
    accountID: string,
    crypto: CryptoProvider,
  ): Promise<{ sessionID: SessionID; sessionDone: () => void }> {
    const kvStore = KvStoreContext.getInstance().getStorage();
    const existingSession = await kvStore.get(accountID as string);

    if (existingSession) {
      console.log("Using existing session", existingSession);
      return Promise.resolve({
        sessionID: existingSession as SessionID,
        sessionDone: () => {},
      });
    }

    // We need to provide this for backwards compatibility with the old session provider
    // With the current session provider we should never get here because:
    // - New accounts provide their session and go through the persistSession method
    // - Existing accounts should already have a session
    const newSessionID = crypto.newRandomSessionID(
      accountID as RawAccountID | AgentID,
    );
    await kvStore.set(accountID, newSessionID);

    console.error("Created new session", newSessionID);

    return Promise.resolve({
      sessionID: newSessionID,
      sessionDone: () => {},
    });
  }

  async persistSession(
    accountID: string,
    sessionID: SessionID,
  ): Promise<{ sessionDone: () => void }> {
    const kvStore = KvStoreContext.getInstance().getStorage();
    await kvStore.set(accountID, sessionID);
    return Promise.resolve({
      sessionDone: () => {},
    });
  }
}
