import {
  CryptoProvider,
  KvStoreContext,
  SessionID,
  SessionProvider,
} from "jazz-tools";
import { AgentID, RawAccountID } from "cojson";

const lockedSessions = new Set<SessionID>();

export class ReactNativeSessionProvider implements SessionProvider {
  async acquireSession(
    accountID: string,
    crypto: CryptoProvider,
  ): Promise<{ sessionID: SessionID; sessionDone: () => void }> {
    const kvStore = KvStoreContext.getInstance().getStorage();
    const existingSession = await kvStore.get(accountID as string);

    // Check if the session is already in use, should happen only if the dev
    // mounts multiple providers at the same time
    if (lockedSessions.has(existingSession as SessionID)) {
      const newSessionID = crypto.newRandomSessionID(
        accountID as RawAccountID | AgentID,
      );

      console.error("Existing session in use, creating new one", newSessionID);

      return Promise.resolve({
        sessionID: newSessionID,
        sessionDone: () => {},
      });
    }

    if (existingSession) {
      console.log("Using existing session", existingSession);
      lockedSessions.add(existingSession as SessionID);
      return Promise.resolve({
        sessionID: existingSession as SessionID,
        sessionDone: () => {
          lockedSessions.delete(existingSession as SessionID);
        },
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
    lockedSessions.add(newSessionID);

    console.error("Created new session", newSessionID);

    return Promise.resolve({
      sessionID: newSessionID,
      sessionDone: () => {
        lockedSessions.delete(newSessionID);
      },
    });
  }

  async persistSession(
    accountID: string,
    sessionID: SessionID,
  ): Promise<{ sessionDone: () => void }> {
    const kvStore = KvStoreContext.getInstance().getStorage();
    await kvStore.set(accountID, sessionID);
    lockedSessions.add(sessionID);
    return Promise.resolve({
      sessionDone: () => {
        lockedSessions.delete(sessionID);
      },
    });
  }
}
