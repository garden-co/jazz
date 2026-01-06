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

    if (!existingSession) {
      const newSessionID = crypto.newRandomSessionID(
        accountID as RawAccountID | AgentID,
      );
      await kvStore.set(accountID, newSessionID);
      lockedSessions.add(newSessionID);

      console.log("Created new session", newSessionID);

      return Promise.resolve({
        sessionID: newSessionID,
        sessionDone: () => {
          lockedSessions.delete(newSessionID);
        },
      });
    }

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

    console.log("Using existing session", existingSession);
    lockedSessions.add(existingSession as SessionID);

    return Promise.resolve({
      sessionID: existingSession as SessionID,
      sessionDone: () => {
        lockedSessions.delete(existingSession as SessionID);
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

    console.log("Persisted session", sessionID);

    return Promise.resolve({
      sessionDone: () => {
        lockedSessions.delete(sessionID);
      },
    });
  }
}
