import { AgentID, CryptoProvider, RawAccountID, SessionID } from "cojson";
import { ID, Account, SessionProvider } from "jazz-tools";
import { SessionIDStorage } from "./SessionIDStorage";

export class BrowserSessionProvider implements SessionProvider {
  async acquireSession(
    accountID: ID<Account> | AgentID,
    crypto: CryptoProvider,
  ): Promise<{ sessionID: SessionID; sessionDone: () => void }> {
    const { sessionPromise, resolveSession } = createSessionLockPromise();

    // Get the list of sessions for the account, to try to acquire an existing session
    const sessionsList = await acquireSessionList(accountID);

    for (const [index, sessionID] of sessionsList.entries()) {
      const sessionAcquired = await tryToAcquireSession(
        sessionID,
        sessionPromise,
      );

      if (sessionAcquired) {
        console.log("Using existing session", sessionID, "at index", index); // This log is used in the e2e tests to verify the correctness of the feature
        return {
          sessionID,
          sessionDone: resolveSession,
        };
      }
    }

    const newSessionID = crypto.newRandomSessionID(
      accountID as RawAccountID | AgentID,
    );

    // Acquire exclusively the session to store the new session ID for reuse in future sessions
    await lockAndStoreSession(accountID, newSessionID, sessionPromise);

    console.log("Created new session", newSessionID); // This log is used in the e2e tests to verify the correctness of the feature

    return {
      sessionID: newSessionID,
      sessionDone: resolveSession,
    };
  }

  async persistSession(
    accountID: ID<Account> | AgentID,
    sessionID: SessionID,
  ): Promise<{ sessionDone: () => void }> {
    const { sessionPromise, resolveSession } = createSessionLockPromise();

    // Store the session id for future use and lock it until the session is done
    await lockAndStoreSession(accountID, sessionID, sessionPromise);

    console.log("Stored new session", sessionID);

    return { sessionDone: resolveSession };
  }
}

async function lockAndStoreSession(
  accountID: ID<Account> | AgentID,
  sessionID: SessionID,
  sessionPromise: Promise<void>,
) {
  const sessionAcquired = await tryToAcquireSession(sessionID, sessionPromise);

  if (!sessionAcquired) {
    // This should never happen because the session has been randomly generated
    throw new Error("Couldn't get lock on new session");
  }

  // We don't need to wait for this to finish, we only need to acquire the lock on the new session
  storeSessionID(accountID, sessionID);
}

function tryToAcquireSession(
  sessionID: SessionID,
  sessionDonePromise: Promise<void>,
) {
  return new Promise<boolean>((resolve) => {
    // Acquire exclusively the session if available
    navigator.locks.request(
      `load_session_${sessionID}`,
      { mode: "exclusive", ifAvailable: true },
      async (lock) => {
        if (!lock) {
          resolve(false); // Session already in use
          return;
        }

        resolve(true); // Session is available

        // Return the promise to lock the session until sessionDone is called
        return sessionDonePromise;
      },
    );
  });
}

function acquireSessionList(accountID: ID<Account> | AgentID) {
  return navigator.locks.request(
    `get_sessions_list_${accountID}`,
    { mode: "shared" },
    async (lock) => {
      if (!lock) {
        console.error("Couldn't get lock to get sessions list", accountID);
        return [];
      }

      return SessionIDStorage.getSessionsList(accountID);
    },
  );
}

function storeSessionID(
  accountID: ID<Account> | AgentID,
  sessionID: SessionID,
) {
  return navigator.locks.request(
    `store_session_${accountID}`,
    { mode: "exclusive" },
    async (lock) => {
      if (!lock) {
        console.error("Couldn't get lock to store session ID", accountID);
      }

      const sessionsList = SessionIDStorage.getSessionsList(accountID);
      SessionIDStorage.storeSessionID(
        accountID,
        sessionID,
        sessionsList.length,
      );
    },
  );
}

function createSessionLockPromise() {
  let resolveSession: () => void;
  const sessionPromise = new Promise<void>((resolve) => {
    resolveSession = resolve;
  });

  return {
    sessionPromise,
    resolveSession: resolveSession!,
  };
}
