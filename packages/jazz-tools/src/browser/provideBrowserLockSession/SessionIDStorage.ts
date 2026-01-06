import { SessionID } from "cojson";

function getSessionKey(accountID: string, index: number) {
  return accountID + "_" + index;
}

export class SessionIDStorage {
  static getSessionsList(accountID: string) {
    let sessionsList: SessionID[] = [];
    let i = 0;
    let lastSessionID: SessionID | null;

    do {
      lastSessionID = localStorage.getItem(
        getSessionKey(accountID, i),
      ) as SessionID | null;
      if (lastSessionID) {
        sessionsList.push(lastSessionID);
      }
      i++;
    } while (lastSessionID);

    return sessionsList;
  }

  static storeSessionID(
    accountID: string,
    sessionID: SessionID,
    index: number,
  ) {
    localStorage.setItem(getSessionKey(accountID, index), sessionID);
  }
}
