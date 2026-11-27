import type { AuthData } from "../../schema";
import { requestAccount } from "./request-account";

/** The caller, from the Better Auth session cookie zero-cache forwards. */
export async function authData(request: Request): Promise<AuthData | undefined> {
  const account = await requestAccount(request);
  return account ? { userID: account.accountId } : undefined;
}
