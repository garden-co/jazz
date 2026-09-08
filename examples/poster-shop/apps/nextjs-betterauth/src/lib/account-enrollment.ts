import {
  AccountAuthError,
  type AccountHandle,
  type AccountManager,
  type JWTAuth,
} from "jazz-tools";

export async function loginOrRegister(
  accounts: AccountManager<JWTAuth>,
  credential: JWTAuth,
): Promise<AccountHandle> {
  try {
    return await accounts.loginJWT(credential);
  } catch (cause) {
    if (!(cause instanceof AccountAuthError) || cause.code !== "identity_not_assigned") throw cause;
    try {
      return await accounts.registerJWT(credential);
    } catch (registerCause) {
      if (
        !(registerCause instanceof AccountAuthError) ||
        registerCause.code !== "identity_already_assigned"
      )
        throw registerCause;
      return await accounts.loginJWT(credential);
    }
  }
}

export async function bootstrapPersonalCanvas(token: string): Promise<Response> {
  return await fetch("/api/bootstrap", {
    method: "POST",
    credentials: "same-origin",
    headers: { authorization: `Bearer ${token}` },
  });
}
