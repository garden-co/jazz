import { createJazzSessionOwner, type JazzSession } from "../session/state.js";
import { createAccountManager, type AccountManagerConfig } from "./create-account-manager.js";
import {
  createJazzClient,
  type JazzClient,
  type JazzClientConfig,
} from "../react-native/create-jazz-client.js";

export type JazzSessionConfig = Omit<JazzClientConfig, "account"> &
  AccountManagerConfig & {
    initial?: "local-first";
  };

/** Prepare the host account store once and own client replacement across authentication. */
export async function createJazzSession(
  config: JazzSessionConfig,
): Promise<JazzSession<JazzClient>> {
  const accounts = await createAccountManager(config);
  return createJazzSessionOwner({
    accounts,
    initial: config.initial,
    openClient: (account) => createJazzClient({ ...config, account }),
  });
}
