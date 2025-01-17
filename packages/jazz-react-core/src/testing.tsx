import { Account, AnonymousJazzAgent } from "jazz-tools";
import { getJazzContextShape } from "jazz-tools/testing";
import { useMemo } from "react";
import { JazzContext } from "./provider.js";

export function JazzTestProvider<Acc extends Account>({
  children,
  account,
}: {
  children: React.ReactNode;
  account: Acc | { guest: AnonymousJazzAgent };
}) {
  const value = useMemo(() => {
    return {
      ...getJazzContextShape(account),
      toggleNetwork: () => {},
    };
  }, [account]);

  return <JazzContext.Provider value={value}>{children}</JazzContext.Provider>;
}

export {
  createJazzTestAccount,
  createJazzTestGuest,
  linkAccounts,
  setActiveAccount,
} from "jazz-tools/testing";
