import { DemoAuthBasicUI, createJazzReactApp, useDemoAuth } from "jazz-react";

const Jazz = createJazzReactApp();
export const { useAccount, useCoState } = Jazz;

export function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const [auth, authState] = useDemoAuth();

  return (
    <>
      <Jazz.Provider
        auth={auth}
        // replace `you@example.com` with your email as a temporary API key
        peer="wss://cloud.jazz.tools/?key=you@example.com"
      >
        {children}
      </Jazz.Provider>
      <DemoAuthBasicUI appName="Briscola" state={authState} />
    </>
  );
}
