"use client"; // tells Next.js that this component can't be server-side rendered. If you're not using Next.js, you can remove it.
// [!code --:1]
// @ts-expect-error Duplicate import
import { JazzReactProvider, PasskeyAuthBasicUI } from "jazz-tools/react";
// [!code ++:1]
import {
  // @ts-expect-error Duplicate import
  JazzReactProvider,
  // @ts-expect-error Duplicate import
  PasskeyAuthBasicUI,
  useAgent,
} from "jazz-tools/react";
import { JazzFestAccount } from "@/app/schema";

const apiKey = process.env.NEXT_PUBLIC_JAZZ_API_KEY;

export function JazzWrapper({ children }: { children: React.ReactNode }) {
  return (
    <JazzReactProvider
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
      AccountSchema={JazzFestAccount}
      enableSSR
    >
      {/* [!code --:3] */}
      <PasskeyAuthBasicUI appName="JazzFest">{children}</PasskeyAuthBasicUI>
      {/* [!code ++:1] */}
      <Auth>{children}</Auth>
    </JazzReactProvider>
  );
}

// [!code ++:10]
export function Auth({ children }: { children: React.ReactNode }) {
  const agent = useAgent();
  const isGuest = agent.$type$ !== "Account";
  if (isGuest) return children;
  return <PasskeyAuthBasicUI appName="JazzFest">{children}</PasskeyAuthBasicUI>;
}
