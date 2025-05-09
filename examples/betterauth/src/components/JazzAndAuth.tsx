"use client";

import { emailOTPClient, magicLinkClient } from "better-auth/client/plugins";
import { JazzProvider } from "jazz-react";
import { AuthProvider } from "jazz-react-auth-betterauth";
import * as NextImage from "next/image";
import * as NextLink from "next/link";
import { useRouter } from "next/navigation";
import { type ReactNode, lazy } from "react";

const JazzDevTools =
  process.env.NODE_ENV === "production"
    ? () => null
    : lazy(() =>
        import("jazz-inspector").then((res) => ({
          default: res.JazzInspector,
        })),
      );

export function JazzAndAuth({ children }: { children: ReactNode }) {
  const router = useRouter();
  return (
    <JazzProvider
      sync={{
        peer: "wss://cloud.jazz.tools/?key=betterauth-example@garden.co",
      }}
    >
      <>
        <AuthProvider
          Link={NextLink.default}
          Image={NextImage.default}
          navigate={router.push}
          replace={router.replace}
          options={{
            baseURL: process.env.NEXT_PUBLIC_AUTH_BASE_URL,
            plugins: [magicLinkClient(), emailOTPClient()],
          }}
        >
          {children}
        </AuthProvider>
        <JazzDevTools />
      </>
    </JazzProvider>
  );
}
