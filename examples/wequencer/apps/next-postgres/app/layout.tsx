import type { Metadata } from "next";
import "@astryxdesign/core/astryx.css";
import "@garden-co/design/jazz/theme.css";
import "@garden-co/design/jazz/components.css";
import "@garden-co/design/jazz/fonts.css";
import "./globals.css";
import { AppTheme } from "@/components/app-theme";
import { headers } from "next/headers";
import { DataProvider } from "@/components/data-provider";
import { auth } from "@/lib/auth";

export const metadata: Metadata = {
  title: "Wequencer",
  description: "A collaborative step sequencer built with Jazz, Next.js, and Better Auth",
  robots: { index: false, follow: false },
};

export default async function RootLayout({ children }: { children: React.ReactNode }) {
  const session = await auth.api.getSession({ headers: await headers() });
  const user = session ? { id: session.user.id, name: session.user.name } : null;
  return (
    <html lang="en">
      <body>
        <AppTheme>
          <DataProvider user={user}>{children}</DataProvider>
        </AppTheme>
      </body>
    </html>
  );
}
