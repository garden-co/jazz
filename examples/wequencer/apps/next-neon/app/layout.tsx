import type { Metadata } from "next";
import "@astryxdesign/core/astryx.css";
import "@garden-co/design/jazz/theme.css";
import "@garden-co/design/jazz/components.css";
import "@garden-co/design/jazz/fonts.css";
import "./globals.css";
import { AppTheme } from "@/components/app-theme";
import { NeonProvider } from "@/components/neon-provider";
import { getUser } from "@/lib/auth";

// Every page reads the session cookie.
export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Wequencer",
  description: "A collaborative step sequencer built with Jazz, Next.js, and Better Auth",
  robots: { index: false, follow: false },
};

export default async function RootLayout({ children }: { children: React.ReactNode }) {
  const signedIn = await getUser();
  const user = signedIn ? { id: signedIn.id, name: signedIn.name } : null;
  return (
    <html lang="en">
      <body>
        <AppTheme>
          <NeonProvider user={user}>{children}</NeonProvider>
        </AppTheme>
      </body>
    </html>
  );
}
