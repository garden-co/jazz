import type { Metadata } from "next";
import "@astryxdesign/core/astryx.css";
import "@garden-co/design/jazz/theme.css";
import "@garden-co/design/jazz/components.css";
import "@garden-co/design/jazz/fonts.css";
import "./globals.css";
import { AppTheme } from "@/components/app-theme";
import { ConvexAuthNextjsServerProvider } from "@convex-dev/auth/nextjs/server";
import { ConvexClientProvider } from "@/components/convex-provider";

export const metadata: Metadata = {
  title: "Wequencer",
  description: "A collaborative step sequencer built with Convex, Next.js, and Convex Auth",
  robots: { index: false, follow: false },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <ConvexAuthNextjsServerProvider>
      <html lang="en">
        <body>
          <AppTheme>
            <ConvexClientProvider>{children}</ConvexClientProvider>
          </AppTheme>
        </body>
      </html>
    </ConvexAuthNextjsServerProvider>
  );
}
