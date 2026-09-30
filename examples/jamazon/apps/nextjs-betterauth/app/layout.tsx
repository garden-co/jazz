import type { Metadata } from "next";
import "@astryxdesign/core/astryx.css";
import "@garden-co/design/jazz/theme.css";
import "@garden-co/design/jazz/components.css";
import "@garden-co/design/jazz/fonts.css";
import "./globals.css";
import { StoreProviders } from "@/src/components/StoreProviders";

export const metadata: Metadata = {
  title: "Jamazon",
  description: "A local-first music-instrument storefront built with Jazz",
  // The brand fonts are licensed for Garden Computing sites only.
  robots: { index: false, follow: false },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body>
        <StoreProviders>{children}</StoreProviders>
      </body>
    </html>
  );
}
