import type { Metadata } from "next";
import "@astryxdesign/core/astryx.css";
import "@garden-co/design/jazz/theme.css";
import "@garden-co/design/jazz/components.css";
import "@garden-co/design/jazz/fonts.css";
import "./globals.css";
import { StoreProviders } from "@/src/components/StoreProviders";
import { publicCatalogue } from "@/src/server/public-catalogue";

// The first paint carries the current public catalogue, read per request.
export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Jamazon",
  description: "A local-first music-instrument storefront built with Jazz",
  // The brand fonts are licensed for Garden Computing sites only.
  robots: { index: false, follow: false },
};

export default async function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  const catalogue = await publicCatalogue();
  return (
    <html lang="en" suppressHydrationWarning>
      <body>
        <StoreProviders catalogue={catalogue}>{children}</StoreProviders>
      </body>
    </html>
  );
}
