import type { Metadata, Viewport } from "next";
import { JazzProvider } from "@/components/jazz-provider";
import { JazzTheme } from "@/components/jazz-theme";
import "./globals.css";

export const metadata: Metadata = {
  title: "BandBook",
  description: "A local-first workspace for running a band",
  robots: { index: false, follow: false },
};

export const viewport: Viewport = { width: "device-width", initialScale: 1 };

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>
        <JazzTheme>
          <JazzProvider>{children}</JazzProvider>
        </JazzTheme>
      </body>
    </html>
  );
}
