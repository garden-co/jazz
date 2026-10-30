import type { Metadata } from "next";
import "@astryxdesign/core/astryx.css";
import "@garden-co/design/jazz/theme.css";
import "@garden-co/design/jazz/components.css";
import "@garden-co/design/jazz/fonts.css";
import "./styles.css";

export const metadata: Metadata = {
  title: "RecordPlayer",
  description: "A local-first music library and shared playlists, built on Jazz.",
  // The Jazz fonts are licensed for Garden Computing surfaces only.
  robots: { index: false, follow: false },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
