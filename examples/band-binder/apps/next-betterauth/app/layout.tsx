import type { Metadata } from "next";
import "./styles.css";

export const metadata: Metadata = {
  title: "BandBinder",
  description: "A local-first band workspace example built with Jazz.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
