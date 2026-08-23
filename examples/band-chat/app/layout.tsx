import type { Metadata } from "next";
import "../src/app.css";
export const metadata: Metadata = {
  title: "BandChat",
  description: "Local-first band chat with Jazz",
};
export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
