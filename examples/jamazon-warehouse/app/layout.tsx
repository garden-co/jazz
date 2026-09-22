import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Jamazon Warehouse",
  description: "Jazz warehouse operations example",
};
export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
