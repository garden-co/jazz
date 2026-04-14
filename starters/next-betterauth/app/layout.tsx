import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "My Jazz App",
  description: "Built with Jazz, Next.js, and BetterAuth",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
