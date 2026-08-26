import type { Metadata } from "next";
import "./globals.css";
import { JazzProvider } from "@/components/jazz-provider";

export const metadata: Metadata = {
  title: "Wequencer",
  description: "A collaborative step sequencer built with Jazz, Next.js, and Better Auth",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <JazzProvider>{children}</JazzProvider>
      </body>
    </html>
  );
}
