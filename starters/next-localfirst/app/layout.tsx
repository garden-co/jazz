import type { Metadata } from "next";
import "./globals.css";
import { JazzProvider } from "@/components/jazz-provider";

export const metadata: Metadata = {
  title: "My Jazz App",
  description: "Built with Jazz and Next.js",
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
