import { JazzAndAuth } from "@/components/JazzAndAuth";
import { Toaster } from "@/components/ui/sonner";
import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Jazz Example: Better Auth",
  description: "Jazz example application demonstrating Better Auth integration",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="antialiased">
        <JazzAndAuth>
          {children}
          <Toaster richColors />
        </JazzAndAuth>
      </body>
    </html>
  );
}
