"use client";
import "./globals.css";
import { JazzProvider } from "jazz-react";

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={`antialiased p-4`}>
        <JazzProvider
          sync={{
            peer: "wss://cloud.jazz.tools",
          }}
        >
          {children}
        </JazzProvider>
      </body>
    </html>
  );
}
