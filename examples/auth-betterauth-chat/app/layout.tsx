import type { Metadata } from "next";
import "../../auth-simple-chat/src/styles.css";

export const metadata: Metadata = {
  title: "auth-betterauth-chat",
  description: "Next.js Better Auth chat example for Jazz",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
