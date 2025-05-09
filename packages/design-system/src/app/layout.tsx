import type { Metadata } from "next";
import "./globals.css";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { fontClasses } from "../fonts";

export const metadata: Metadata = {
  title: "Jazz Design System by Garden Computing, Inc",
  description: "Jazz Design System by Garden Computing, Inc",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="h-full">
      <body
        className={[
          ...fontClasses,
          "h-full",
          "bg-white dark:bg-stone-950 text-default",
        ].join(" ")}
      >
        {children}
      </body>
    </html>
  );
}
