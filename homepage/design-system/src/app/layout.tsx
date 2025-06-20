import type { Metadata } from "next";
import "./globals.css";
import { Toaster } from "react-hot-toast";
import { ThemeProvider } from "../components/organisms/ThemeProvider";
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
    <html lang="en" className="h-full" suppressHydrationWarning>
      <body
        className={[
          ...fontClasses,
          "h-full",
          "bg-white dark:bg-stone-950 text-default",
        ].join(" ")}
      >
        <ThemeProvider
          attribute="class"
          defaultTheme="system"
          enableSystem
          disableTransitionOnChange
        >
          <Toaster position="bottom-right" />
          {children}
        </ThemeProvider>
      </body>
    </html>
  );
}
