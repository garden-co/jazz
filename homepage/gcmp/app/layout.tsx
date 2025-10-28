import { ThemeProvider } from "@/components/ThemeProvider";
import type { Metadata } from "next";
import "./globals.css";
import { fontClasses } from "@garden-co/design-system/src/fonts";

import { GcmpNav } from "@/components/Nav";
import { ThemeToggle } from "@/components/ThemeToggle";
import { Copyright } from "@garden-co/design-system/src/components/atoms/Copyright";
import { Analytics } from "@vercel/analytics/react";
import { SpeedInsights } from "@vercel/speed-insights/next";

export const metaTags = {
  title: "garden computing",
  description:
    "Computers are magic. So why do we put up with so much complexity? We believe just a few new ideas can make all the difference.",
  url: "https://garden.co",
};

export const metadata: Metadata = {
  // metadataBase is a convenience option to set a base URL prefix for metadata fields that require a fully qualified URL.
  metadataBase: new URL(metaTags.url),
  title: {
    template: "%s | garden computing",
    default: metaTags.title,
  },
  applicationName: "garden computing",
  description: metaTags.description,
  openGraph: {
    title: metaTags.title,
    description: metaTags.description,
    url: metaTags.url,
    siteName: "garden computing",
    images: [
      {
        url: "/social-image.png",
        width: 1200,
        height: 630,
      },
    ],
  },
  alternates: {
    canonical: metaTags.url,
    types: {
      "application/rss+xml": `${
        process.env.VERCEL_URL
          ? `https://${process.env.VERCEL_URL}`
          : "http://localhost:3000"
      }/api/rss`,
    },
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="h-full">
      <body
        className={[
          ...fontClasses,
          "min-h-full flex flex-col items-center",
          "bg-white text-default dark:bg-stone-950",
        ].join(" ")}
      >
        <SpeedInsights />
        <Analytics />
        <ThemeProvider
          attribute="class"
          defaultTheme="system"
          enableSystem
          disableTransitionOnChange
        >
          <GcmpNav />
          <main className="flex-1 w-full">{children}</main>
          <footer className="py-8 text-sm flex justify-between gap-3 w-full container mt-12 md:mt-20">
            <Copyright />

            <ThemeToggle className="hidden md:block" />
          </footer>
        </ThemeProvider>
      </body>
    </html>
  );
}
