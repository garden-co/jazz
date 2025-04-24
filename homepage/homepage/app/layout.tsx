import "./globals.css";
import type { Metadata } from "next";
import { fontClasses } from "@garden-co/design-system/src/fonts";
import { ThemeProvider } from "@/components/ThemeProvider";
import { JazzFooter } from "@/components/footer";
import { marketingCopy } from "@/content/marketingCopy";
import { Analytics } from "@vercel/analytics/react";
import { SpeedInsights } from "@vercel/speed-insights/next";

const metaTags = {
  title: `Jazz - ${marketingCopy.headline}`,
  description: marketingCopy.description,
  url: "https://jazz.tools",
};

export const metadata: Metadata = {
  // metadataBase is a convenience option to set a base URL prefix for metadata fields that require a fully qualified URL.
  metadataBase: new URL(metaTags.url),
  title: {
    template: "%s | Jazz",
    default: metaTags.title,
  },
  applicationName: "Jazz",
  description: metaTags.description,
  openGraph: {
    title: {
      template: "%s | Jazz",
      default: metaTags.title,
    },
    description: metaTags.description,
    url: metaTags.url,
    siteName: "Jazz",
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="h-full" suppressHydrationWarning>
      <body
        className={[
          ...fontClasses,
          "min-h-full flex flex-col items-center [&_*]:scroll-mt-[5rem]",
          "bg-white dark:bg-stone-950 text-default",
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
          {children}
          <JazzFooter />
        </ThemeProvider>
      </body>
    </html>
  );
}
