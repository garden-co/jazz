import type { Metadata } from "next";
import "./globals.css";
import { JazzProvider } from "@/components/jazz-provider";
export const metadata: Metadata = {
  title: "PosterShop",
  description: "Collaborative local-first poster design",
};
export default function Layout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>
        <JazzProvider>{children}</JazzProvider>
      </body>
    </html>
  );
}
