import type { Metadata } from "next";
import "./globals.css";
export const metadata: Metadata = {
  title: "PosterShop",
  description: "Collaborative local-first poster design",
};
export default function Layout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
