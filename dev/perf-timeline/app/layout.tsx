import type { Metadata } from "next";
import "./style.css";

export const metadata: Metadata = {
  title: "Jazz · Performance timeline",
  description:
    "Wallclock benchmark history across Jazz releases, main commits, and performance experiments.",
};
export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
