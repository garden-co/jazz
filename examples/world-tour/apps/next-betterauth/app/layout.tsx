import type { Metadata } from "next";
import "./styles.css";
export const metadata: Metadata = {
  title: "WorldTour",
  description: "Jazz tour scheduling example",
};
export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
