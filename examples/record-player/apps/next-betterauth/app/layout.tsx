import type { Metadata } from "next";
import "./styles.css";

export const metadata: Metadata = {
  title: "RecordPlayer",
  description: "A local-first music player example.",
};
export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
