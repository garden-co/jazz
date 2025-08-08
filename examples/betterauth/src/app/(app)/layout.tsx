import type { Metadata } from "next";
import { Navbar } from "@/components/navbar";

export const metadata: Metadata = {
  title: "Jazz Example: Better Auth",
  description: "Jazz example application demonstrating Better Auth integration",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <>
      <Navbar />
      <div className="container mx-auto pt-16 min-h-screen flex flex-col items-center justify-center">
        {children}
      </div>
    </>
  );
}
