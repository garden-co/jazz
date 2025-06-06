import { Pizzazz } from "@/components/Pizzazz";
import { JazzNav } from "@/components/nav";
import { Suspense } from "react";

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="flex-1 w-full">
      <JazzNav />
      <main>{children}</main>
      <Suspense>
        <Pizzazz />
      </Suspense>
    </div>
  );
}
