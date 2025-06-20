"use client";

import { usePathname } from "next/navigation";
import { ViewsLayout } from "./ViewsLayout";

export default function Layout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const viewName = pathname.split("/").pop();

  return (
    <ViewsLayout>
      <h2 className="text-2xl font-bold capitalize my-3">{viewName}</h2>
      {children}
    </ViewsLayout>
  );
}
