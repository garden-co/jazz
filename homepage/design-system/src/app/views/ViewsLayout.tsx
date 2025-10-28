"use client";

import { ViewsSideMenu } from "./ViewsSideMenu";

export function ViewsLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="container py-8 lg:py-16 relative h-full overflow-hidden flex flex-row gap-2">
      <ViewsSideMenu />
      <div className="flex-1 overflow-y-scroll overflow-x-hidden pr-8">
        <h1 className="text-2xl font-semibold font-display">
          Jazz Design System
        </h1>

        {children}
      </div>
    </div>
  );
}
