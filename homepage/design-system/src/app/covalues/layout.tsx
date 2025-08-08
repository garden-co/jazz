"use client";

import { JazzReactProvider } from "jazz-tools/react";

export default function CovaluesLayout({ children }: { children: any }) {
  return (
    <JazzReactProvider sync={{ when: "never" }}>{children}</JazzReactProvider>
  );
}
