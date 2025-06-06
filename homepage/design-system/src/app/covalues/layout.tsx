"use client";

import { JazzProvider } from "jazz-react";

export default function CovaluesLayout({
  children,
}: {
  children: any;
}) {
  return <JazzProvider sync={{ when: "never" }}>{children}</JazzProvider>;
}
