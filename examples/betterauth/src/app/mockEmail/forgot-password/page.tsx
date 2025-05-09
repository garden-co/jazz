"use client";

import { Reset } from "@/components/emails";

export default function Page() {
  const searchParams = new URLSearchParams(window.location.search);
  const name = searchParams.get("name") ?? undefined;
  const url = searchParams.get("url") ?? undefined;
  const otp = searchParams.get("otp") ?? undefined;
  return <Reset name={name} url={url} otp={otp}></Reset>;
}
