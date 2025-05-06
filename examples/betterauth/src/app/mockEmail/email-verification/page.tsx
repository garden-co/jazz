"use client";

import { Verify } from "@/components/emails";

export default function Page() {
  const searchParams = new URLSearchParams(window.location.search);
  const name = searchParams.get("name") ?? undefined;
  const url = searchParams.get("url") ?? undefined;
  const otp = searchParams.get("otp") ?? undefined;
  return <Verify name={name} url={url} otp={otp}></Verify>;
}
