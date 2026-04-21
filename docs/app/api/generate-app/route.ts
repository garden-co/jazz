import { NextResponse } from "next/server";

export async function POST() {
  const res = await fetch("https://v2.dashboard.jazz.tools/api/apps/generate", {
    method: "POST",
  });

  const data: unknown = await res.json();
  return NextResponse.json(data, { status: res.status });
}
