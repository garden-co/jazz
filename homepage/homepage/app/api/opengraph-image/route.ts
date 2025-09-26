import { NextRequest } from "next/server";
import {
  OpenGraphImage
} from "@garden-co/design-system/src/components/organisms/OpenGraphImage";

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const title = searchParams.get("title") ?? "Jazz";
  const framework = searchParams.get("framework") ?? "";
  const topic = searchParams.get("topic") ?? "";
  const subtopic = searchParams.get("subtopic") ?? "";

  return OpenGraphImage({ title, framework, topic, subtopic });
}