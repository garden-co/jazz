import { NextRequest } from "next/server";
import {
  OpenGraphImage
} from "@garden-co/design-system/src/components/organisms/OpenGraphImage";

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const title = searchParams.get("title") ?? "Jazz";
  const framework = searchParams.get("framework");
  const topic = searchParams.get("topic");
  const subtopic = searchParams.get("subtopic");

  const displayFramework =
    framework && topic !== "server-side" ? formatLabel(framework) : "";

  return OpenGraphImage({
    title,
    framework: displayFramework,
    topic: formatLabel(topic),
    subtopic: formatLabel(subtopic),
  });
}

function formatLabel(str: string | null) {
  if (!str) return "";
  return str
    .split("-")
    .map((part) => part[0].toUpperCase() + part.slice(1))
    .join(" ");
}