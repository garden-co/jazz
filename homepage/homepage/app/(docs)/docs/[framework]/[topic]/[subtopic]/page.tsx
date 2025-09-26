import { docNavigationItems } from "@/content/docs/docNavigationItems";
import { Framework, frameworks } from "@/content/framework";
import { DocPage, getDocMetadata } from "@/lib/docMdxContent";

export async function generateMetadata({
  params,
}: {
  params: Promise<{ topic: string; subtopic: string; framework: string }>;
}) {
  const { topic, subtopic, framework } = await params;

  return getDocMetadata(framework, [topic, subtopic]);
}

export default async function Page({
  params,
}: {
  params: Promise<{ topic: string; subtopic: string; framework: string }>;
}) {
  const { topic, subtopic, framework } = await params;

  return <DocPage framework={framework} slug={[topic, subtopic]} />;
}

// https://nextjs.org/docs/app/api-reference/functions/generate-static-params
export const dynamicParams = false;
export const dynamic = "force-static";

export async function generateStaticParams() {
  const paths: Array<{
    topic?: string;
    subtopic?: string;
    framework: Framework;
  }> = [];

  const processItem = (item: any) => {
    if (item.href && item.href.startsWith("/docs")) {
      const [topic, subtopic] = item.href
        .replace("/docs", "")
        .split("/")
        .filter(Boolean);
      if (topic && subtopic) {
        for (const framework of frameworks) {
          paths.push({
            topic,
            subtopic,
            framework,
          });
        }
      }
    }

    // Recursively process nested items
    if (item.items && item.items.length > 0) {
      item.items.forEach(processItem);
    }
  };

  for (const heading of docNavigationItems) {
    for (const item of heading?.items) {
      processItem(item);
    }
  }

  return paths;
}