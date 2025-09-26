import { docNavigationItems } from "@/content/docs/docNavigationItems";
import { Framework, frameworks } from "@/content/framework";
import { DocPage, getDocMetadata } from "@/lib/docMdxContent";

export async function generateMetadata({
  params,
}: {
  params: Promise<{ topic: string; framework: string }>;
}) {
  const { topic, framework } = await params;

  return getDocMetadata(framework, [topic]);
}

export default async function Page({
  params,
}: {
  params: Promise<{ topic: string; framework: string }>;
}) {
  const { topic, framework } = await params;

  return <DocPage framework={framework} slug={[topic]} />;
}

// https://nextjs.org/docs/app/api-reference/functions/generate-static-params
export const dynamicParams = false;
export const dynamic = "force-static";

export async function generateStaticParams() {
  const paths: Array<{ topic?: string; framework: Framework }> = [];

  const processItem = (item: any) => {
    if (item.href && item.href.startsWith("/docs")) {
      const [topic] = item.href
        .replace("/docs", "")
        .split("/")
        .filter(Boolean);
      if (topic) {
        for (const framework of frameworks) {
          paths.push({
            topic,
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
