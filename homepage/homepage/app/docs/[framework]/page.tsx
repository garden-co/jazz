import { Framework, frameworks } from "@/content/framework";
import { DocPage, getDocMetadata } from "@/lib/docMdxContent";

export async function generateMetadata({
  params,
}: {
  params: Promise<{ framework: string }>;
}) {
  const { framework } = await params;

  return getDocMetadata(framework, []);
}

export default async function Page({
  params,
}: {
  params: Promise<{ framework: string }>;
}) {
  const { framework } = await params;

  return <DocPage framework={framework} slug={[]} />;
}

// https://nextjs.org/docs/app/api-reference/functions/generate-static-params
export const dynamicParams = false;
export const dynamic = "force-static";

export async function generateStaticParams() {
  const paths: Array<{ framework: Framework }> = [];

  for (const framework of frameworks) {
    paths.push({
      framework,
    });
  }

  return paths;
}
