import { Framework, frameworks } from "@/content/framework";
import { DocPage, generateOGMetadata, getDocMetadata } from "@/lib/docMdxContent";

export async function generateMetadata({
  params,
}: {
  params: Promise<{ framework: string }>;
}) {
  const { framework } = await params;
  const docMeta = getDocMetadata(framework, []);
  return generateOGMetadata(framework, [], docMeta);
}

export default async function Page({
  params,
}: {
  params: Promise<{ framework: string }>;
}) {
  const { framework } = await params;
  return <DocPage framework={framework} slug={[]} />;
}

export const dynamicParams = false;
export const dynamic = "force-static";

export async function generateStaticParams() {
  return frameworks.map((framework) => ({ framework }));
}
