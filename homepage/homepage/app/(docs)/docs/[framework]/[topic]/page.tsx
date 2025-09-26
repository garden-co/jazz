import { DocPage, generateOGMetadata, getDocMetadata } from "@/lib/docMdxContent";

export async function generateMetadata({
  params,
}: {
  params: Promise<{ topic: string; framework: string }>;
}) {
  const { topic, framework } = await params;
  const docMeta = getDocMetadata(framework, [topic]);
  return generateOGMetadata(framework, [topic], docMeta);
}

export default async function Page({
  params,
}: {
  params: Promise<{ topic: string; framework: string }>;
}) {
  const { topic, framework } = await params;
  return <DocPage framework={framework} slug={[topic]} />;
}
