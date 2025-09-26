import { DocPage, generateOGMetadata, getDocMetadata } from "@/lib/docMdxContent";

export async function generateMetadata({
  params,
}: {
  params: Promise<{ topic: string; subtopic: string; framework: string }>;
}) {
   const { topic, subtopic, framework } = await params;
  const docMeta = getDocMetadata(framework, [topic, subtopic]);
  return generateOGMetadata(framework, [topic, subtopic], docMeta);
}

export default async function Page({
  params,
}: {
  params: Promise<{ topic: string; subtopic: string; framework: string }>;
}) {
  const { topic, subtopic, framework } = await params;
  return <DocPage framework={framework} slug={[topic, subtopic]} />;
}
