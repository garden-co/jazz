import { DocsOpenGraphImage, imageSize, imageContentType } from 'gcmp-design-system/src/app/components/organisms/OpenGraphImage';
import { getDocMetadata, getMdxWithToc } from '@/lib/docMdxContent';
export const title = "Quickstart";
export const size = imageSize;
export const contentType = imageContentType;
export const alt = "Quickstart";

export default async function Image({ params }: { params: Promise<{ framework: string, topic: string }> }) {
  const { framework, topic } = await params;
  const { tocItems } = await getMdxWithToc(framework, [topic]);

  const title = tocItems[0].value;

  return DocsOpenGraphImage({
    title: title,
    framework,
    contents: tocItems[0].children?.map((child) => child.value) ?? [],
  });
}
