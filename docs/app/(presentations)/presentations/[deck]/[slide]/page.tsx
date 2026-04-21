import { getMDXComponents } from "@/mdx-components";
import { presentationsSource } from "@/lib/presentations";
import { createRelativeLink } from "fumadocs-ui/mdx";
import type { Metadata } from "next";
import { notFound } from "next/navigation";

export default async function PresentationSlidePage(
  props: PageProps<"/presentations/[deck]/[slide]">,
) {
  const params = await props.params;
  const page = presentationsSource.getPage([params.deck, params.slide]);

  if (!page) notFound();

  const MDX = page.data.body;

  return (
    <div className="presentation-slide">
      <MDX
        components={getMDXComponents({
          a: createRelativeLink(presentationsSource, page),
        })}
      />
    </div>
  );
}

export function generateStaticParams() {
  return presentationsSource.getPages().map((page) => ({
    deck: page.slugs[0],
    slide: page.slugs[1],
  }));
}

export async function generateMetadata(
  props: PageProps<"/presentations/[deck]/[slide]">,
): Promise<Metadata> {
  const params = await props.params;
  const page = presentationsSource.getPage([params.deck, params.slide]);

  if (!page) notFound();

  return {
    title: `${page.data.title} - ${page.data.deckTitle}`,
    description: page.data.description,
  };
}
