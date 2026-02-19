import { products } from "@/content/showcase";
import { H1 } from "@garden-co/design-system/src/components/atoms/Headings";
import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { ContactForm } from "@/components/ContactForm";
import { ShowcaseGrid } from "@/components/ShowcaseGrid";
import type { Metadata } from "next";

const metaTags = {
  title: "Built with Jazz",
  description: "Successful adopters across diverse industries.",
  url: "https://jazz.tools",
};

export const metadata: Metadata = {
  title: metaTags.title,
  description: metaTags.description,
  openGraph: {
    title: metaTags.title,
    description: metaTags.description,
    images: [
      {
        url: `${metaTags.url}/api/opengraph-image?title=${encodeURIComponent(metaTags.title)}`,
        height: 630,
        alt: metaTags.title,
      },
    ],
  },
};

export default function Page() {
  return (
    <div className="container flex flex-col gap-6 pb-10 lg:pb-20">
      <hgroup className="pt-12 md:pt-20 mb-10 grid gap-2">
        <H1>Built with Jazz</H1>
        <p className="text-lg text-pretty leading-relaxed max-w-3xl dark:text-stone-200 md:text-xl">
          Successful apps and systems across diverse industries.
        </p>
        <Button href="#submit" className="mt-2" intent="primary">
          Submit your app &rarr;
        </Button>
      </hgroup>

      <ShowcaseGrid products={products} />

      <div id="submit" className="scroll-mt-24">
        <ContactForm />
      </div>
    </div>
  );
}
