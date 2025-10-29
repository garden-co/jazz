import { products } from "@/content/showcase";
import { HeroHeader } from "@garden-co/design-system/src/components/molecules/HeroHeader";
import { ContactForm } from "@/components/ContactForm";
import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";

const metaTags = {
  title: "Built with Jazz",
  description: "Great apps by smart people.",
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
      <HeroHeader
        title="Built with Jazz"
        slogan="Great apps by smart people."
      />

      <div className="grid items-start gap-8 md:grid-cols-12">
        <div className="grid gap-8 sm:grid-cols-2 md:col-span-8">
          {products.map((product) => (
            <Link
              href={product.url}
              key={product.url}
              className="shadow-xs group flex flex-col gap-3 rounded-lg border bg-stone-50 p-3 dark:bg-stone-950 md:gap-4"
            >
              <Image
                className="rounded-md border dark:border-0"
                src={product.imageUrl}
                width="900"
                height="675"
                alt=""
              />
              <div className="flex flex-col gap-2">
                <h2 className="font-medium leading-none text-highlight">
                  {product.name}
                </h2>
                <p className="text-sm">{product.description}</p>
              </div>
            </Link>
          ))}
        </div>

        <div className="md:col-span-4">
          <ContactForm />
        </div>
      </div>
    </div>
  );
}
