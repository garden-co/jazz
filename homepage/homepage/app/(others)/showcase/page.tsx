import { products } from "@/content/showcase";
import { HeroHeader } from "@garden-co/design-system/src/components/molecules/HeroHeader";
import { ContactForm } from "@/components/ContactForm";
import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import { H2, H3 } from "@garden-co/design-system/src/components/atoms/Headings";
import { Testimonial } from "@garden-co/design-system/src/components/molecules/Testimonial";

const metaTags = {
  title: "Built with Jazz",
  description: "Successful adopters across diverse industries.",
  url:  "https://jazz.tools",
}

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
        slogan="Successful apps and systems across diverse industries."
      />

      <div className="flex flex-col gap-8">
        {products.map((product) => (
          <div key={product.url} className="group pb-8 border-b flex md:flex-row flex-col gap-3 rounded-lg md:gap-4 dark:bg-stone-950">
            <Link href={product.url} className="max-w-[calc(min(100%,32rem))] flex-2">
              <Image
                className="w-full rounded-md border dark:border-0"
                src={product.imageUrl}
                width="900"
                height="675"
                alt=""
              />
            </Link>

            <div className="flex-3 flex min-w-[calc(min(100%,32rem))] flex-col gap-2">
              <H2 className="font-medium leading-none text-highlight">
                {product.name}
              </H2>
              <H3>{product.slogan}</H3>
              <Link
                href={product.url}
                className="text-lg text-stone-500 underline dark:text-stone-400"
              >
                {product.url.replace("https://", "")}
              </Link>
              <p className="text-sm">{product.description}</p>
              {
                product.testimonials.map((testimonial) => (
                  <Testimonial size="sm" key={testimonial.name}
                    {...testimonial}
                  />
                ))
              }
            </div>
          </div>
        ))}
      </div>

      {/* Contact Form Section */}
      <ContactForm />
    </div>
  );
}
