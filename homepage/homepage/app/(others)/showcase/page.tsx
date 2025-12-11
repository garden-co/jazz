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
        slogan="Successful apps and systems across diverse industries."
      />

      <div className="flex flex-col gap-8">
        {products.map((product) => (
          <div
            key={product.url}
            id={product.name}
            className="group flex flex-col gap-3 rounded-lg border-b pb-8 dark:bg-stone-950 md:flex-row md:gap-4 target:animate-in"
          >
            <Link
              href={product.url}
              target="_blank"
              className="flex-2 max-w-[calc(min(100%,32rem))]"
            >
              <Image
                className="w-full rounded-md border dark:border-0"
                src={product.image.src}
                width="900"
                height="675"
                alt=""
              />
            </Link>

            <div className="flex-3 flex min-w-[calc(min(100%,32rem))] flex-col gap-2">
              {product.logo ? (
                <div
                  className="flex items-center gap-2 text-black dark:text-white"
                  aria-label={product.name}
                >
                  {<product.logo height={30} />}
                </div>
              ) : (
                <H2 className="font-medium leading-none text-highlight">
                  {product.name}
                </H2>
              )}
              <H3>{product.slogan}</H3>
              <Link
                href={product.url}
                target="_blank"
                className="text-lg text-stone-500 underline dark:text-stone-400"
              >
                {product.url.replace("https://", "")}
              </Link>
              <p className="text-sm">{product.description}</p>
              {product.testimonials.map((testimonial) => (
                <Testimonial
                  size="sm"
                  key={testimonial.name}
                  {...testimonial}
                />
              ))}
            </div>
          </div>
        ))}
      </div>

      {/* Contact Form Section */}
      <ContactForm />
    </div>
  );
}
