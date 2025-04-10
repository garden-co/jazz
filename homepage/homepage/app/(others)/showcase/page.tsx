import { products } from "@/content/showcase";
import { HeroHeader } from "gcmp-design-system/src/app/components/molecules/HeroHeader";
import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";

const title = "Built with Jazz";
const description = "Great apps by smart people.";

export const metadata: Metadata = {
  title,
  description,
  openGraph: {
    title,
    description,
  },
};

export default function Page() {
  return (
    <div className="container flex flex-col gap-6 pb-10 lg:pb-20">
      <HeroHeader
        title="Built with Jazz"
        slogan="Great apps by smart people."
      />

      <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-8">
        {products.map((product) => (
          <Link
            href={product.url}
            key={product.url}
            className="group border bg-stone-50 shadow-sm p-3 flex flex-col gap-3 rounded-lg md:gap-4 dark:bg-stone-950"
          >
            <Image
              className="rounded-md border dark:border-0"
              src={product.imageUrl}
              width="900"
              height="675"
              alt=""
            />
            <div className="space-y-2">
              <h2 className="font-medium text-stone-900 dark:text-white leading-none">
                {product.name}
              </h2>
              <p className="text-sm">{product.description}</p>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
