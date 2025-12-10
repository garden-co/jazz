import { products } from "@/content/showcase";
import Link from "next/link";

export function FeaturedShowcaseSection() {
  return (
    <section>
      <div className="container mb-20 flex flex-col gap-6 py-10 text-black dark:text-white">
        <p className="text-center text-stone-500">
          Indie devs, startups and enterprises ship better apps, faster - with
          Jazz.
        </p>
        <div className="flex flex-wrap justify-between gap-8">
          {products.map(
            (product) =>
              product.featured &&
              product.logo && (
                <Link
                  key={product.url}
                  href={"/showcase#" + product.name}
                  target="_blank"
                >
                  <div title={product.slogan}>
                    {<product.logo height={25} />}
                  </div>
                </Link>
              ),
          )}
        </div>
      </div>
    </section>
  );
}
