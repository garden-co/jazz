import { products } from "@/content/showcase";
import Link from "next/link";
import Marquee from "react-fast-marquee";

function ProductLogoLink({
  product,
  className = "",
}: {
  product: (typeof products)[number];
  className?: string;
}) {
  const LogoComponent = product.logo;
  if (!LogoComponent) return null;

  return (
    <Link
      href={"/showcase#" + product.name}
      target="_blank"
      className={`hover:opacity-70 ${className}`}
    >
      <div title={product.slogan}>
        <LogoComponent height={25} />
      </div>
    </Link>
  );
}

export function LogosSection() {
  const featuredProducts = products.filter(
    (product) => product.featured && product.logo,
  );

  return (
    <section>
      <div className="mb10 container mt-10 flex flex-col gap-6 py-10 text-black dark:text-white md:mb-16">
        <p className="text-center text-stone-500">
          Indie devs, startups and enterprises ship better apps, faster - with
          Jazz.
        </p>
        <div className="lg:hidden">
          <Marquee speed={50} loop={0} pauseOnClick autoFill>
            {[...featuredProducts].map((product, index) => (
              <div key={`${product.url}-${index}`} className="mx-6">
                <ProductLogoLink product={product} />
              </div>
            ))}
          </Marquee>
        </div>
        <div className="hidden flex-wrap justify-between gap-6 lg:flex">
          {featuredProducts.map((product) => (
            <ProductLogoLink key={product.url} product={product} />
          ))}
        </div>
      </div>
    </section>
  );
}
