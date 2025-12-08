import { Pricing } from "@/components/Pricing";
import { HeroHeader } from "@garden-co/design-system/src/components/molecules/HeroHeader";
import { Metadata } from "next";

const metaTags = {
  title: "Jazz Cloud Pricing",
  description: "Starter, Indie, Pro and Enterprise Tiers",
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

export default function PricingPage() {
  return (<div className="container flex flex-col gap-6">
    <HeroHeader
      title="Pricing"
      slogan="Real-time sync and storage infrastructure that scales up to millions of
        users."
    />
      <Pricing />
      </div>
  );
}