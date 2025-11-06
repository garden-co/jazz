import { Pricing } from "@/components/Pricing";
import { LatencyMap } from "@/components/cloud/latencyMap";
import { GridCard } from "@garden-co/design-system/src/components/atoms/GridCard";
import { H2, H3 } from "@garden-co/design-system/src/components/atoms/Headings";
import { P } from "@garden-co/design-system/src/components/atoms/Paragraph";
import { GappedGrid } from "@garden-co/design-system/src/components/molecules/GappedGrid";
import { HeroHeader } from "@garden-co/design-system/src/components/molecules/HeroHeader";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";
import { SectionHeader } from "@garden-co/design-system/src/components/molecules/SectionHeader";
import type { Metadata } from "next";
import CloudPlusBackup from "./cloudPlusBackup.mdx";
import CloudPlusDIY from "./cloudPlusDIY.mdx";
import CompletelyDIY from "./completelyDIY.mdx";

const metaTags = {
  title: "Jazz Cloud",
  description: "Serverless sync & storage for Jazz apps.",
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

export default function Cloud() {
  const h3Style = "text-xl md:text-2xl mb-2 font-semibold tracking-tight";
  return (
    <div className="flex flex-col gap-16">
      <div className="container flex flex-col gap-12 overflow-hidden">
        <HeroHeader
          title="Jazz Cloud"
          slogan="Real-time sync and storage infrastructure that scales up to millions of users."
        />
        <LatencyMap />
        <GappedGrid>
          <GridCard>
            <H2 className={h3Style}>Optimal cloud routing</H2>
            <P>
              Get ultra-low latency between any group of users with our
              decentralized cloud interconnect.
            </P>
          </GridCard>
          <GridCard>
            <H2 className={h3Style}>Smart caching</H2>

            <P>
              Give users instant load times, with their latest data state always
              cached close to them.
            </P>
          </GridCard>
          <GridCard>
            <H2 className={h3Style}>Blob storage & media streaming</H2>

            <P>
              Store files and media streams as idiomatic `CoValues` without S3.
            </P>
          </GridCard>
        </GappedGrid>
      </div>

      <div className="border-y bg-stone-100 py-8 dark:border-y-0 dark:bg-stone-925 dark:bg-transparent dark:py-0 lg:py-16">
        <div className="container flex flex-col gap-5">
          <H2>Pricing</H2>

          <Pricing />
        </div>
      </div>

      <div className="container flex flex-col gap-16">
        <div>
          <SectionHeader
            title="Custom deployment scenarios"
            slogan="You can rely on Jazz Cloud. But you don't have to."
          />
          <P>
            Because Jazz is open-source, you can optionally run your own sync
            nodes &mdash; in a variety of setups.
          </P>
          <GappedGrid>
            <GridCard>
              <Prose size="sm">
                <CloudPlusBackup />
              </Prose>
            </GridCard>
            <GridCard>
              <Prose size="sm">
                <CloudPlusDIY />
              </Prose>
            </GridCard>
            <GridCard>
              <Prose size="sm">
                <CompletelyDIY />
              </Prose>
            </GridCard>
          </GappedGrid>
        </div>
      </div>
    </div>
  );
}
