import { ChatDemoSection } from "@/components/home/ChatDemoSection";
import { EarlyAdopterSection } from "@/components/home/EarlyAdopterSection";
import { FeaturesSection } from "@/components/home/FeaturesSection";
import { HeroSection } from "@/components/home/HeroSection";
import { LocalFirstFeaturesSection } from "@/components/home/LocalFirstFeaturesSection";
import ProblemStatementSection from "@/components/home/ProblemStatementSection";
import { LatencyMap } from "@/components/cloud/latencyMap";
import { Pricing } from "@/components/Pricing";

export default function Home() {
  return (
    <>
      <HeroSection />

      <ChatDemoSection />

      <div className="container grid gap-8 pt-12">
        <ProblemStatementSection />
        <LocalFirstFeaturesSection />
      </div>

      <FeaturesSection />

      <div className="container flex flex-col gap-4 py-8 lg:py-16">
        <LatencyMap />

        <Pricing />
      </div>

      <EarlyAdopterSection />
    </>
  );
}
