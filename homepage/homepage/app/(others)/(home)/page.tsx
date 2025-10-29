import { ChatDemoSection } from "@/components/home/ChatDemoSection";
import { CollaborationFeaturesSection } from "@/components/home/CollaborationFeaturesSection";
import { EarlyAdopterSection } from "@/components/home/EarlyAdopterSection";
import { EncryptionSection } from "@/components/home/EncryptionSection";
import { FeaturesSection } from "@/components/home/FeaturesSection";
import { HeroSection } from "@/components/home/HeroSection";
import { LocalFirstFeaturesSection } from "@/components/home/LocalFirstFeaturesSection";
import ProblemStatementSection from "@/components/home/ProblemStatementSection";
import { SupportedEnvironmentsSection } from "@/components/home/SupportedEnvironmentsSection";
import { LatencyMap } from "@/components/cloud/latencyMap";
import { Pricing } from "@/components/Pricing";

export default function Home() {
  return (
    <>
      <HeroSection />

      <ChatDemoSection />

      <SupportedEnvironmentsSection />

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
