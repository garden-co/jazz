import { ChatDemoSection } from "./components/ChatDemoSection";
import { CollaborationFeaturesSection } from "./components/collaborationFeatures/CollaborationFeaturesSection";
import { EarlyAdopterSection } from "./components/EarlyAdopterSection";
import { EncryptionSection } from "./components/EncryptionSection";
import { EverythingElseSection } from "./components/everythingElse/EverythingElseSection";
import { HeroSection } from "./components/hero/HeroSection";
import { GetStartedSnippetSelect } from "./components/hero/GetStartedSnippetSelect";
import { HowJazzWorksSection } from "./components/fourSteps/HowJazzWorksSection";
import { LocalFirstFeaturesSection } from "./components/LocalFirstFeaturesSection";
import BeforeAndAfterSection from "./components/beforeAndAfter/BeforeAndAfterSection";
import { SupportedEnvironmentsSection } from "./components/hero/SupportedEnvironmentsSection";
import { Testimonial } from "@garden-co/design-system/src/components/molecules/Testimonial";

export default function Home() {
  return (
    <>
      <HeroSection />
      <div className="container flex flex-col gap-12 lg:gap-20">
        <GetStartedSnippetSelect />
        <SupportedEnvironmentsSection />
        <HowJazzWorksSection />

        <Testimonial name="Spreadsheet app (stealth)" role="CTO">
          <p>
            You don&apos;t have to think about deploying a database, SQL
            schemas, relations, and writing queries… Basically,{" "}
            <span className="bg-highlight px-1">
              if you know TypeScript, you know Jazz
            </span>
            , and you can ship an app. It&apos;s just so nice!
          </p>
        </Testimonial>

        <ChatDemoSection />

        <BeforeAndAfterSection />

        <LocalFirstFeaturesSection />

        <CollaborationFeaturesSection />

        <EncryptionSection />

        <Testimonial name="Invoice Radar" role="Technical Founder">
          We just wanted to build a single-player experience first, planning to
          add team and org features much later. But because of Jazz, we had that
          from day one.{" "}
          <span className="bg-highlight px-1">
            All we needed to add was an invite button.
          </span>
        </Testimonial>

        <EverythingElseSection />

        <EarlyAdopterSection />
      </div>
    </>
  );
}
