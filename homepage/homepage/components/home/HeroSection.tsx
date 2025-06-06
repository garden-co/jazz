import { marketingCopy } from "@/content/marketingCopy";
import { H1 } from "@garden-co/design-system/src/components/atoms/Headings";
import {
  Icon,
  type IconName,
} from "@garden-co/design-system/src/components/atoms/Icon";
import { Kicker } from "@garden-co/design-system/src/components/atoms/Kicker";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";
import Link from "next/link";
import { GetStartedSnippetSelect } from "./GetStartedSnippetSelect";


const features: Array<{
  title: string;
  icon: IconName;
}> = [
  {
    title: "Instant updates",
    icon: "instant",
  },
  {
    title: "Real-time sync",
    icon: "devices",
  },
  {
    title: "Multiplayer",
    icon: "spatialPresence",
  },
  {
    title: "File uploads",
    icon: "upload",
  },
  {
    title: "Social features",
    icon: "social",
  },
  {
    title: "Permissions",
    icon: "permissions",
  },
  {
    title: "E2E encryption",
    icon: "encryption",
  },
  {
    title: "Authentication",
    icon: "auth",
  },
];

export function HeroSection() {
  return (
    <div className="container grid items-center gap-x-8 gap-y-12 my-12 md:my-16 lg:my-24 lg:gap-x-10 lg:grid-cols-12">
      <div className="flex flex-col justify-center gap-5 lg:col-span-11 lg:gap-8">
        <Kicker>Toolkit for backendless apps</Kicker>
        <H1>
          <span className="inline-block text-highlight">
            {marketingCopy.headline}
          </span>
        </H1>

        <Prose size="lg" className="text-pretty max-w-2xl dark:text-stone-200">
          <p>
            Jazz gives you data without needing a database — plus auth,
            permissions, files and multiplayer without needing a backend.
          </p>
          <p>
            Do everything right from the frontend and ship better apps, faster.
          </p>
          <p>
            Open source. Self-host or use{" "}
            <Link className="text-reset" href="/cloud">
              Jazz Cloud
            </Link>{" "}
            for zero-config magic.
          </p>
        </Prose>

        <div className="grid grid-cols-2 gap-2 max-w-3xl sm:grid-cols-4 sm:gap-4">
          {features.map(({ title, icon }) => (
            <div
              key={title}
              className="flex text-xs sm:text-sm gap-2 items-center"
            >
              <Icon size="xs" name={icon} />
              <p>{title}</p>
            </div>
          ))}
        </div>
          <div className="col-span-10">
            <GetStartedSnippetSelect />
          </div>
      </div>
    </div>
  );
}
