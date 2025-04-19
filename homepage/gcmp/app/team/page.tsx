import { TeamMember, team } from "@/app/team/members";
import { HeroHeader } from "@garden-co/design-system/src/components/molecules/HeroHeader";
import {
  SiBluesky,
  SiGithub,
  SiLinkedin,
  SiX,
} from "@icons-pack/react-simple-icons";
import { GlobeIcon, LucideIcon } from "lucide-react";
import Link from "next/link";

function SocialLink({
  link,
  label,
  icon: Icon,
}: {
  label: string;
  link: string;
  icon: LucideIcon;
}) {
  return (
    <Link href={link} className="p-1 -m-1">
      <Icon size={16} />
      <span className="sr-only">{label}</span>
    </Link>
  );
}

function Person({ person }: { person: TeamMember }) {
  const imageClassName = "size-24 shadow rounded-md bg-stone-100 sm:size-28 ";
  return (
    <div className="flex items-center gap-6">
      <img src={`/team/${person.image}`} className={imageClassName} />

      <div className="flex flex-col gap-3">
        <h3 className="text-lg leading-none font-semibold tracking-tight text-highlight">
          {person.name}
        </h3>
        <p className="text-sm leading-none text-gray-600 dark:text-stone-400">
          {person.titles.join(", ")}
        </p>
        <p className="text-sm leading-none text-gray-600 dark:text-stone-400">
          {person.location}
        </p>

        <div className="flex gap-2 mt-0.5">
          {person.website && (
            <SocialLink
              link={person.website}
              icon={GlobeIcon}
              label="Website"
            />
          )}
          {person.x && (
            <SocialLink
              link={`https://x.com/${person.x}`}
              icon={SiX}
              label="X profile"
            />
          )}
          {person.bluesky && (
            <SocialLink
              link={`https://bsky.app/profile/${person.bluesky}`}
              icon={SiBluesky}
              label="Bluesky profile"
            />
          )}
          {person.github && (
            <SocialLink
              link={`https://github.com/${person.github}`}
              label="Github profile"
              icon={SiGithub}
            />
          )}
          {person.linkedin && (
            <SocialLink
              link={`https://www.linkedin.com/in/${person.linkedin}`}
              icon={SiLinkedin}
              label="Linkedin profile"
            />
          )}
        </div>
      </div>
    </div>
  );
}

export default function TeamPage() {
  return (
    <div className="container">
      <HeroHeader title="Meet the team" slogan="" />

      <div className="grid gap-8 sm:grid-cols-2 lg:grid-cols-3 lg:gap-12">
        {team.map((person) => (
          <Person key={person.name} person={person} />
        ))}
      </div>
    </div>
  );
}
