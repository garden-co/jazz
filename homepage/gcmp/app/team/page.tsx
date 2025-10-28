import { ProfileCard } from "@/app/components/ProfileCard";
import { team } from "@/app/team/members";
import { HeroHeader } from "@garden-co/design-system/src/components/molecules/HeroHeader";

export default function TeamPage() {
  return (
    <div className="container">
      <HeroHeader title="Meet the team" slogan="" />

      <div className="grid gap-8 sm:grid-cols-2 lg:grid-cols-3 lg:gap-12">
        {team.map((person) => (
          <ProfileCard key={person.name} person={person} />
        ))}
      </div>
    </div>
  );
}
