"use client";
import { ProfileCard } from "@/app/components/ProfileCard";
import { team } from "@/app/team/members";

import Router from "next/router";

const membersNameToInfoMap = {
  anselm: "Anselm Eickhoff",
  guido: "Guido D'Orsi",
  gio: "Giordano Ricci",
  trisha: "Trisha Lim",
  benjamin: "Benjamin Leveritt",
  nikos: "Nikos Papadopoulos",
  emil: "Emil Sayahi",
  meg: "Meg Culotta",
  nikita: "Nikita Voloboev",
  sammii: "Sammii Kellow",
  brad: "Bradley Kowalski",
};

export default async function TeamMemberPage({
  params,
}: { params: Promise<{ member: string }> }) {
  const { member } = await params;

  if (!(member in membersNameToInfoMap)) {
    Router.push("/team");
  }

  const memberName =
    membersNameToInfoMap[member as keyof typeof membersNameToInfoMap];
  const memberInfo = team.find(
    (m: { name: string }) => m.name.toLowerCase() === memberName.toLowerCase(),
  );

  if (!memberInfo) {
    Router.push("/team");
    return null;
  }

  return (
    <div className="w-full min-h-[calc(100vh-8rem)] flex items-center justify-center">
      <ProfileCard person={memberInfo} />
    </div>
  );
}
