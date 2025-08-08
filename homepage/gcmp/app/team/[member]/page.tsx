"use client";

import Router from "next/router";
import { ProfileCard } from "@/app/components/ProfileCard";
import { team } from "@/app/team/members";

export default async function TeamMemberPage({
  params,
}: {
  params: Promise<{ member: string }>;
}) {
  const { member } = await params;

  const memberInfo = team.find((m) => m.slug === member);

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
