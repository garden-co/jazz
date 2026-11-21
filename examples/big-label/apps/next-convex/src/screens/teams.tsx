"use client";

import { useState } from "react";
import {
  Button,
  EmptyState,
  HStack,
  Icon,
  IconButton,
  Link,
  Selector,
  Table,
  Text,
  VStack,
  VisuallyHidden,
} from "@astryxdesign/core";
import { useMutation, useQuery } from "convex/react";
import { api } from "../../convex/_generated/api";
import type { Id } from "../../convex/_generated/dataModel";
import { TeamDialog } from "../components/forms";
import { StatusBadge, formatCount, formatDate, sentence } from "../components/list";
import { ConfirmButton } from "../components/confirm-button";
import { PageHeader, PageSection } from "../components/page";
import { useCan, useOrganization } from "../lib/organization";
import { href, navigate } from "../lib/route";
import { useWrite } from "../lib/use-write";

const teamRoles = ["lead", "member"] as const;

export function TeamsPage() {
  const organization = useOrganization();
  const canManage = useCan("manageTeams");
  const [isAdding, setIsAdding] = useState(false);
  // Teams by name, with bounded counts (limit 2000) computed by the server.
  const teams = useQuery(api.teams.list, { organizationId: organization.id });
  const isLoading = teams === undefined;

  return (
    <VStack gap={5}>
      <PageHeader
        title="Teams"
        description="Teams group members and take on releases."
        actions={
          <Button
            label="Create team"
            isDisabled={!canManage}
            tooltip={canManage ? undefined : "Admins can create teams"}
            onClick={() => setIsAdding(true)}
          />
        }
      />
      {!isLoading && teams?.length === 0 ? (
        <EmptyState title="No teams yet" description="Create a team to staff your releases." />
      ) : (
        <Table
          density="compact"
          data={teams ?? []}
          idKey="_id"
          columns={[
            {
              key: "name",
              header: "Team",
              renderCell: (team) => <Link href={href.team(team._id)}>{team.name}</Link>,
            },
            {
              key: "members",
              header: "Members",
              renderCell: (team) => formatCount(team.members, team.membersCapped),
            },
            {
              key: "releases",
              header: "Releases",
              renderCell: (team) => formatCount(team.releases, team.releasesCapped),
            },
          ]}
        />
      )}
      {isAdding && (
        <TeamDialog isOpen onOpenChange={setIsAdding} onCreated={(id) => navigate(href.team(id))} />
      )}
    </VStack>
  );
}

export function TeamPage({ id }: { id: string }) {
  const organization = useOrganization();
  const canManage = useCan("manageTeams");
  const write = useWrite();
  const [isRenaming, setIsRenaming] = useState(false);
  const [membershipId, setMembershipId] = useState<string | null>(null);
  const [teamRole, setTeamRole] = useState<string>("member");
  const loaded = useQuery(api.teams.get, { organizationId: organization.id, id });
  const team = loaded ?? undefined;
  const isLoading = loaded === undefined;
  const details = useQuery(
    api.teams.details,
    team ? { organizationId: organization.id, teamId: team._id } : "skip",
  );
  const assignments = details?.assignments ?? [];
  const releases = details?.releases ?? [];
  const memberships = useQuery(api.members.list, { organizationId: organization.id }) ?? [];
  const addToTeam = useMutation(api.teams.addMember);
  const removeFromTeam = useMutation(api.teams.removeMember);
  const removeTeam = useMutation(api.teams.remove);

  if (!team)
    return isLoading ? null : (
      <EmptyState
        title="Team not found"
        actions={<Button label="Back to teams" href={href.teams} variant="secondary" />}
      />
    );

  const onTeam = new Set(assignments.map((assignment) => assignment.membershipId));
  const candidates = memberships.filter((membership) => !onTeam.has(membership._id));
  const addMember = () => {
    if (!membershipId) return;
    write("Couldn't add to the team", () =>
      addToTeam({
        organizationId: organization.id,
        teamId: team._id,
        membershipId: membershipId as Id<"memberships">,
        role: teamRole,
      }),
    );
    setMembershipId(null);
  };
  const deleteTeam = () => {
    write("Couldn't delete the team", () =>
      removeTeam({ organizationId: organization.id, teamId: team._id }),
    );
    navigate(href.teams);
  };
  const sortedReleases = [...releases].sort((a, b) => b.releaseDate - a.releaseDate);

  return (
    <VStack gap={6}>
      <PageHeader
        title={team.name}
        parent={{ label: "Teams", href: href.teams }}
        actions={
          canManage && (
            <>
              <Button label="Rename" variant="secondary" onClick={() => setIsRenaming(true)} />
              <ConfirmButton
                label="Delete team"
                title={`Delete ${team.name}?`}
                description="Members stay in the label; the team and its release assignments go."
                onConfirm={deleteTeam}
              />
            </>
          )
        }
      />
      <PageSection title="Members">
        {assignments.length === 0 ? (
          <Text color="secondary">Nobody is on this team yet.</Text>
        ) : (
          <Table
            density="compact"
            data={assignments}
            idKey="_id"
            columns={[
              {
                key: "name",
                header: "Name",
                renderCell: (assignment) => assignment.membership?.person?.name ?? "Unknown",
              },
              {
                key: "role",
                header: "Team role",
                renderCell: (assignment) => sentence(assignment.role),
              },
              {
                key: "actions",
                header: <VisuallyHidden>Actions</VisuallyHidden>,
                align: "end",
                renderCell: (assignment) =>
                  canManage && (
                    <IconButton
                      label={`Remove ${assignment.membership?.person?.name ?? "member"} from team`}
                      icon={<Icon icon="close" size="sm" />}
                      variant="ghost"
                      size="sm"
                      onClick={() =>
                        write("Couldn't remove from the team", () =>
                          removeFromTeam({
                            organizationId: organization.id,
                            assignmentId: assignment._id,
                          }),
                        )
                      }
                    />
                  ),
              },
            ]}
          />
        )}
        {canManage && candidates.length > 0 && (
          <HStack gap={2} vAlign="end" wrap="wrap">
            <Selector
              label="Add a member"
              placeholder="Choose a member"
              options={candidates.map((membership) => ({
                value: membership._id,
                label: membership.person?.name ?? "Unknown",
              }))}
              value={membershipId}
              onChange={setMembershipId}
              hasSearch
              hasClear
              width={240}
            />
            <Selector
              label="Team role"
              options={teamRoles.map((value) => ({ value, label: sentence(value) }))}
              value={teamRole}
              onChange={setTeamRole}
              width={140}
            />
            <Button
              label="Add"
              variant="secondary"
              isDisabled={!membershipId}
              onClick={addMember}
            />
          </HStack>
        )}
      </PageSection>
      <PageSection title="Releases">
        {sortedReleases.length === 0 ? (
          <Text color="secondary">
            Assign this team from a release page to see its releases here.
          </Text>
        ) : (
          <Table
            density="compact"
            data={sortedReleases}
            idKey="_id"
            columns={[
              {
                key: "title",
                header: "Title",
                renderCell: (release) => (
                  <Link href={href.release(release._id)}>{release.title}</Link>
                ),
              },
              { key: "catalogNumber", header: "Cat. no." },
              {
                key: "releaseDate",
                header: "Release date",
                renderCell: (release) => formatDate(release.releaseDate),
              },
              {
                key: "status",
                header: "Status",
                renderCell: (release) => <StatusBadge status={release.status} />,
              },
            ]}
          />
        )}
      </PageSection>
      {isRenaming && <TeamDialog isOpen onOpenChange={setIsRenaming} team={team} />}
    </VStack>
  );
}
