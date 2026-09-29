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
import { useAll, useDb, useOne } from "jazz-tools/react";
import { app } from "../../schema";
import { TeamDialog } from "../components/forms";
import { StatusBadge, formatCount, formatDate, sentence } from "../components/list";
import { ConfirmButton } from "../components/confirm-button";
import { PageHeader, PageSection } from "../components/page";
import { useCan, useOrganization } from "../lib/organization";
import { href, navigate } from "../lib/route";
import { deleteTeam as removeTeam } from "../lib/mutations";
import { useWrite } from "../lib/use-write";

const teamRoles = ["lead", "member"] as const;

const countLimit = 2000;

export function TeamsPage() {
  const organization = useOrganization();
  const canManage = useCan("manageTeams");
  const [isAdding, setIsAdding] = useState(false);
  const { data: teams, isLoading } = useAll(
    app.teams.where({ organizationId: organization.id }).orderBy("name", "asc"),
  );
  const { data: members = [] } = useAll(
    app.teamAssignments.where({ organizationId: organization.id }).limit(countLimit),
  );
  const { data: releases = [] } = useAll(
    app.releaseTeams.where({ organizationId: organization.id }).limit(countLimit),
  );
  const count = (rows: { teamId: string }[], teamId: string) =>
    formatCount(rows.filter((row) => row.teamId === teamId).length, rows.length >= countLimit);

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
          idKey="id"
          columns={[
            {
              key: "name",
              header: "Team",
              renderCell: (team) => <Link href={href.team(team.id)}>{team.name}</Link>,
            },
            { key: "members", header: "Members", renderCell: (team) => count(members, team.id) },
            { key: "releases", header: "Releases", renderCell: (team) => count(releases, team.id) },
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
  const db = useDb();
  const write = useWrite();
  const [isRenaming, setIsRenaming] = useState(false);
  const [membershipId, setMembershipId] = useState<string | null>(null);
  const [teamRole, setTeamRole] = useState<string>("member");
  const { data: team, isLoading } = useOne(
    app.teams.where({ id, organizationId: organization.id }),
  );
  const { data: assignments = [] } = useAll(
    app.teamAssignments
      .where({ organizationId: organization.id, teamId: id })
      .include({ membership: { person: true } }),
  );
  const { data: memberships = [] } = useAll(
    app.memberships.where({ organizationId: organization.id }).include({ person: true }).limit(500),
  );
  const { data: releases = [] } = useAll(
    app.releaseTeams
      .where({ organizationId: organization.id, teamId: id })
      .include({ release: true })
      .limit(200),
  );

  if (!team)
    return isLoading ? null : (
      <EmptyState
        title="Team not found"
        actions={<Button label="Back to teams" href={href.teams} variant="secondary" />}
      />
    );

  const onTeam = new Set(assignments.map((assignment) => assignment.membershipId));
  const candidates = memberships.filter((membership) => !onTeam.has(membership.id));
  const addMember = () => {
    if (!membershipId) return;
    write("Couldn't add to the team", () =>
      db
        .insert(app.teamAssignments, {
          organizationId: organization.id,
          teamId: id,
          membershipId,
          role: teamRole,
        })
        .wait({ tier: "global" }),
    );
    setMembershipId(null);
  };
  const deleteTeam = () => {
    write("Couldn't delete the team", async () =>
      (await removeTeam(db, organization.id, id)).wait({ tier: "global" }),
    );
    navigate(href.teams);
  };
  const sortedReleases = releases
    .flatMap((entry) => (entry.release ? [entry.release] : []))
    .sort((a, b) => new Date(b.releaseDate).getTime() - new Date(a.releaseDate).getTime());

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
            idKey="id"
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
                          db.delete(app.teamAssignments, assignment.id).wait({ tier: "global" }),
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
                value: membership.id,
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
            idKey="id"
            columns={[
              {
                key: "title",
                header: "Title",
                renderCell: (release) => (
                  <Link href={href.release(release.id)}>{release.title}</Link>
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
