"use client";

import { useState } from "react";
import {
  Badge,
  Button,
  Icon,
  IconButton,
  Selector,
  Table,
  Text,
  VStack,
  VisuallyHidden,
} from "@astryxdesign/core";
import { useAll } from "../lib/live";
import { queries } from "../queries";
import { AddMemberDialog } from "../components/forms";
import { PageHeader, PageSection } from "../components/page";
import { useCan, useOrganization } from "../lib/organization";
import { mutations } from "../lib/mutations";
import { useWrite } from "../lib/use-write";
import { capabilities, roleLabels, roles, isRole } from "../roles";

export function PeoplePage() {
  const organization = useOrganization();
  const canManage = useCan("manageMembers");
  const write = useWrite();
  const [isAdding, setIsAdding] = useState(false);
  const { data: memberships = [] } = useAll(
    queries.memberships.byLabel({ organizationId: organization.id }),
  );
  const members = [...memberships].sort((a, b) =>
    (a.person?.name ?? "").localeCompare(b.person?.name ?? ""),
  );
  const changeRole = (membershipId: string, role: string) =>
    write("Couldn't change the role", mutations.changeRole(membershipId, role));
  const remove = (membershipId: string) =>
    write("Couldn't remove the member", mutations.removeMember(membershipId));

  return (
    <VStack gap={6}>
      <PageHeader
        title="People"
        description="Everyone in this label and their role."
        actions={
          <Button
            label="Add member"
            isDisabled={!canManage}
            tooltip={canManage ? undefined : "Admins can add members"}
            onClick={() => setIsAdding(true)}
          />
        }
      />
      <Table
        density="compact"
        data={members}
        idKey="id"
        columns={[
          {
            key: "name",
            header: "Name",
            renderCell: (member) =>
              member.id === organization.membershipId
                ? `${member.person?.name ?? "You"} (you)`
                : (member.person?.name ?? "Unknown"),
          },
          {
            key: "role",
            header: "Role",
            renderCell: (member) =>
              canManage && member.id !== organization.membershipId ? (
                <Selector
                  label={`Role for ${member.person?.name ?? "member"}`}
                  isLabelHidden
                  size="sm"
                  variant="ghost"
                  options={roles.map((role) => ({ value: role, label: roleLabels[role] }))}
                  value={member.role}
                  onChange={(role) => changeRole(member.id, role)}
                />
              ) : (
                <Badge
                  variant={member.role === "admin" ? "info" : "neutral"}
                  label={isRole(member.role) ? roleLabels[member.role] : member.role}
                />
              ),
          },
          {
            key: "actions",
            header: <VisuallyHidden>Actions</VisuallyHidden>,
            align: "end",
            renderCell: (member) =>
              canManage &&
              member.id !== organization.membershipId && (
                <IconButton
                  label={`Remove ${member.person?.name ?? "member"}`}
                  icon={<Icon icon="close" size="sm" />}
                  variant="ghost"
                  size="sm"
                  onClick={() => remove(member.id)}
                />
              ),
          },
        ]}
      />
      <PageSection title="What each role can do">
        <Text color="secondary">
          Controls a role can&apos;t use are hidden or disabled. The server checks every change
          against the same rules, so a change sent anyway is refused.
        </Text>
        <Table
          density="compact"
          data={capabilities}
          idKey="id"
          columns={[
            { key: "label", header: "Permission" },
            ...roles.map((role) => ({
              key: role,
              header: roleLabels[role],
              align: "center" as const,
              renderCell: (capability: (typeof capabilities)[number]) =>
                capability.roles.includes(role) ? (
                  <Icon icon="check" size="sm" color="success" label="Allowed" />
                ) : (
                  <Text color="secondary">
                    <VisuallyHidden>Not allowed</VisuallyHidden>
                    <span aria-hidden>–</span>
                  </Text>
                ),
            })),
          ]}
        />
      </PageSection>
      {isAdding && <AddMemberDialog isOpen onOpenChange={setIsAdding} />}
    </VStack>
  );
}
