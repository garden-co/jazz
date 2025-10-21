import {
  Everyone,
  JsonObject,
  LocalNode,
  RawAccount,
  RawCoValue,
  RawGroup,
} from "cojson";
import { CoID } from "cojson";
import { useState } from "react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../ui/table.js";
import { AccountOrGroupText } from "./account-or-group-text.js";
import { RawDataCard } from "./raw-data-card.js";
import { PageInfo, isCoId } from "./types.js";
import { Button, Icon, Modal, Input, Select } from "../ui/index.js";

function partitionMembers(data: Record<string, string>) {
  const everyone = Object.entries(data)
    .filter(([key]) => key === "everyone")
    .map(([key, value]) => ({
      id: key as CoID<RawCoValue>,
      role: value as string,
    }));

  const members = Object.entries(data)
    .filter(([key]) => isCoId(key))
    .map(([key, value]) => ({
      id: key as CoID<RawCoValue>,
      role: value,
    }));

  const parentGroups = Object.entries(data)
    .filter(([key]) => key.startsWith("parent_co_"))
    .map(([key, value]) => ({
      id: key.slice(7) as CoID<RawCoValue>,
      role: value,
    }));

  const childGroups = Object.entries(data)
    .filter(([key]) => key.startsWith("child_co_"))
    .map(([key, value]) => ({
      id: key.slice(6) as CoID<RawCoValue>,
      role: value,
    }));

  return { everyone, members, parentGroups, childGroups };
}

export function GroupView({
  coValue,
  data,
  onNavigate,
  node,
}: {
  coValue: RawCoValue;
  data: JsonObject;
  onNavigate: (pages: PageInfo[]) => void;
  node: LocalNode;
}) {
  const [isAddAccountModalOpen, setIsAddAccountModalOpen] = useState(false);
  const [isAddGroupModalOpen, setIsAddGroupModalOpen] = useState(false);

  const { everyone, members, parentGroups, childGroups } = partitionMembers(
    data as Record<string, string>,
  );

  const onRemoveMember = async (id: CoID<RawCoValue>) => {
    if (confirm("Are you sure you want to remove this member?") === false) {
      return;
    }
    try {
      const group = await node.load(coValue.id);
      if (group === "unavailable") {
        throw new Error("Group not found");
      }
      const rawGroup = group as RawGroup;
      rawGroup.removeMember(id as any);
    } catch (error) {
      console.error(error);
      throw error;
    }
  };

  const onRemoveGroup = async (id: CoID<RawCoValue>) => {
    if (confirm("Are you sure you want to remove this group?") === false) {
      return;
    }
    try {
      const group = await node.load(coValue.id);
      if (group === "unavailable") {
        throw new Error("Group not found");
      }
      const rawGroup = group as RawGroup;
      const targetGroup = await node.load(id);
      if (targetGroup === "unavailable") {
        throw new Error("Group not found");
      }
      const rawTargetGroup = targetGroup as RawGroup;
      rawGroup.revokeExtend(rawTargetGroup);
    } catch (error) {
      console.error(error);
      throw error;
    }
  };

  const handleAddAccountSubmit = async (
    event: React.FormEvent<HTMLFormElement>,
  ) => {
    event.preventDefault();
    const form = event.currentTarget;

    const accountId = (form.elements.namedItem("accountId") as HTMLInputElement)
      ?.value;
    const role = (form.elements.namedItem("role") as HTMLSelectElement)?.value;

    try {
      const group = await node.load(coValue.id);
      if (group === "unavailable") {
        throw new Error("Group not found");
      }

      let rawAccount: RawAccount | Everyone = "everyone";

      if (accountId !== "everyone") {
        const account = await node.load(accountId as CoID<RawCoValue>);
        if (account === "unavailable") {
          throw new Error("Account not found");
        }
        rawAccount = account as RawAccount;
      }

      const rawGroup = group as RawGroup;
      rawGroup.addMember(rawAccount, role as "reader" | "writer" | "admin");

      setIsAddAccountModalOpen(false);
    } catch (error: any) {
      console.error(error);
      alert("Failed to add account: " + error.message);
    }
  };

  const handleAddGroupSubmit = async (
    event: React.FormEvent<HTMLFormElement>,
  ) => {
    event.preventDefault();
    const form = event.currentTarget;

    const groupId = (form.elements.namedItem("groupId") as HTMLInputElement)
      ?.value;
    const role = (form.elements.namedItem("role") as HTMLSelectElement)?.value;

    try {
      const group = await node.load(coValue.id);
      if (group === "unavailable") {
        throw new Error("Group not found");
      }

      const targetGroup = await node.load(groupId as CoID<RawCoValue>);
      if (targetGroup === "unavailable") {
        throw new Error("Group not found");
      }

      const rawGroup = group as RawGroup;
      const rawTargetGroup = targetGroup as RawGroup;
      rawGroup.extend(
        rawTargetGroup,
        role as "reader" | "writer" | "admin" | "inherit",
      );

      setIsAddGroupModalOpen(false);
    } catch (error: any) {
      console.error(error);
      alert("Failed to add group: " + error.message);
    }
  };

  return (
    <>
      <Table>
        <TableHead>
          <TableRow>
            <TableHeader>Member</TableHeader>
            <TableHeader>Permission</TableHeader>
            <TableHeader></TableHeader>
          </TableRow>
        </TableHead>
        <TableBody>
          {everyone.map((member) => (
            <TableRow key={member.id}>
              <TableCell>{member.id}</TableCell>
              <TableCell>{member.role}</TableCell>
              <TableCell>
                {member.role !== "revoked" && (
                  <Button
                    variant="secondary"
                    onClick={() => onRemoveMember(member.id)}
                  >
                    <Icon name="delete" />
                  </Button>
                )}
              </TableCell>
            </TableRow>
          ))}
          {members.map((member) => (
            <TableRow key={member.id}>
              <TableCell>
                <AccountOrGroupText
                  coId={member.id}
                  node={node}
                  showId
                  onClick={() => {
                    onNavigate([{ coId: member.id, name: member.id }]);
                  }}
                />
              </TableCell>
              <TableCell>{member.role}</TableCell>
              <TableCell>
                {member.role !== "revoked" && (
                  <Button
                    variant="secondary"
                    onClick={() => onRemoveMember(member.id)}
                  >
                    <Icon name="delete" />
                  </Button>
                )}
              </TableCell>
            </TableRow>
          ))}
          {parentGroups.map((group) => (
            <TableRow key={group.id}>
              <TableCell>
                <AccountOrGroupText
                  coId={group.id}
                  node={node}
                  showId
                  onClick={() => {
                    onNavigate([{ coId: group.id, name: group.id }]);
                  }}
                />
              </TableCell>
              <TableCell>{group.role}</TableCell>
              <TableCell>
                {group.role !== "revoked" && (
                  <Button
                    variant="secondary"
                    onClick={() => onRemoveGroup(group.id)}
                  >
                    <Icon name="delete" />
                  </Button>
                )}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      <div
        style={{
          display: "flex",
          justifyContent: "flex-end",
          gap: "0.75rem",
          marginTop: "1rem",
        }}
      >
        <Button
          variant="primary"
          onClick={() => setIsAddAccountModalOpen(true)}
        >
          Add Account
        </Button>
        <Button variant="primary" onClick={() => setIsAddGroupModalOpen(true)}>
          Add Group
        </Button>
      </div>

      {childGroups.length > 0 && (
        <Table>
          <TableHead>
            <TableRow>
              <TableHeader>Children</TableHeader>
            </TableRow>
          </TableHead>
          <TableBody>
            {childGroups.map((group) => (
              <TableRow key={group.id}>
                <TableCell>
                  <AccountOrGroupText
                    coId={group.id}
                    node={node}
                    showId
                    onClick={() => {
                      onNavigate([{ coId: group.id, name: group.id }]);
                    }}
                  />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}

      <RawDataCard data={data} />

      <Modal
        isOpen={isAddAccountModalOpen}
        onClose={() => setIsAddAccountModalOpen(false)}
        heading="Add Account"
        showButtons={false}
      >
        <form onSubmit={handleAddAccountSubmit}>
          <div
            style={{ display: "flex", flexDirection: "column", gap: "1rem" }}
          >
            <Input
              name="accountId"
              label="Account ID"
              placeholder="Enter account ID"
              required
            />
            <Select name="role" label="Role">
              <option value="reader">Reader</option>
              <option value="writer">Writer</option>
              <option value="admin">Admin</option>
              <option value="writeOnly">Write Only</option>
            </Select>
            <div
              style={{
                display: "flex",
                gap: "0.75rem",
                justifyContent: "flex-end",
                marginTop: "0.5rem",
              }}
            >
              <Button
                type="button"
                variant="secondary"
                onClick={() => setIsAddAccountModalOpen(false)}
              >
                Cancel
              </Button>
              <Button type="submit" variant="primary">
                Add
              </Button>
            </div>
          </div>
        </form>
      </Modal>

      <Modal
        isOpen={isAddGroupModalOpen}
        onClose={() => setIsAddGroupModalOpen(false)}
        heading="Add Group"
        showButtons={false}
      >
        <form onSubmit={handleAddGroupSubmit}>
          <div
            style={{ display: "flex", flexDirection: "column", gap: "1rem" }}
          >
            <Input
              name="groupId"
              label="Group ID"
              placeholder="Enter group ID"
              required
            />
            <Select name="role" label="Role">
              <option value="reader">Reader</option>
              <option value="writer">Writer</option>
              <option value="admin">Admin</option>
              <option value="inherit">Inherit</option>
            </Select>
            <div
              style={{
                display: "flex",
                gap: "0.75rem",
                justifyContent: "flex-end",
                marginTop: "0.5rem",
              }}
            >
              <Button
                type="button"
                variant="secondary"
                onClick={() => setIsAddGroupModalOpen(false)}
              >
                Cancel
              </Button>
              <Button type="submit" variant="primary">
                Add
              </Button>
            </div>
          </div>
        </form>
      </Modal>
    </>
  );
}
