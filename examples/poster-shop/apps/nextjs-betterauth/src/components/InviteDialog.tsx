"use client";

import {
  Button,
  Dialog,
  DialogHeader,
  SegmentedControl,
  SegmentedControlItem,
  Text,
  TextInput,
  VStack,
} from "@astryxdesign/core";
import { useDb } from "jazz-tools/react";
import { UserPlus } from "lucide-react";
import { useState } from "react";
import { app } from "@/schema";

/** Admins issue an invite link; redeeming it happens server-side. */
export function InviteButton({ canvasId }: { canvasId: string }) {
  const db = useDb();
  const [open, setOpen] = useState(false);
  const [role, setRole] = useState<"editor" | "viewer">("editor");
  const [link, setLink] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const createLink = () => {
    const token = crypto.randomUUID();
    db.insert(app.canvasInvites, { canvasId, token, role });
    setLink(`${window.location.origin}/dashboard?join=${token}`);
    setCopied(false);
  };

  return (
    <>
      <Button
        label="Invite"
        variant="secondary"
        icon={<UserPlus />}
        onClick={() => {
          setLink(null);
          setOpen(true);
        }}
      />
      <Dialog isOpen={open} onOpenChange={setOpen} width="28rem">
        <DialogHeader title="Invite collaborators" onOpenChange={setOpen} />
        <VStack gap={4} padding={4}>
          <Text color="secondary">
            Anyone who signs in with this link joins the poster with the role you choose.
          </Text>
          <SegmentedControl
            label="Role"
            value={role}
            onChange={(value) => {
              setRole(value as "editor" | "viewer");
              setLink(null);
            }}
            layout="fill"
          >
            <SegmentedControlItem value="editor" label="Can edit" />
            <SegmentedControlItem value="viewer" label="Can view" />
          </SegmentedControl>
          {link ? (
            <VStack gap={2}>
              <TextInput label="Invite link" value={link} isReadOnly />
              <Button
                label={copied ? "Link copied" : "Copy link"}
                variant="primary"
                clickAction={async () => {
                  await navigator.clipboard.writeText(link);
                  setCopied(true);
                }}
              />
            </VStack>
          ) : (
            <Button label="Create invite link" variant="primary" onClick={createLink} />
          )}
        </VStack>
      </Dialog>
    </>
  );
}
