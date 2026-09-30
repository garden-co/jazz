"use client";

import { useState, type FormEvent } from "react";
import { useDb } from "jazz-tools/react";
import {
  Avatar,
  Banner,
  Button,
  Card,
  Center,
  Dialog,
  DialogHeader,
  FileInput,
  Heading,
  HStack,
  Text,
  TextInput,
  VStack,
} from "@astryxdesign/core";
import { app, type Profile } from "../../schema";
import { avatarFromFile } from "../lib/attachments";
import { useObjectUrl } from "../lib/use-object-url";

interface ProfileDraft {
  displayName: string;
  avatar: Uint8Array | null;
  avatarType: string | null;
}

function ProfileFields({
  draft,
  onChange,
}: {
  draft: ProfileDraft;
  onChange: (draft: ProfileDraft) => void;
}) {
  const [error, setError] = useState<string | null>(null);
  const src = useObjectUrl(draft.avatar, draft.avatarType);

  async function pick(file: File | undefined) {
    if (!file) return;
    setError(null);
    try {
      const { bytes, type } = await avatarFromFile(file);
      onChange({ ...draft, avatar: bytes, avatarType: type });
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }

  return (
    <VStack gap={4}>
      <HStack gap={3} vAlign="end">
        <Avatar name={draft.displayName || "?"} src={src} size="xl" tooltip={false} />
        <VStack gap={2}>
          <FileInput
            label="Profile photo"
            placeholder="Choose photo"
            accept="image/png,image/jpeg,image/webp,image/gif"
            value={null}
            onChange={(file) => void pick(Array.isArray(file) ? file[0] : (file ?? undefined))}
          />
          {draft.avatar ? (
            <Button
              label="Remove photo"
              size="sm"
              variant="ghost"
              onClick={() => onChange({ ...draft, avatar: null, avatarType: null })}
            />
          ) : null}
        </VStack>
      </HStack>
      <TextInput
        label="Display name"
        description="Bandmates see this next to your messages."
        value={draft.displayName}
        onChange={(displayName) => onChange({ ...draft, displayName })}
        isRequired
      />
      {error ? (
        <Banner status="error" title={error} container="section" collapsible={false} />
      ) : null}
    </VStack>
  );
}

/** First run: every account chooses how it appears before joining rooms. */
export function ProfileSetup({
  author,
  defaultDisplayName,
}: {
  author: string;
  defaultDisplayName?: string;
}) {
  const db = useDb();
  const [draft, setDraft] = useState<ProfileDraft>({
    displayName: defaultDisplayName ?? "",
    avatar: null,
    avatarType: null,
  });
  const [error, setError] = useState<string | null>(null);

  function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const displayName = draft.displayName.trim();
    if (!displayName) return;
    try {
      // Two tabs finishing setup at once can create two profiles; the app
      // uses the oldest (see BandChat.tsx). A deterministic id would avoid the
      // duplicate, but anyone who knows the account id could claim it first.
      db.insert(app.profiles, {
        author,
        displayName,
        avatar: draft.avatar ?? undefined,
        avatarType: draft.avatarType ?? undefined,
      });
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }

  return (
    <Center axis="both" padding={4} className="page-fill">
      <VStack gap={6} width="100%" maxWidth={440}>
        <VStack gap={2}>
          <Heading level={1}>Set up your profile</Heading>
          <Text color="secondary">
            Your name and photo are shared only with people in your rooms and with room creators you
            ask to join.
          </Text>
        </VStack>
        <Card width="100%">
          <form onSubmit={save}>
            <VStack gap={4}>
              <ProfileFields draft={draft} onChange={setDraft} />
              {error ? (
                <Banner status="error" title={error} container="section" collapsible={false} />
              ) : null}
              <Button type="submit" variant="primary" label="Continue" width="100%" />
            </VStack>
          </form>
        </Card>
      </VStack>
    </Center>
  );
}

export function ProfileDialog({
  isOpen,
  onOpenChange,
  profile,
}: {
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
  profile: Profile;
}) {
  return (
    <Dialog isOpen={isOpen} onOpenChange={onOpenChange} purpose="form">
      <DialogHeader title="Your profile" onOpenChange={onOpenChange} />
      {isOpen ? <ProfileEditor profile={profile} onDone={() => onOpenChange(false)} /> : null}
    </Dialog>
  );
}

function ProfileEditor({ profile, onDone }: { profile: Profile; onDone: () => void }) {
  const db = useDb();
  const [draft, setDraft] = useState<ProfileDraft>({
    displayName: profile.displayName,
    avatar: profile.avatar ?? null,
    avatarType: profile.avatarType ?? null,
  });

  function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const displayName = draft.displayName.trim();
    if (!displayName) return;
    db.update(app.profiles, profile.id, {
      displayName,
      avatar: draft.avatar,
      avatarType: draft.avatarType,
    });
    onDone();
  }

  return (
    <form onSubmit={save}>
      <VStack gap={4} padding={4}>
        <ProfileFields draft={draft} onChange={setDraft} />
        <HStack gap={2} justify="end">
          <Button label="Cancel" variant="ghost" onClick={onDone} />
          <Button label="Save" variant="primary" type="submit" />
        </HStack>
      </VStack>
    </form>
  );
}
