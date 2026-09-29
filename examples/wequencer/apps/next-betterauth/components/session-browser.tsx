"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { useAll, useSession } from "jazz-tools/react";
import { Button } from "@astryxdesign/core/Button";
import { ClickableCard } from "@astryxdesign/core/ClickableCard";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { Grid } from "@astryxdesign/core/Grid";
import { Heading } from "@astryxdesign/core/Heading";
import { HStack } from "@astryxdesign/core/HStack";
import { Spinner } from "@astryxdesign/core/Spinner";
import { Text } from "@astryxdesign/core/Text";
import { VStack } from "@astryxdesign/core/VStack";
import { app } from "@/schema";
import { AccountId } from "@/components/account-id";
import { PageColumn } from "@/components/page-column";
import { NewSessionDialog } from "@/components/new-session-dialog";

export function SessionBrowser() {
  const router = useRouter();
  const author = useSession()?.user.account;
  const { data: sessions = [], isLoading } = useAll(app.sessions.orderBy("$createdAt", "desc"));
  const [isCreating, setIsCreating] = useState(false);

  return (
    <PageColumn>
      <HStack gap={4} justify="between" align="center" wrap="wrap">
        <Heading level={1}>Sessions</Heading>
        <Button variant="primary" label="New session" onClick={() => setIsCreating(true)} />
      </HStack>
      {isLoading ? (
        <Spinner label="Finding your sessions…" />
      ) : sessions.length === 0 ? (
        <EmptyState
          headingLevel={2}
          title="No sessions yet"
          description="Start a session and add bandmates, or send your account ID to a bandmate so they can add you to theirs."
          actions={
            <Button variant="primary" label="New session" onClick={() => setIsCreating(true)} />
          }
        />
      ) : (
        <Grid columns={{ minWidth: 240 }} gap={4}>
          {sessions.map((session) => (
            <ClickableCard key={session.id} label={session.title} href={`/dashboard/${session.id}`}>
              <VStack gap={1}>
                <Heading level={2} maxLines={1}>
                  {session.title}
                </Heading>
                <Text type="supporting">{session.tempo_bpm} BPM</Text>
              </VStack>
            </ClickableCard>
          ))}
        </Grid>
      )}
      {author ? <AccountId accountId={author} /> : null}
      {author ? (
        <NewSessionDialog
          author={author}
          isOpen={isCreating}
          onOpenChange={setIsCreating}
          onCreated={(sessionId) => router.push(`/dashboard/${sessionId}`)}
        />
      ) : null}
    </PageColumn>
  );
}
