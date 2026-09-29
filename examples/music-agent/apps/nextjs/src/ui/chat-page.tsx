"use client";

import { useEffect, useState } from "react";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { AppShell } from "@astryxdesign/core/AppShell";
import { Badge } from "@astryxdesign/core/Badge";
import { Button } from "@astryxdesign/core/Button";
import { SideNav, SideNavHeading, SideNavItem, SideNavSection } from "@astryxdesign/core/SideNav";
import { TopNav, TopNavHeading } from "@astryxdesign/core/TopNav";
import { app } from "@/schema";
import { StatusScreen } from "@/components/status-screen";
import { bootstrapWorkspace } from "@/src/lib/account-enrollment";
import { authClient, getJwtFromBetterAuth } from "@/src/lib/auth-client";
import { ConversationView } from "./conversation-view";

/** Prepare the workspace once (server-side, idempotent), then show the app. */
export function ChatPage({ agentLabel }: { agentLabel: string }) {
  const { data: session } = authClient.useSession();
  const [bootstrap, setBootstrap] = useState<"loading" | "ready" | "failed">("loading");
  useEffect(() => {
    if (!session) return;
    let cancelled = false;
    void getJwtFromBetterAuth()
      .then((token) => (token ? bootstrapWorkspace(token) : undefined))
      .then((response) => {
        if (!cancelled) setBootstrap(response?.ok ? "ready" : "failed");
      });
    return () => {
      cancelled = true;
    };
  }, [session?.user.id]);
  if (!session || bootstrap === "loading")
    return <StatusScreen message="Preparing your booking desk…" />;
  if (bootstrap === "failed") return <StatusScreen error="The workspace could not be prepared." />;
  return <MusicAgentApp agentLabel={agentLabel} userName={session.user.name} />;
}

export function MusicAgentApp({ agentLabel, userName }: { agentLabel: string; userName: string }) {
  const db = useDb();
  const account = useSession()?.user.account;
  const { data: artists = [] } = useAll(app.artists);
  const { data: conversations = [] } = useAll(app.conversations.orderBy("createdAt", "desc"));
  const [selectedId, setSelectedId] = useState<string>();
  const artist = artists[0];
  const selected = conversations.find((c) => c.id === selectedId) ?? conversations[0];

  function startConversation() {
    if (!account || !artist) return;
    const { value } = db.insert(app.conversations, {
      ownerAccount: account,
      artistId: artist.id,
      title: "New conversation",
      createdAt: new Date(),
    });
    setSelectedId(value.id);
  }

  const sideNav = (
    <SideNav
      header={
        <SideNavHeading
          heading={artist?.name ?? "MusicAgent"}
          subheading={`Booked by ${userName}`}
        />
      }
      topContent={
        <Button
          label="New conversation"
          onClick={startConversation}
          isDisabled={!artist}
          width="100%"
        />
      }
    >
      <SideNavSection title="Conversations">
        {conversations.map((conversation) => (
          <SideNavItem
            key={conversation.id}
            label={conversation.title}
            isSelected={conversation.id === selected?.id}
            onClick={() => setSelectedId(conversation.id)}
          />
        ))}
      </SideNavSection>
    </SideNav>
  );

  return (
    <AppShell
      variant="section"
      contentPadding={0}
      topNav={
        <TopNav
          heading={<TopNavHeading heading="MusicAgent" />}
          endContent={
            <Badge variant={agentLabel === "Claude" ? "info" : "neutral"} label={agentLabel} />
          }
        />
      }
      sideNav={sideNav}
    >
      {selected && artist ? (
        <ConversationView key={selected.id} conversation={selected} agentLabel={agentLabel} />
      ) : (
        <StatusScreen message="Loading conversations…" />
      )}
    </AppShell>
  );
}
