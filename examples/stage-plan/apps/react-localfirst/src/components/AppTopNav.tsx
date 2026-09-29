import { useState } from "react";
import { useDb } from "jazz-tools/react";
import { Avatar } from "@astryxdesign/core/Avatar";
import { Switch } from "@astryxdesign/core/Switch";
import { HStack } from "@astryxdesign/core/Stack";
import { TopNav, TopNavHeading, TopNavItem } from "@astryxdesign/core/TopNav";
import type { Me } from "../model/actions.js";
import { href, type Route } from "../router.js";
import { ProfileDialog } from "./ProfileDialog.js";

export function AppTopNav({ route, me }: { route: Route; me?: Me }) {
  const [isProfileOpen, setProfileOpen] = useState(false);
  return (
    <>
      <TopNav
        label="Main"
        heading={<TopNavHeading heading="StagePlan" headingHref={href.shows()} />}
        startContent={
          <>
            <TopNavItem
              label="Shows"
              href={href.shows()}
              isSelected={route.page === "shows" || route.page === "show"}
            />
            <TopNavItem
              label="Checklist"
              href={href.checklist()}
              isSelected={route.page === "checklist"}
            />
          </>
        }
        endContent={
          <HStack gap={3} vAlign="center">
            <SyncSwitch />
            {me && (
              <Avatar
                name={me.profile.name}
                size="sm"
                tooltip={`${me.profile.name}: edit your name`}
                onClick={() => setProfileOpen(true)}
              />
            )}
          </HStack>
        }
      />
      {me && <ProfileDialog me={me} isOpen={isProfileOpen} onOpenChange={setProfileOpen} />}
    </>
  );
}

/**
 * Pauses syncing to show local-first behaviour: edits keep working while
 * offline and reach the crew once you switch back.
 */
function SyncSwitch() {
  const db = useDb();
  const [isOnline, setOnline] = useState(true);
  return (
    <Switch
      label="Online"
      size="sm"
      value={isOnline}
      changeAction={async (next) => {
        setOnline(next);
        if (next) await db.reconnect();
        else await db.disconnect();
      }}
    />
  );
}
