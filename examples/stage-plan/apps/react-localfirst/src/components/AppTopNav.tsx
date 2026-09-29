import { useState, useSyncExternalStore } from "react";
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
 * offline and reach the crew once you switch back. The switch says whether
 * syncing is on, not whether the server is reachable: Jazz has no public
 * connection-status API yet, and the browser only reports whether there is a
 * network at all.
 */
function SyncSwitch() {
  const db = useDb();
  const [isPaused, setPaused] = useState(false);
  const [isSwitching, setSwitching] = useState(false);
  const hasNetwork = useSyncExternalStore(
    subscribeToNetwork,
    () => navigator.onLine,
    () => true,
  );
  return (
    <Switch
      label={hasNetwork ? "Sync" : "Sync (no network)"}
      size="sm"
      value={hasNetwork && !isPaused}
      isDisabled={!hasNetwork || isSwitching}
      changeAction={async (next) => {
        setSwitching(true);
        try {
          if (next) await db.reconnect();
          else await db.disconnect();
          setPaused(!next);
        } finally {
          setSwitching(false);
        }
      }}
    />
  );
}

function subscribeToNetwork(onChange: () => void) {
  window.addEventListener("online", onChange);
  window.addEventListener("offline", onChange);
  return () => {
    window.removeEventListener("online", onChange);
    window.removeEventListener("offline", onChange);
  };
}
