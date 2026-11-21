import { useState, useSyncExternalStore } from "react";
import { db } from "../model/db.js";
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
 * Shows whether Instant is connected. Instant has no public API to pause
 * syncing, so unlike the Jazz app's switch this one can't be flipped.
 */
function SyncSwitch() {
  const status = db.useConnectionStatus();
  const hasNetwork = useSyncExternalStore(
    subscribeToNetwork,
    () => navigator.onLine,
    () => true,
  );
  return (
    <Switch
      label={hasNetwork ? "Sync" : "Sync (no network)"}
      size="sm"
      value={hasNetwork && status === "authenticated"}
      isDisabled
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
