"use client";

import { useEffect, useState } from "react";
import {
  AppShell,
  Banner,
  Button,
  Center,
  EmptyState,
  LayoutContent,
  NavHeadingMenu,
  NavHeadingMenuItem,
  SideNav,
  SideNavHeading,
  SideNavItem,
  SideNavSection,
  Spinner,
  Text,
  TopNav,
  TopNavHeading,
} from "@astryxdesign/core";
import { useAll, useSession } from "jazz-tools/react";
import { app } from "../schema";
import { OrganizationProvider, type CurrentOrganization } from "./lib/organization";
import { href, useRoute, type Route } from "./lib/route";
import { isRole, roleLabels } from "./roles";
import { ArtistPage, ArtistsPage } from "./screens/artists";
import { CataloguePage, CataloguesPage } from "./screens/catalogues";
import { OverviewPage } from "./screens/overview";
import { PeoplePage } from "./screens/people";
import { ReleasePage, ReleasesPage } from "./screens/releases";
import { SettingsPage } from "./screens/settings";
import { TeamPage, TeamsPage } from "./screens/teams";

const selectedOrganizationKey = "big-label-organization";

const navigation = [
  { label: "Overview", href: href.overview, pages: ["overview"] },
  { label: "Artists", href: href.artists, pages: ["artists", "artist"] },
  { label: "Releases", href: href.releases, pages: ["releases", "release"] },
  { label: "Catalogues", href: href.catalogues, pages: ["catalogues", "catalogue"] },
  { label: "Teams", href: href.teams, pages: ["teams", "team"] },
  { label: "People", href: href.people, pages: ["people"] },
  { label: "Settings", href: href.settings, pages: ["settings"] },
] as const;

export function Operations({
  email,
  preparing = false,
  setupError = null,
  onSignOut,
}: {
  email: string;
  /** The server is still creating the personal label. */
  preparing?: boolean;
  /** Setting up the personal label failed; shown as a banner with a retry. */
  setupError?: { message: string; retry: () => void } | null;
  onSignOut: () => void;
}) {
  const session = useSession();
  const route = useRoute();
  const account = session?.user.account;
  // Every label this account belongs to: an indexed lookup on the denormalized userId.
  const { data: memberships, isLoading } = useAll(
    account
      ? app.memberships.where({ userId: account }).include({ organization: true }).limit(100)
      : undefined,
  );
  const organizations: CurrentOrganization[] = (memberships ?? [])
    .flatMap((membership) =>
      membership.organization && isRole(membership.role)
        ? [
            {
              id: membership.organization.id,
              name: membership.organization.name,
              membershipId: membership.id,
              personId: membership.personId,
              role: membership.role,
            },
          ]
        : [],
    )
    .sort((a, b) => a.name.localeCompare(b.name));
  const [selectedId, setSelectedId] = useState<string | null>(null);
  useEffect(() => {
    try {
      setSelectedId(localStorage.getItem(selectedOrganizationKey));
    } catch {}
  }, []);
  const selectOrganization = (id: string) => {
    setSelectedId(id);
    try {
      localStorage.setItem(selectedOrganizationKey, id);
    } catch {}
    window.location.hash = href.overview;
  };
  const organization = organizations.find((entry) => entry.id === selectedId) ?? organizations[0];

  const topNav = (
    <TopNav
      label="BigLabel"
      heading={<TopNavHeading heading="BigLabel" headingHref={href.overview} />}
      endContent={
        <>
          <Text type="supporting" color="secondary" maxLines={1} className="account-email">
            {email}
          </Text>
          <Button label="Sign out" variant="ghost" size="sm" onClick={onSignOut} />
        </>
      }
    />
  );

  const setupBanner = setupError ? (
    <Banner
      status="error"
      title="Couldn't set up your personal label"
      description={setupError.message}
      endContent={<Button label="Try again" size="sm" onClick={setupError.retry} />}
    />
  ) : null;

  if (!organization)
    return (
      <AppShell topNav={topNav} height="auto" variant="section">
        <LayoutContent padding={8}>
          {setupBanner}
          {setupBanner ? null : preparing ? (
            <Center>
              <Spinner label="Preparing your personal label…" />
            </Center>
          ) : (
            !isLoading && (
              <EmptyState
                title="No label yet"
                description="Your personal label is created when you first sign in. Reload to try again."
              />
            )
          )}
        </LayoutContent>
      </AppShell>
    );

  return (
    <AppShell
      topNav={topNav}
      height="auto"
      variant="section"
      sideNav={
        <SideNav
          header={
            <SideNavHeading
              heading={organization.name}
              subheading={roleLabels[organization.role]}
              menu={
                <NavHeadingMenu>
                  {organizations.map((entry) => (
                    <NavHeadingMenuItem
                      key={entry.id}
                      label={entry.name}
                      description={roleLabels[entry.role]}
                      onClick={() => selectOrganization(entry.id)}
                    />
                  ))}
                </NavHeadingMenu>
              }
            />
          }
        >
          <SideNavSection title="Label" isHeaderHidden>
            {navigation.map((item) => (
              <SideNavItem
                key={item.label}
                label={item.label}
                href={item.href}
                isSelected={(item.pages as readonly string[]).includes(route.page)}
              />
            ))}
          </SideNavSection>
        </SideNav>
      }
    >
      <LayoutContent padding={8} isScrollable={false}>
        {setupBanner}
        <OrganizationProvider organization={organization}>
          <Page key={organization.id} route={route} />
        </OrganizationProvider>
      </LayoutContent>
    </AppShell>
  );
}

function Page({ route }: { route: Route }) {
  switch (route.page) {
    case "overview":
      return <OverviewPage />;
    case "artists":
      return <ArtistsPage />;
    case "artist":
      return <ArtistPage key={route.id} id={route.id} />;
    case "releases":
      return <ReleasesPage />;
    case "release":
      return <ReleasePage key={route.id} id={route.id} />;
    case "catalogues":
      return <CataloguesPage />;
    case "catalogue":
      return <CataloguePage key={route.id} id={route.id} />;
    case "teams":
      return <TeamsPage />;
    case "team":
      return <TeamPage key={route.id} id={route.id} />;
    case "people":
      return <PeoplePage />;
    case "settings":
      return <SettingsPage />;
  }
}
