"use client";

import { useState } from "react";
import {
  Banner,
  Button,
  HStack,
  SegmentedControl,
  SegmentedControlItem,
  Text,
  TextInput,
  VStack,
} from "@astryxdesign/core";
import { useDb } from "jazz-tools/react";
import { app } from "../../schema";
import { PageHeader, PageSection } from "../components/page";
import { fixtureProfiles } from "../fixtures";
import { getJwtFromBetterAuth } from "../lib/auth-client";
import { useCan, useOrganization } from "../lib/organization";
import { useWrite } from "../lib/use-write";

type DemoResult = { created: boolean; organizationIds: string[] };

export function SettingsPage() {
  const organization = useOrganization();
  const canEdit = useCan("editSettings");
  const db = useDb();
  const write = useWrite();
  const [name, setName] = useState(organization.name);

  return (
    <VStack gap={8}>
      <PageHeader title="Settings" />
      <PageSection title="Label">
        <HStack gap={2} vAlign="end" wrap="wrap">
          <TextInput
            label="Label name"
            value={name}
            onChange={setName}
            isDisabled={!canEdit}
            disabledMessage="Admins can rename the label"
            width={320}
          />
          {canEdit && (
            <Button
              label="Save"
              variant="secondary"
              isDisabled={!name.trim() || name.trim() === organization.name}
              onClick={() =>
                write("Couldn't rename the label", () =>
                  db
                    .update(app.organizations, organization.id, { name: name.trim() })
                    .wait({ tier: "global" }),
                )
              }
            />
          )}
        </HStack>
      </PageSection>
      <DemoData />
    </VStack>
  );
}

/**
 * Loads a deterministic `createFixture` profile as extra labels you
 * administer. The server creates them: browsers can't create organizations.
 */
function DemoData() {
  const [profile, setProfile] = useState<"smoke" | "small">("small");
  const [state, setState] = useState<
    | { status: "idle" }
    | { status: "loading" }
    | { status: "done"; result: DemoResult }
    | { status: "error"; message: string }
  >({ status: "idle" });
  const size = fixtureProfiles[profile];

  const load = async () => {
    setState({ status: "loading" });
    try {
      const token = await getJwtFromBetterAuth();
      if (!token) throw new Error("Your session has expired. Sign in again.");
      const response = await fetch("/api/demo-data", {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify({ profile }),
      });
      if (!response.ok) throw new Error(`The server couldn't load the data (${response.status}).`);
      setState({ status: "done", result: (await response.json()) as DemoResult });
    } catch (error) {
      setState({
        status: "error",
        message: error instanceof Error ? error.message : String(error),
      });
    }
  };

  return (
    <PageSection title="Demo data">
      <Text color="secondary">
        Load a deterministic fixture as extra labels with artists, releases, catalogues, teams and
        members. You become the admin of each one; switch between them from the label menu.
      </Text>
      <SegmentedControl
        label="Fixture size"
        value={profile}
        onChange={(value) => setProfile(value as "smoke" | "small")}
      >
        <SegmentedControlItem value="smoke" label="Smoke" />
        <SegmentedControlItem value="small" label="Small" />
      </SegmentedControl>
      <Text type="supporting" color="secondary">
        {size.organizations} labels, each with {size.membersPerOrganization} members,{" "}
        {size.artistsPerOrganization} artists and {size.artistsPerOrganization} releases.
      </Text>
      <HStack>
        <Button
          label="Load demo data"
          variant="secondary"
          isLoading={state.status === "loading"}
          onClick={() => void load()}
        />
      </HStack>
      {state.status === "done" && (
        <Banner
          status="success"
          title={
            state.result.created
              ? `Loaded ${state.result.organizationIds.length} demo labels`
              : "This demo data is already loaded"
          }
          description="Pick a label from the menu at the top of the side navigation."
        />
      )}
      {state.status === "error" && (
        <Banner status="error" title="Demo data didn't load" description={state.message} />
      )}
    </PageSection>
  );
}
