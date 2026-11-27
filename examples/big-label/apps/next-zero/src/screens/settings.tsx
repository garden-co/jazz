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
import { useZero } from "@rocicorp/zero/react";
import { mutators } from "../../mutators";
import { PageHeader, PageSection } from "../components/page";
import { fixtureProfiles } from "../fixtures";
import { useCan, useOrganization } from "../lib/organization";
import { useWrite } from "../lib/use-write";
import { confirmed } from "../lib/write-errors";

type DemoResult = { created: boolean; organizationIds: string[] };

export function SettingsPage() {
  const organization = useOrganization();
  const canEdit = useCan("editSettings");
  const db = useZero();
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
                  confirmed(db.mutate(mutators.organizations.rename({ id: organization.id, name: name.trim() }))),
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
      const response = await fetch("/api/demo-data", {
        method: "POST",
        credentials: "same-origin",
        headers: { "content-type": "application/json" },
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
