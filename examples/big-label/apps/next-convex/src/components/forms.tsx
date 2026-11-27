"use client";

import { useState } from "react";
import { DateInput, Selector, TextInput } from "@astryxdesign/core";
import { useConvex, useMutation, useQuery } from "convex/react";
import { ConvexError } from "convex/values";
import { api } from "../../convex/_generated/api";
import type { Id } from "../../convex/_generated/dataModel";
import { artistStatuses, genres, releaseFormats, releaseStatuses, searchKey } from "../fixtures";
import { useOrganization } from "../lib/organization";
import { useWrite } from "../lib/use-write";
import { roleLabels, type Role } from "../roles";
import { FormDialog } from "./form-dialog";

type DialogProps = { isOpen: boolean; onOpenChange: (isOpen: boolean) => void };
type ISODate = `${number}${number}${number}${number}-${number}${number}-${number}${number}`;

export type ArtistFields = { _id: Id<"artists">; name: string; genre: string; status: string };
export type ReleaseFields = {
  _id: Id<"releases">;
  artistId: string;
  catalogueId?: string | null;
  catalogNumber: string;
  title: string;
  format: string;
  releaseDate: number;
  status: string;
};

const options = (values: readonly string[]) =>
  values.map((value) => ({ value, label: value[0]!.toUpperCase() + value.slice(1) }));

export function ArtistDialog({
  artist,
  onCreated,
  ...dialog
}: DialogProps & { artist?: ArtistFields; onCreated?: (id: string) => void }) {
  const write = useWrite();
  const organization = useOrganization();
  const createArtist = useMutation(api.artists.create);
  const updateArtist = useMutation(api.artists.update);
  const [name, setName] = useState(artist?.name ?? "");
  const [genre, setGenre] = useState(artist?.genre ?? genres[0]!);
  const [status, setStatus] = useState(artist?.status ?? "developing");

  const save = () => {
    const fields = { name: name.trim(), genre, status, searchKey: searchKey(name.trim(), genre) };
    if (artist) {
      write("Couldn't save the artist", () =>
        updateArtist({ organizationId: organization.id, artistId: artist._id, ...fields }),
      );
      return;
    }
    write("Couldn't add the artist", async () =>
      onCreated?.(await createArtist({ organizationId: organization.id, ...fields })),
    );
  };

  return (
    <FormDialog
      {...dialog}
      title={artist ? "Edit artist" : "Add artist"}
      submitLabel={artist ? "Save" : "Add artist"}
      canSubmit={name.trim().length > 0}
      onSubmit={save}
    >
      <TextInput label="Name" value={name} onChange={setName} isRequired hasAutoFocus />
      <Selector label="Genre" options={options(genres)} value={genre} onChange={setGenre} />
      <Selector
        label="Status"
        options={options(artistStatuses)}
        value={status}
        onChange={setStatus}
      />
    </FormDialog>
  );
}

export function ReleaseDialog({
  release,
  artistId: presetArtistId,
  onCreated,
  ...dialog
}: DialogProps & {
  release?: ReleaseFields;
  artistId?: string;
  onCreated?: (id: string) => void;
}) {
  const write = useWrite();
  const organization = useOrganization();
  const convex = useConvex();
  const saveRelease = useMutation(api.releases.save);
  const artists = useQuery(api.artists.options, { organizationId: organization.id }) ?? [];
  const catalogues = useQuery(api.catalogues.list, { organizationId: organization.id }) ?? [];
  const [title, setTitle] = useState(release?.title ?? "");
  const [artistId, setArtistId] = useState(release?.artistId ?? presetArtistId ?? "");
  const [catalogueId, setCatalogueId] = useState<string | null>(release?.catalogueId ?? null);
  const [catalogNumber, setCatalogNumber] = useState(release?.catalogNumber ?? "");
  const [format, setFormat] = useState(release?.format ?? "Single");
  const [releaseDate, setReleaseDate] = useState<ISODate | undefined>(
    toISODate(release ? new Date(release.releaseDate) : new Date()),
  );
  const [status, setStatus] = useState(release?.status ?? "planning");
  const [numberError, setNumberError] = useState<string>();

  // Suggest the number after the highest one in the chosen catalogue.
  const chooseCatalogue = async (id: string | null) => {
    setCatalogueId(id);
    const catalogue = catalogues.find((entry) => entry._id === id);
    if (!catalogue || release) return;
    setCatalogNumber(
      await convex.query(api.releases.nextCatalogNumber, {
        organizationId: organization.id,
        catalogueId: catalogue._id,
      }),
    );
    setNumberError(undefined);
  };

  // saveRelease's transaction checks the number: a taken one keeps the
  // dialog open with the error; any other failure is reported.
  const submit = () => {
    const number = catalogNumber.trim();
    void saveRelease({
      organizationId: organization.id,
      releaseId: release?._id,
      title: title.trim(),
      artistId: artistId as Id<"artists">,
      catalogueId: (catalogueId as Id<"catalogues"> | null) ?? null,
      catalogNumber: number,
      format,
      releaseDate: Date.parse(`${releaseDate}T00:00:00.000Z`),
      status,
      searchKey: searchKey(title.trim(), number),
    }).then(
      (id) => {
        dialog.onOpenChange(false);
        if (!release) onCreated?.(id);
      },
      (error: unknown) => {
        if (error instanceof ConvexError && error.data?.code === "catalog_number_taken") {
          setNumberError(`${number} is already in use`);
          return;
        }
        write(release ? "Couldn't save the release" : "Couldn't add the release", () =>
          Promise.reject(error),
        );
      },
    );
    return false;
  };

  return (
    <FormDialog
      {...dialog}
      title={release ? "Edit release" : "Add release"}
      submitLabel={release ? "Save" : "Add release"}
      canSubmit={Boolean(title.trim() && artistId && catalogNumber.trim() && releaseDate)}
      onSubmit={submit}
    >
      <TextInput label="Title" value={title} onChange={setTitle} isRequired hasAutoFocus />
      <Selector
        label="Artist"
        options={artists.map((artist) => ({ value: artist._id, label: artist.name }))}
        value={artistId}
        onChange={setArtistId}
        placeholder="Choose an artist"
        hasSearch
        isRequired
      />
      <Selector
        label="Catalogue"
        options={catalogues.map((catalogue) => ({
          value: catalogue._id,
          label: `${catalogue.name} (${catalogue.code})`,
        }))}
        value={catalogueId}
        onChange={(id) => void chooseCatalogue(id)}
        placeholder="No catalogue"
        hasClear
        isOptional
      />
      <TextInput
        label="Catalogue number"
        value={catalogNumber}
        onChange={(value) => {
          setCatalogNumber(value);
          setNumberError(undefined);
        }}
        description="Unique within the label, for example NFC-012."
        status={numberError ? { type: "error", message: numberError } : undefined}
        isRequired
      />
      <Selector
        label="Format"
        options={options(releaseFormats)}
        value={format}
        onChange={setFormat}
      />
      <DateInput label="Release date" value={releaseDate} onChange={setReleaseDate} isRequired />
      <Selector
        label="Status"
        options={options(releaseStatuses)}
        value={status}
        onChange={setStatus}
      />
    </FormDialog>
  );
}

export function TeamDialog({
  team,
  onCreated,
  ...dialog
}: DialogProps & { team?: { _id: Id<"teams">; name: string }; onCreated?: (id: string) => void }) {
  const write = useWrite();
  const organization = useOrganization();
  const createTeam = useMutation(api.teams.create);
  const renameTeam = useMutation(api.teams.rename);
  const [name, setName] = useState(team?.name ?? "");
  const save = () => {
    if (team) {
      write("Couldn't rename the team", () =>
        renameTeam({ organizationId: organization.id, teamId: team._id, name: name.trim() }),
      );
      return;
    }
    write("Couldn't create the team", async () =>
      onCreated?.(await createTeam({ organizationId: organization.id, name: name.trim() })),
    );
  };
  return (
    <FormDialog
      {...dialog}
      title={team ? "Rename team" : "Create team"}
      submitLabel={team ? "Save" : "Create team"}
      canSubmit={name.trim().length > 0}
      onSubmit={save}
    >
      <TextInput label="Team name" value={name} onChange={setName} isRequired hasAutoFocus />
    </FormDialog>
  );
}

export function CatalogueDialog({
  catalogue,
  ...dialog
}: DialogProps & { catalogue?: { _id: Id<"catalogues">; name: string; code: string } }) {
  const write = useWrite();
  const organization = useOrganization();
  const createCatalogue = useMutation(api.catalogues.create);
  const updateCatalogue = useMutation(api.catalogues.update);
  const [name, setName] = useState(catalogue?.name ?? "");
  const [code, setCode] = useState(catalogue?.code ?? "");
  const save = () => {
    const fields = { name: name.trim(), code: code.trim().toUpperCase() };
    if (catalogue) {
      write("Couldn't save the catalogue", () =>
        updateCatalogue({ organizationId: organization.id, catalogueId: catalogue._id, ...fields }),
      );
      return;
    }
    write("Couldn't add the catalogue", () =>
      createCatalogue({ organizationId: organization.id, ...fields }),
    );
  };
  return (
    <FormDialog
      {...dialog}
      title={catalogue ? "Edit catalogue" : "Add catalogue"}
      submitLabel={catalogue ? "Save" : "Add catalogue"}
      canSubmit={Boolean(name.trim() && code.trim())}
      onSubmit={save}
    >
      <TextInput label="Name" value={name} onChange={setName} isRequired hasAutoFocus />
      <TextInput
        label="Code"
        value={code}
        onChange={setCode}
        description="Prefix for catalogue numbers, for example NFC."
        isRequired
      />
    </FormDialog>
  );
}

/**
 * Admins add someone who has signed in to BigLabel, by their sign-in email.
 * Emails are private, so the server looks them up (`members.addByEmail`) and
 * checks that the caller is an admin. New members can't be admins.
 */
export function AddMemberDialog(dialog: DialogProps) {
  const organization = useOrganization();
  const addByEmail = useMutation(api.members.addByEmail);
  const [email, setEmail] = useState("");
  const [role, setRole] = useState<Role>("editor");
  const [error, setError] = useState<string>();
  const [isPending, setIsPending] = useState(false);
  const submit = () => {
    setIsPending(true);
    setError(undefined);
    void addMember(addByEmail, { organizationId: organization.id, email, role })
      .then((message) => {
        if (message) setError(message);
        else dialog.onOpenChange(false);
      })
      .finally(() => setIsPending(false));
    return false;
  };
  return (
    <FormDialog
      {...dialog}
      title="Add member"
      submitLabel="Add member"
      canSubmit={Boolean(email.trim()) && !isPending}
      onSubmit={submit}
    >
      <TextInput
        label="Email"
        type="email"
        value={email}
        onChange={(value) => {
          setEmail(value);
          setError(undefined);
        }}
        description="The email they sign in to BigLabel with."
        status={error ? { type: "error", message: error } : undefined}
        isRequired
        hasAutoFocus
      />
      <Selector
        label="Role"
        options={(["editor", "viewer"] as const).map((value) => ({
          value,
          label: roleLabels[value],
        }))}
        value={role}
        onChange={(value) => setRole(value as Role)}
        description="Promote members to admin from the member list once they have joined."
      />
    </FormDialog>
  );
}

const addMemberErrors: Record<string, string> = {
  "not-found": "Nobody has signed in to BigLabel with that email yet.",
  "already-member": "They're already a member of this label.",
  forbidden: "Only admins can add members.",
};

/** Returns an error message, or nothing when the member was added. */
async function addMember(
  addByEmail: (input: { organizationId: Id<"organizations">; email: string; role: Role }) => Promise<{
    status: string;
  }>,
  input: { organizationId: Id<"organizations">; email: string; role: Role },
) {
  try {
    const { status } = await addByEmail(input);
    if (status === "added") return undefined;
    return addMemberErrors[status] ?? `The member couldn't be added (${status}).`;
  } catch (error) {
    return `The member couldn't be added (${error instanceof Error ? error.message : String(error)}).`;
  }
}

function toISODate(date: Date) {
  return date.toISOString().slice(0, 10) as ISODate;
}
