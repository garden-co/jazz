"use client";

import { useState } from "react";
import { DateInput, Selector, TextInput } from "@astryxdesign/core";
import { useAll, useDb } from "jazz-tools/react";
import { app } from "../../schema";
import {
  artistStatuses,
  formatCatalogNumber,
  genres,
  releaseFormats,
  releaseStatuses,
  searchKey,
} from "../fixtures";
import { useOrganization } from "../lib/organization";
import { useWrite } from "../lib/use-write";
import { roleLabels, type Role } from "../roles";
import { FormDialog } from "./form-dialog";

type DialogProps = { isOpen: boolean; onOpenChange: (isOpen: boolean) => void };
type ISODate = `${number}${number}${number}${number}-${number}${number}-${number}${number}`;

export type ArtistFields = { id: string; name: string; genre: string; status: string };
export type ReleaseFields = {
  id: string;
  artistId: string;
  catalogueId?: string | null;
  catalogNumber: string;
  title: string;
  format: string;
  releaseDate: Date;
  status: string;
};

const options = (values: readonly string[]) =>
  values.map((value) => ({ value, label: value[0]!.toUpperCase() + value.slice(1) }));

export function ArtistDialog({
  artist,
  onCreated,
  ...dialog
}: DialogProps & { artist?: ArtistFields; onCreated?: (id: string) => void }) {
  const db = useDb();
  const write = useWrite();
  const organization = useOrganization();
  const [name, setName] = useState(artist?.name ?? "");
  const [genre, setGenre] = useState(artist?.genre ?? genres[0]!);
  const [status, setStatus] = useState(artist?.status ?? "developing");

  const save = () => {
    const fields = { name: name.trim(), genre, status, searchKey: searchKey(name.trim(), genre) };
    if (artist) {
      write("Couldn't save the artist", () =>
        db.update(app.artists, artist.id, fields).wait({ tier: "global" }),
      );
      return;
    }
    const created = db.insert(app.artists, { ...fields, organizationId: organization.id });
    write("Couldn't add the artist", () => created.wait({ tier: "global" }));
    onCreated?.(created.value.id);
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
  const db = useDb();
  const write = useWrite();
  const organization = useOrganization();
  const { data: artists = [] } = useAll(
    app.artists.where({ organizationId: organization.id }).orderBy("name", "asc").limit(500),
  );
  const { data: catalogues = [] } = useAll(
    app.catalogues.where({ organizationId: organization.id }).orderBy("name", "asc"),
  );
  const [title, setTitle] = useState(release?.title ?? "");
  const [artistId, setArtistId] = useState(release?.artistId ?? presetArtistId ?? "");
  const [catalogueId, setCatalogueId] = useState<string | null>(release?.catalogueId ?? null);
  const [catalogNumber, setCatalogNumber] = useState(release?.catalogNumber ?? "");
  const [format, setFormat] = useState(release?.format ?? "Single");
  const [releaseDate, setReleaseDate] = useState<ISODate | undefined>(
    toISODate(release?.releaseDate ?? new Date()),
  );
  const [status, setStatus] = useState(release?.status ?? "planning");
  const [numberError, setNumberError] = useState<string>();

  // Suggest the next free number in the chosen catalogue.
  const chooseCatalogue = async (id: string | null) => {
    setCatalogueId(id);
    const catalogue = catalogues.find((entry) => entry.id === id);
    if (!catalogue || release) return;
    const inCatalogue = await db.all(
      app.releases.where({ organizationId: organization.id, catalogueId: catalogue.id }),
    );
    setCatalogNumber(formatCatalogNumber(catalogue.code, inCatalogue.length + 1));
    setNumberError(undefined);
  };

  const save = () => {
    const number = catalogNumber.trim();
    const fields = {
      title: title.trim(),
      artistId,
      catalogueId,
      catalogNumber: number,
      format,
      releaseDate: new Date(`${releaseDate}T00:00:00.000Z`),
      status,
      searchKey: searchKey(title.trim(), number),
    };
    const id = release?.id ?? crypto.randomUUID();
    // Catalogue numbers are unique per label. The exclusive transaction makes
    // the authority re-check that no concurrent write took the number.
    const commit = db.exclusiveTransaction(async (tx) => {
      const taken = await tx.all(
        app.releases.where({ organizationId: organization.id, catalogNumber: number }),
      );
      if (taken.some((row) => row.id !== id)) throw new Error(`${number} is already in use`);
      if (release) tx.update(app.releases, id, fields);
      else tx.insert(app.releases, { ...fields, organizationId: organization.id }, { id });
    });
    write(release ? "Couldn't save the release" : "Couldn't add the release", async () =>
      (await commit).wait(),
    );
    if (!release) onCreated?.(id);
  };

  // A local read gives instant feedback; the transaction above is the real check.
  const submit = () => {
    const number = catalogNumber.trim();
    void db
      .all(app.releases.where({ organizationId: organization.id, catalogNumber: number }))
      .then((taken) => {
        if (taken.some((row) => row.id !== release?.id)) {
          setNumberError(`${number} is already in use`);
          return;
        }
        save();
        dialog.onOpenChange(false);
      });
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
        options={artists.map((artist) => ({ value: artist.id, label: artist.name }))}
        value={artistId}
        onChange={setArtistId}
        placeholder="Choose an artist"
        hasSearch
        isRequired
      />
      <Selector
        label="Catalogue"
        options={catalogues.map((catalogue) => ({
          value: catalogue.id,
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
}: DialogProps & { team?: { id: string; name: string }; onCreated?: (id: string) => void }) {
  const db = useDb();
  const write = useWrite();
  const organization = useOrganization();
  const [name, setName] = useState(team?.name ?? "");
  const save = () => {
    if (team) {
      write("Couldn't rename the team", () =>
        db.update(app.teams, team.id, { name: name.trim() }).wait({ tier: "global" }),
      );
      return;
    }
    const created = db.insert(app.teams, { organizationId: organization.id, name: name.trim() });
    write("Couldn't create the team", () => created.wait({ tier: "global" }));
    onCreated?.(created.value.id);
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
}: DialogProps & { catalogue?: { id: string; name: string; code: string } }) {
  const db = useDb();
  const write = useWrite();
  const organization = useOrganization();
  const [name, setName] = useState(catalogue?.name ?? "");
  const [code, setCode] = useState(catalogue?.code ?? "");
  const save = () => {
    const fields = { name: name.trim(), code: code.trim().toUpperCase() };
    if (catalogue) {
      write("Couldn't save the catalogue", () =>
        db.update(app.catalogues, catalogue.id, fields).wait({ tier: "global" }),
      );
      return;
    }
    write("Couldn't add the catalogue", () =>
      db
        .insert(app.catalogues, { ...fields, organizationId: organization.id })
        .wait({ tier: "global" }),
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

/** Admins add someone who already has a BigLabel profile. New members can't be admins. */
export function AddMemberDialog({
  memberPersonIds,
  ...dialog
}: DialogProps & { memberPersonIds: Set<string> }) {
  const db = useDb();
  const write = useWrite();
  const organization = useOrganization();
  const { data: people = [] } = useAll(app.people.orderBy("name", "asc").limit(500));
  const [personId, setPersonId] = useState("");
  const [role, setRole] = useState<Role>("editor");
  const candidates = people.filter((person) => !memberPersonIds.has(person.id));
  const save = () => {
    const person = candidates.find((entry) => entry.id === personId);
    if (!person) return false;
    write("Couldn't add the member", () =>
      db
        .insert(app.memberships, {
          organizationId: organization.id,
          personId: person.id,
          userId: person.userId,
          role,
        })
        .wait({ tier: "global" }),
    );
  };
  return (
    <FormDialog
      {...dialog}
      title="Add member"
      submitLabel="Add member"
      canSubmit={Boolean(personId)}
      onSubmit={save}
    >
      <Selector
        label="Person"
        options={candidates.map((person) => ({ value: person.id, label: person.name }))}
        value={personId}
        onChange={setPersonId}
        placeholder="Choose a person"
        description="People appear here once they have signed in to BigLabel."
        hasSearch
        isRequired
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

function toISODate(date: Date) {
  return date.toISOString().slice(0, 10) as ISODate;
}
