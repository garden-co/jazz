"use client";

import { useState } from "react";
import { DateInput, Selector, TextInput } from "@astryxdesign/core";
import { id as newId } from "@instantdb/react";
import { db } from "../lib/db";
import { authHeaders } from "../lib/auth-client";
import { artistStatuses, genres, releaseFormats, releaseStatuses, searchKey } from "../fixtures";
import { nextCatalogNumber, saveRelease } from "../lib/mutations";
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
  const write = useWrite();
  const organization = useOrganization();
  const [name, setName] = useState(artist?.name ?? "");
  const [genre, setGenre] = useState(artist?.genre ?? genres[0]!);
  const [status, setStatus] = useState(artist?.status ?? "developing");

  const save = () => {
    const fields = { name: name.trim(), genre, status, searchKey: searchKey(name.trim(), genre) };
    if (artist) {
      write("Couldn't save the artist", () => db.transact(db.tx.artists[artist.id].update(fields)));
      return;
    }
    const id = newId();
    write("Couldn't add the artist", () =>
      db.transact(db.tx.artists[id].create(fields).link({ organization: organization.id })),
    );
    onCreated?.(id);
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
  const { data } = db.useQuery({
    artists: {
      $: { where: { organization: organization.id }, order: { name: "asc" }, limit: 500 },
    },
    catalogues: { $: { where: { organization: organization.id }, order: { name: "asc" } } },
  });
  const artists = data?.artists ?? [];
  const catalogues = data?.catalogues ?? [];
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

  // Suggest the number after the highest one in the chosen catalogue.
  const chooseCatalogue = async (id: string | null) => {
    setCatalogueId(id);
    const catalogue = catalogues.find((entry) => entry.id === id);
    if (!catalogue || release) return;
    setCatalogNumber(await nextCatalogNumber(catalogue));
    setNumberError(undefined);
  };

  const save = () => {
    const number = catalogNumber.trim();
    const id = release?.id ?? newId();
    const commit = saveRelease(
      organization.id,
      { id, isNew: !release, artistId: release?.artistId, catalogueId: release?.catalogueId },
      {
        title: title.trim(),
        artistId,
        catalogueId,
        catalogNumber: number,
        format,
        releaseDate: new Date(`${releaseDate}T00:00:00.000Z`),
        status,
        searchKey: searchKey(title.trim(), number),
      },
    );
    write(release ? "Couldn't save the release" : "Couldn't add the release", () => commit);
    if (!release) onCreated?.(id);
  };

  // A read gives quick feedback; the unique catalogue key is the real check.
  const submit = () => {
    const number = catalogNumber.trim();
    void db
      .queryOnce({
        releases: {
          $: { where: { organization: organization.id, catalogNumber: number }, fields: ["id"] },
        },
      })
      .then(({ data: { releases: taken } }) => {
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
  const write = useWrite();
  const organization = useOrganization();
  const [name, setName] = useState(team?.name ?? "");
  const save = () => {
    if (team) {
      write("Couldn't rename the team", () =>
        db.transact(db.tx.teams[team.id].update({ name: name.trim() })),
      );
      return;
    }
    const id = newId();
    write("Couldn't create the team", () =>
      db.transact(
        db.tx.teams[id].create({ name: name.trim() }).link({ organization: organization.id }),
      ),
    );
    onCreated?.(id);
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
  const write = useWrite();
  const organization = useOrganization();
  const [name, setName] = useState(catalogue?.name ?? "");
  const [code, setCode] = useState(catalogue?.code ?? "");
  const save = () => {
    const fields = { name: name.trim(), code: code.trim().toUpperCase() };
    if (catalogue) {
      write("Couldn't save the catalogue", () =>
        db.transact(db.tx.catalogues[catalogue.id].update(fields)),
      );
      return;
    }
    write("Couldn't add the catalogue", () =>
      db.transact(db.tx.catalogues[newId()].create(fields).link({ organization: organization.id })),
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
 * Emails are private, so the server looks them up (`POST /api/members`) and
 * checks that the caller is an admin. New members can't be admins.
 */
export function AddMemberDialog(dialog: DialogProps) {
  const organization = useOrganization();
  const [email, setEmail] = useState("");
  const [role, setRole] = useState<Role>("editor");
  const [error, setError] = useState<string>();
  const [isPending, setIsPending] = useState(false);
  const user = db.useAuth().user;
  const submit = () => {
    setIsPending(true);
    setError(undefined);
    void addMember({ organizationId: organization.id, email, role }, user ? authHeaders(user) : {})
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
  input: { organizationId: string; email: string; role: Role },
  headers: Record<string, string>,
) {
  // The Instant token is the only credential the route needs.
  const response = await fetch("/api/members", {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(input),
  });
  if (response.ok) return undefined;
  const { status } = (await response.json().catch(() => ({}))) as { status?: string };
  return addMemberErrors[status ?? ""] ?? `The member couldn't be added (${response.status}).`;
}

function toISODate(date: Date) {
  return date.toISOString().slice(0, 10) as ISODate;
}
