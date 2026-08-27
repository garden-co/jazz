"use client";

import { useAll, useDb, useSession } from "jazz-tools/react";
import { app } from "../schema";

export function Operations({ onSignOut }: { onSignOut: () => void }) {
  const session = useSession();
  const db = useDb();
  const { data: memberships = [] } = useAll(
    app.memberships
      .where({ userId: session?.user ?? "__none__" })
      .include({ organization: true })
      .limit(50),
  );
  const organization = memberships[0]?.organization as { id: string; name: string } | undefined;
  const { data: artists = [] } = useAll(
    app.artists.where({ organizationId: organization?.id ?? "__none__" }).limit(100),
  );
  const { data: releases = [] } = useAll(
    app.releases
      .where({ organizationId: organization?.id ?? "__none__" })
      .include({ artist: true })
      .limit(100),
  );
  if (!organization)
    return (
      <main className="shell">
        <h1>BigLabel</h1>
        <p>
          No organization is assigned to this identity. Provision an organization and its first
          admin through the server-authorized bootstrap flow.
        </p>
      </main>
    );
  const addArtist = () =>
    void db.insert(app.artists, {
      organizationId: organization.id,
      name: `New artist ${artists.length + 1}`,
      genre: "Electronic",
      status: "developing",
    });
  return (
    <main className="shell">
      <header>
        <div>
          <small>BIGLABEL / OPERATIONS</small>
          <h1>{organization.name}</h1>
          <p>Artists, releases, teams, and tenant-safe workflows.</p>
        </div>
        <div>
          <span className="pill">{session?.user ? "connected" : "connecting"}</span>
          <button onClick={onSignOut}>Sign out</button>
        </div>
      </header>
      <section className="metrics">
        <Metric label="Artists" value={artists.length} />
        <Metric
          label="Scheduled releases"
          value={releases.filter((r) => r.status === "scheduled").length}
        />
        <Metric label="Team members" value={memberships.length} />
      </section>
      <section className="grid">
        <article>
          <h2>
            Artist roster <button onClick={addArtist}>Add artist</button>
          </h2>
          {artists.map((artist) => (
            <div className="row" key={artist.id}>
              <strong>{artist.name}</strong>
              <span>{artist.genre}</span>
              <em>{artist.status}</em>
            </div>
          ))}
        </article>
        <article>
          <h2>Release pipeline</h2>
          {releases.map((release) => (
            <div className="row" key={release.id}>
              <strong>{release.title}</strong>
              <span>{(release.artist as { name?: string } | undefined)?.name}</span>
              <em>{release.status}</em>
            </div>
          ))}
        </article>
      </section>
      <footer>
        Live receipt: {artists.length} owned artists and {releases.length} related releases.
      </footer>
    </main>
  );
}
function Metric({ label, value }: { label: string; value: number }) {
  return (
    <div>
      <small>{label}</small>
      <b>{value}</b>
    </div>
  );
}
