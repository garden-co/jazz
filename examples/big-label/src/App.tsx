import type { DbConfig } from "jazz-tools";
import { JazzProvider, useAll, useDb, useSession } from "jazz-tools/react";
import { app } from "../schema.js";
import { tenantOperations } from "./scenarios.js";
import "./app.css";

const config: DbConfig = {
  appId: import.meta.env.VITE_JAZZ_APP_ID,
  env: "dev",
  serverUrl: import.meta.env.VITE_JAZZ_SERVER_URL,
  secret: import.meta.env.VITE_JAZZ_SECRET,
};
export function App() {
  return (
    <JazzProvider config={config} fallback={<main>Connecting to BigLabel…</main>}>
      <Operations />
    </JazzProvider>
  );
}

function Operations() {
  const session = useSession();
  const db = useDb();
  const { data: memberships = [] } = useAll(
    app.memberships
      .where({ userId: session?.user_id ?? "__none__" })
      .include({ organization: true }),
  );
  const organization = memberships[0]?.organization as { id: string; name: string } | undefined;
  const { data: artists = [] } = useAll(
    app.artists.where({ organizationId: organization?.id ?? "__none__" }),
  );
  const { data: releases = [] } = useAll(
    app.releases
      .where({ organizationId: organization?.id ?? "__none__" })
      .include({ artist: true }),
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
  const receipt = {
    ...tenantOperations("small"),
    organizationId: organization.id,
    visibleArtists: artists.length,
    visibleReleases: releases.length,
  };
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
        <span className="pill">{session?.user_id ? "connected" : "connecting"}</span>
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
        Live receipt: {receipt.visibleArtists} owned artists, {receipt.visibleReleases} related
        releases, {receipt.foreignRows} foreign rows.
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
