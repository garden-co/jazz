import { JazzProvider, useAll, useSession } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { app } from "../schema.js";
import { createFixture } from "./fixtures.js";
import { tenantOperations } from "./scenarios.js";
import "./app.css";

const demo = createFixture("small");
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
  const organization = demo.organizations[0]!;
  // This exact shape is the live indexed tenant query; fixtures keep the empty first-run screen useful.
  const { data: liveArtists = [] } = useAll(app.artists.where({ organizationId: organization.id }));
  const artists = liveArtists.length
    ? liveArtists
    : demo.artists.filter((artist) => artist.organizationId === organization.id);
  const releases = demo.releases.filter((release) => release.organizationId === organization.id);
  const receipt = tenantOperations("small");
  return (
    <main className="shell">
      <header>
        <div>
          <small>BIGLABEL / OPERATIONS</small>
          <h1>{organization.name}</h1>
          <p>Artists, releases, teams, and tenant-safe workflows.</p>
        </div>
        <span className="pill">{session?.user_id ? "connected" : "demo fixture"}</span>
      </header>
      <section className="metrics">
        <Metric label="Artists" value={artists.length} />
        <Metric
          label="Scheduled releases"
          value={releases.filter((r) => r.status === "scheduled").length}
        />
        <Metric
          label="Team members"
          value={demo.memberships.filter((m) => m.organizationId === organization.id).length}
        />
      </section>
      <section className="grid">
        <article>
          <h2>Artist roster</h2>
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
              <span>{demo.artists.find((artist) => artist.id === release.artistId)?.name}</span>
              <em>{release.status}</em>
            </div>
          ))}
        </article>
      </section>
      <footer>
        Scenario receipt: {receipt.visibleArtists} owned artists, {receipt.visibleReleases} related
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
