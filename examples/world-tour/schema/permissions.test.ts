import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { createPolicyTestApp, PolicyTestApp } from "jazz-tools/testing";
import { join } from "node:path";
import type { Session } from "jazz-tools/backend";
import { app } from "./app.js";
import type { VenueInit } from "./app.js";

const schemaDir = join(import.meta.dirname, ".");

let testApp: PolicyTestApp;

// Members: claims.auth_mode = "local" (matches isMember policy condition)
const alice: Session = { user_id: "alice", claims: { auth_mode: "local", local_mode: "demo" } };
const bob: Session = { user_id: "bob", claims: { auth_mode: "local", local_mode: "demo" } };
// Public visitor: no auth_mode claim, so isMember doesn't match
const publicVisitor: Session = { user_id: "public-visitor", claims: {} };

const boweryBallroom: VenueInit = {
  name: "Bowery Ballroom",
  city: "New York",
  country: "US",
  lat: 40.7204,
  lng: -73.9933,
  capacity: 575,
};

const roundhouse: VenueInit = {
  name: "Roundhouse",
  city: "London",
  country: "GB",
  lat: 51.5434,
  lng: -0.1525,
  capacity: 3300,
};

beforeAll(async () => {
  testApp = await createPolicyTestApp(schemaDir);
});

afterAll(async () => {
  await testApp?.shutdown();
});

describe("permission policies", () => {
  let bandId: string;
  let venueNYId: string;
  let venueLondonId: string;
  let confirmedStopId: string;

  beforeAll(() => {
    testApp.seed((db) => {
      const band = db.insert(app.bands, { name: "The Midnight" });
      bandId = band.id;

      const venueNY = db.insert(app.venues, boweryBallroom);
      venueNYId = venueNY.id;
      const venueLondon = db.insert(app.venues, roundhouse);
      venueLondonId = venueLondon.id;

      db.insert(app.members, { bandId, userId: "alice" });
      db.insert(app.members, { bandId, userId: "bob" });

      const confirmed = db.insert(app.stops, {
        bandId,
        venueId: venueNYId,
        date: new Date("2026-07-15T20:00:00Z"),
        status: "confirmed",
        publicDescription: "Summer tour opening night at the Bowery Ballroom",
        privateNotes: "Soundcheck at 16:00, rider requires extra monitors",
      });
      confirmedStopId = confirmed.id;

      db.insert(app.stops, {
        bandId,
        venueId: venueLondonId,
        date: new Date("2026-08-22T19:30:00Z"),
        status: "tentative",
        publicDescription: "London show \u200A\u2014\u200A pending venue confirmation",
        privateNotes: "Waiting on Roundhouse availability for late August",
      });

      db.insert(app.stops, {
        bandId,
        venueId: venueLondonId,
        date: new Date("2026-08-10T20:00:00Z"),
        status: "cancelled",
        publicDescription: "Originally scheduled Roundhouse date",
        privateNotes: "Cancelled due to scheduling conflict with festival",
      });
    });
  });

  // ── Stop read policies ──────────────────────────────────────────────

  describe("stop read access", () => {
    it("band member can read all stops including tentative and cancelled", async () => {
      const db = testApp.as(alice);
      const stops = await db.all(app.stops);
      expect(stops).toHaveLength(3);

      const statuses = stops.map((s: any) => s.status).sort();
      expect(statuses).toEqual(["cancelled", "confirmed", "tentative"]);
    });

    it("public visitor can only read confirmed stops", async () => {
      const db = testApp.as(publicVisitor);
      const stops = await db.all(app.stops);

      expect(stops).toHaveLength(1);
      expect(stops[0].status).toBe("confirmed");
    });
  });

  // ── Venue read policies ─────────────────────────────────────────────

  describe("venue read access", () => {
    it("band member can read all venues", async () => {
      const db = testApp.as(bob);
      const venues = await db.all(app.venues);
      expect(venues).toHaveLength(2);
    });

    it("public visitor can read all venues", async () => {
      const db = testApp.as(publicVisitor);
      const venues = await db.all(app.venues);
      expect(venues).toHaveLength(2);

      const names = venues.map((v: any) => v.name).sort();
      expect(names).toEqual(["Bowery Ballroom", "Roundhouse"]);
    });
  });

  // ── Stop mutation policies ──────────────────────────────────────────

  describe("stop mutations", () => {
    it("band member can insert a stop", () => {
      testApp.expectAllowed(() => {
        const db = testApp.as(alice);
        db.insert(app.stops, {
          bandId,
          venueId: venueNYId,
          date: new Date("2026-09-01T20:00:00Z"),
          status: "tentative",
          publicDescription: "Potential encore show at the Bowery",
        });
      });
    });

    it("band member can update a stop", () => {
      testApp.expectAllowed(() => {
        const db = testApp.as(bob);
        db.update(app.stops, confirmedStopId, {
          publicDescription: "Summer tour opening night \u200A\u2014\u200A SOLD OUT",
        });
      });
    });

    it("band member can delete a stop", () => {
      const disposable = testApp.seed((db) => {
        return db.insert(app.stops, {
          bandId,
          venueId: venueLondonId,
          date: new Date("2026-12-31T23:00:00Z"),
          status: "tentative",
          publicDescription: "New Year's Eve show \u200A\u2014\u200A maybe",
        });
      });

      testApp.expectAllowed(() => {
        const db = testApp.as(alice);
        db.delete(app.stops, disposable.id);
      });
    });

    it("public visitor cannot insert a stop", () => {
      testApp.expectDenied(() => {
        const db = testApp.as(publicVisitor);
        db.insert(app.stops, {
          bandId,
          venueId: venueNYId,
          date: new Date("2026-10-01T20:00:00Z"),
          status: "confirmed",
          publicDescription: "Unauthorised stop creation attempt",
        });
      });
    });

    it("public visitor cannot update a stop", () => {
      testApp.expectDenied(() => {
        const db = testApp.as(publicVisitor);
        db.update(app.stops, confirmedStopId, {
          publicDescription: "Hacked description",
        });
      });
    });

    it("public visitor cannot delete a stop", () => {
      testApp.expectDenied(() => {
        const db = testApp.as(publicVisitor);
        db.delete(app.stops, confirmedStopId);
      });
    });
  });

  // ── Venue mutation policies ─────────────────────────────────────────

  describe("venue mutations", () => {
    it("band member can insert a venue", () => {
      testApp.expectAllowed(() => {
        const db = testApp.as(alice);
        db.insert(app.venues, {
          name: "Paradiso",
          city: "Amsterdam",
          country: "NL",
          lat: 52.3622,
          lng: 4.8838,
          capacity: 1500,
        });
      });
    });

    it("public visitor cannot insert a venue", () => {
      testApp.expectDenied(() => {
        const db = testApp.as(publicVisitor);
        db.insert(app.venues, {
          name: "Fake Venue",
          city: "Nowhere",
          country: "XX",
          lat: 0,
          lng: 0,
        });
      });
    });
  });
});
