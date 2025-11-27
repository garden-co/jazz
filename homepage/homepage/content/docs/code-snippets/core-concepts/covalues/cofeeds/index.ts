// [!code hide:5]
import { co, Group, z, type SessionID } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
const colleagueAccount = await createJazzTestAccount();
const accountId = colleagueAccount.$jazz.id;
const sessionId = "co_z" as SessionID;

// #region Basic
// Define a schema for feed items
const Activity = co.map({
  timestamp: z.date(),
  action: z.literal(["watering", "planting", "harvesting", "maintenance"]),
  notes: z.optional(z.string()),
});
export type Activity = co.loaded<typeof Activity>;

// Define a feed of garden activities
const ActivityFeed = co.feed(Activity);

// Create a feed instance
const activityFeed = ActivityFeed.create([]);
// #endregion

// #region WithOwner
const teamGroup = Group.create();
teamGroup.addMember(colleagueAccount, "writer");
const teamFeed = ActivityFeed.create([], { owner: teamGroup });
// #endregion

// #region SessionFeed
// Get the feed for a specific session
// @ts-expect-error multiple declarations
const sessionFeed = activityFeed.perSession[sessionId];

// Latest entry from a session
if (sessionFeed?.value.$isLoaded) {
  console.log(sessionFeed.value.action); // "watering"
}
// #endregion

// #region InCurrentSession
// Get the feed for the current session
const currentSessionFeed = activityFeed.inCurrentSession;

// Latest entry from the current session
if (currentSessionFeed?.value.$isLoaded) {
  console.log(currentSessionFeed.value.action); // "harvesting"
}
// #endregion

// #region AccountFeed
// Get the feed for a specific session
// @ts-expect-error multiple declarations
const accountFeed = activityFeed.perAccount[accountId];

// Latest entry from an account
if (accountFeed?.value.$isLoaded) {
  console.log(accountFeed.value.action); // "watering"
}
// #endregion

// #region ByMe
// Get the feed for the current account
const myLatestEntry = activityFeed.byMe;

// Latest entry from the current account
if (myLatestEntry?.value.$isLoaded) {
  console.log(myLatestEntry.value.action); // "harvesting"
}
// #endregion

// #region AllEntries
// Get the feeds for a specific account and session
// @ts-expect-error multiple declarations
const accountFeed = activityFeed.perAccount[accountId];
// @ts-expect-error multiple declarations
const sessionFeed = activityFeed.perSession[sessionId];

// Iterate over all entries from the account
for (const entry of accountFeed.all) {
  if (entry.value.$isLoaded) {
    console.log(entry.value);
  }
}

// Iterate over all entries from the session
for (const entry of sessionFeed.all) {
  if (entry.value.$isLoaded) {
    console.log(entry.value);
  }
}
// #endregion

// #region GetLatest
// Get the latest entry from the current account
const latestEntry = activityFeed.byMe;

if (latestEntry?.value.$isLoaded) {
  console.log(`My last action was ${latestEntry?.value?.action}`);
  // "My last action was harvesting"
}

// Get the latest entry from each account
const latestEntriesByAccount = Object.values(activityFeed.perAccount).map(
  (entry) => ({
    accountName: entry.by?.profile.$isLoaded ? entry.by.profile.name : "Unknown",
    value: entry.value,
  }),
);
// #endregion

// #region PushToFeed
// Log a new activity
activityFeed.$jazz.push(
  Activity.create({
    timestamp: new Date(),
    action: "watering",
    notes: "Extra water for new seedlings",
  }),
);
// #endregion

const fromMobileFeed = ActivityFeed.create([]);
const fromBrowserFeed = ActivityFeed.create([]);

// #region MultiFeed
// On mobile device:
fromMobileFeed.$jazz.push(
  Activity.create({
    timestamp: new Date(),
    action: "harvesting",
    notes: "Vegetable patch",
  }),
);

// On web browser (same user):
fromBrowserFeed.$jazz.push(
  Activity.create({
    timestamp: new Date(),
    action: "planting",
    notes: "Flower bed",
  }),
);

// These are separate entries in the same feed, from the same account
// #endregion

// #region By
// @ts-expect-error multiple declarations
const accountFeed = activityFeed.perAccount[accountId];

// Get the account that made the last entry
console.log(accountFeed?.by);
// #endregion

// #region MadeAt
// @ts-expect-error multiple declarations
const accountFeed = activityFeed.perAccount[accountId];

// Get the timestamp of the last update
console.log(accountFeed?.madeAt);

// Get the timestamp of each entry
for (const entry of accountFeed.all) {
  console.log(entry.madeAt);
}
// #endregion
