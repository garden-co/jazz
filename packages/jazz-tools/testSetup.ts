import { cojsonInternals } from "cojson";

// Use a very high budget to avoid that slow tests fail due to the budget being exceeded.
cojsonInternals.setIncomingMessagesTimeBudget(10000); // 10 seconds
