import { schema as s } from "jazz-tools";

const schema = {
  players: s.table({
    playerId: s.string(),
    name: s.string(),
    color: s.string(),
    mode: s.string(),
    online: s.boolean(),
    lastSeen: s.int(),
    positionX: s.int(),
    positionY: s.int(),
    velocityX: s.int(),
    velocityY: s.int(),
    requiredFuelType: s.string(),
    landerFuelLevel: s.int(),
    landerSpawnX: s.int(),
    thrusting: s.boolean(),
  }),
  fuel_deposits: s.table({
    fuelType: s.string(),
    positionX: s.int(),
    // Simulation event time, in seconds; unlike Jazz's row-creation `$createdAt`,
    // this survives the delete/reinsert flow used to release a deposit.
    spawnedAtSeconds: s.int(),
    collected: s.boolean(),
    collectedBy: s.string(),
  }),
  chat_messages: s.table({
    playerId: s.string(),
    message: s.string(),
    // Sender device event time drives bubble expiry, independently of sync arrival.
    sentAtSeconds: s.int(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Player = s.RowOf<typeof app.players>;
export type PlayerInit = s.InsertOf<typeof app.players>;
export type FuelDeposit = s.RowOf<typeof app.fuel_deposits>;
export type FuelDepositInit = s.InsertOf<typeof app.fuel_deposits>;
export type ChatMessage = s.RowOf<typeof app.chat_messages>;
export type ChatMessageInit = s.InsertOf<typeof app.chat_messages>;
