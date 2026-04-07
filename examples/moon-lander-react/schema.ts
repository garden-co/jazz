import { table, col } from "jazz-tools";

table("players", {
  playerId: col.string(),
  name: col.string(),
  color: col.string(),
  mode: col.string(),
  online: col.boolean(),
  lastSeen: col.int(),
  positionX: col.int(),
  positionY: col.int(),
  velocityX: col.int(),
  velocityY: col.int(),
  requiredFuelType: col.string(),
  landerFuelLevel: col.int(),
  landerSpawnX: col.int(),
  thrusting: col.boolean(),
});

table("fuel_deposits", {
  fuelType: col.string(),
  positionX: col.int(),
  createdAt: col.int(),
  collected: col.boolean(),
  collectedBy: col.string(),
});

table("chat_messages", {
  playerId: col.string(),
  message: col.string(),
  createdAt: col.int(),
});
