import { table, col } from "jazz-ts";

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
});
