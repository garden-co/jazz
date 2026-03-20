import { migrate, col } from "jazz-tools";

migrate("participants", {
  jamId: col.rename("jam"),
  userId: col.rename("user_id"),
});

migrate("beats", {
  instrumentId: col.rename("jam"),
  jamId: col.rename("instrument"),
});
