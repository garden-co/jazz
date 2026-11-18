import { useEffect, useState } from "react";
import { Text, View } from "react-native";
import { createJazzClient } from "jazz-tools";
import { app } from "./jazz/jazz.generated";
export default function App() {
  const [st, setSt] = useState("starting");
  useEffect(() => { (async () => {
    try {
      const session: any = await createJazzClient({ serverUrl: "/jazz", initial: "device", localFirst: "now", client: () => import("jazz-tools/local-first") } as any);
      const me = session.getSnapshot().user?.accountId;
      await session.db.insert(app.notes, { text: "web", owner_id: me }).wait({ tier: "local" });
      const rows = await session.db.all(app.notes);
      setSt("LF=" + session.db.isLocalFirst() + " rows=" + rows.length);
    } catch (e: any) { setSt("ERR " + e.message); }
  })(); }, []);
  return <View><Text testID="st">{st}</Text></View>;
}
