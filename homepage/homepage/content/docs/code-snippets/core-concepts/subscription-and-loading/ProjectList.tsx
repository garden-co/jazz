import { useAccount } from "jazz-tools/react";
import { MyAppAccount } from "./schema";

function ProjectList() {
  const me = useAccount(MyAppAccount, {
    resolve: { profile: true },
  });

  if (!me.$isLoaded) {
    return "Loading...";
  }

  return (
    <div>
      <h1>{me.profile.name}'s projects</h1>
    </div>
  );
}
