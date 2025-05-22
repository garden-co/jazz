"use client";

import { useAccount } from "jazz-react";
import { Account } from "jazz-tools";

export default function Home() {
  const { me } = useAccount(Account, {
    resolve: {
      profile: true,
    },
  });

  return (
    <div className="flex flex-col items-center justify-center h-screen gap-4">
      <h1 className="text-2xl font-bold">SSR rendering example with Jazz</h1>
      <div className="text-sm text-gray-500 w-1/2 text-center">
        Data is still loaded only on the client, the components are rendered on
        the server with all the CoValues as null
      </div>
      <label>
        <div className="text-sm">
          Your profile name{" "}
          <span className="text-xs">(only loaded on the client)</span>
        </div>
        <input
          className="border-2 border-gray-300 rounded-md p-2 w-full"
          value={me?.profile.name ?? ""}
          onChange={(e) => {
            if (!me) {
              return;
            }

            me.profile.name = e.target.value;
          }}
        />
      </label>
    </div>
  );
}
