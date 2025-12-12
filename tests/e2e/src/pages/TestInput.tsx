import { co, z } from "jazz-tools";
import { useCoState, usePassphraseAuth } from "jazz-tools/react";
import { useEffect, useState } from "react";
import { wordlist } from "../wordlist";

export const InputTestCoMap = co.map({
  title: z.string(),
});

function getIdParam() {
  const url = new URL(window.location.href);
  return url.searchParams.get("id") ?? undefined;
}

export function TestInput() {
  const [id, setId] = useState(getIdParam);
  const coMap = useCoState(InputTestCoMap, id);
  const auth = usePassphraseAuth({ wordlist });

  useEffect(() => {
    if (id) return;

    const group = co.group().create();

    group.addMember("everyone", "writer");
    const map = InputTestCoMap.create({ title: "" }, { owner: group });

    setId(map.$jazz.id);

    const url = new URL(window.location.href);
    url.searchParams.set("id", map.$jazz.id);
    history.pushState({}, "", url.toString());
  }, [id]);

  if (!coMap.$isLoaded) return null;

  return (
    <>
      <button onClick={() => auth.signUp()}>Sign Up</button>
      <input
        value={coMap.title}
        onChange={(e) => {
          coMap.$jazz.set("title", e.target.value);
        }}
      />
    </>
  );
}
