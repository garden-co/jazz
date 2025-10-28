import { Group, co, z } from "jazz-tools";
import { useAccount, useCoState } from "jazz-tools/react";
import { useEffect, useState } from "react";

export const Counter = co.feed(z.object({ value: z.number() }));
export type Counter = co.loaded<typeof Counter>;

function getIdParam() {
  const url = new URL(window.location.href);
  return url.searchParams.get("id") ?? undefined;
}

export function ConcurrentChanges() {
  const [id, setId] = useState(getIdParam);
  const counter = useCoState(Counter, id);
  const { me } = useAccount();

  useEffect(() => {
    if (id) {
      const url = new URL(window.location.href);
      url.searchParams.set("id", id);
      history.pushState({}, "", url.toString());
    }
  }, [id]);

  useEffect(() => {
    if (counter?.byMe) {
      count(counter);
    }
  }, [counter?.byMe?.value !== undefined]);

  const createCounter = () => {
    if (!me) return;

    const group = Group.create();

    group.addMember("everyone", "writer");

    const id = Counter.create([{ value: 0 }], group).$jazz.id;

    setId(id);

    window.open(`?id=${id}`, "_blank");
  };

  const done = Object.entries(counter?.perSession ?? {}).every(
    ([_, entry]) => entry.value.value === 300,
  );

  return (
    <div>
      <h1>Concurrent Changes</h1>
      <p>
        {Object.entries(counter?.perSession ?? {}).map(([sessionId, entry]) => (
          <div key={sessionId}>
            <p>{sessionId}</p>
            <p data-testid="value">{entry.value.value}</p>
          </div>
        ))}
      </p>
      <button onClick={createCounter}>Create a new value!</button>
      {done && <p data-testid="done">Done!</p>}
    </div>
  );
}

async function count(counter: Counter) {
  if (!counter.byMe) return;

  let value = counter.byMe.value?.value ?? 0;

  while (value < 300) {
    await new Promise((resolve) => setTimeout(resolve, 10));
    counter.$jazz.push({ value: ++value });
  }
}
