import { useAccount, useCoState } from "jazz-react";
import { CoFeed, Group, ID, co } from "jazz-tools";
import { useEffect, useState } from "react";

export class Counter extends CoFeed.Of(co.json<{ value: number }>()) {}

function getIdParam() {
  const url = new URL(window.location.href);
  return (url.searchParams.get("id") as ID<Counter>) ?? undefined;
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

    const id = Counter.create([{ value: 0 }], group).id;

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
    counter.push({ value: ++value });
  }
}
