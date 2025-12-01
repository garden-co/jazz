import { jazzSSR } from "@/app/jazzSSR";
import { Festival } from "@/app/schema";

export default async function ServerSidePage(props: {
  params: { festivalId: string };
}) {
  const { festivalId } = await props.params;
  const festival = await Festival.load(festivalId, {
    loadAs: jazzSSR,
    resolve: {
      $each: {
        $onError: "catch",
      },
    },
  });

  return (
    <main>
      <h1>🎪 Server-rendered Festival {festivalId}</h1>

      <ul>
        {festival.$isLoaded &&
          festival.map((band) => {
            if (!band.$isLoaded) return null;
            return <li key={band.$jazz.id}>🎶 {band.name}</li>;
          })}
      </ul>
    </main>
  );
}
