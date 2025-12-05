import { jazzSSR } from "@/app/jazzSSR";
import { Festival } from "@/app/schema";
import { FestivalComponent } from "./FestivalComponent";

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

  if (!festival.$isLoaded) return <div>Festival not found</div>;

  return (
    // [!code ++:1]
    <FestivalComponent preloaded={festival.$jazz.export()} festivalId={festivalId} />
  );
}
