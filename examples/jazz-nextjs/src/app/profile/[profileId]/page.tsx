import { jazzSSR } from "@/jazzSSR";
import { Profile } from "jazz-tools";

export default async function ServerSidePage(props: {
  params: Promise<{ profileId: string }>;
}) {
  const { profileId } = await props.params;
  const profile = await Profile.load(profileId, {
    loadAs: jazzSSR,
  });

  return (
    <div className="flex flex-col items-center justify-center h-screen gap-4">
      <h1 className="text-2xl font-bold">SSR rendering example with Jazz</h1>
      <div className="text-sm text-gray-500 w-1/2 text-center">
        This is a server component!
      </div>
      <label>
        <div className="text-sm">
          Your profile name "{profile.$isLoaded ? profile.name : ""}"
        </div>
      </label>
    </div>
  );
}
