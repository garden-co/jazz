import type { PageServerLoad } from "./$types.js";

import { jazzSSR } from "$lib/jazzSSR.js";
import { co } from "jazz-tools";

export const load: PageServerLoad = async ({ params }) => {
  const { profileId } = params;
  const profile = await co.profile().load(profileId, {
    loadAs: jazzSSR,
  });

  return {
    profile: {
      name: profile.$isLoaded ? profile.name : "No name",
    },
  };
};
