import type { PageServerLoad } from './$types';

import { jazzSSR } from "$lib/jazzSSR";
import { co } from "jazz-tools";

export const load: PageServerLoad = async ({ params }) => {
  const { profileId } = params;
  const profile = await co.profile().load(profileId, {
    loadAs: jazzSSR,
  });

  return {
    profile: {
      name: profile?.name || 'No name'
    }
  }
}