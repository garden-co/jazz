import { ref } from "vue";

/**
 * Hash routes, so invite codes never reach server or CDN logs:
 *   #/bands/<bandId>                 a band's tour
 *   #/bands/<bandId>/join/<code>     an invite to join it
 */
export type Route = { bandId: string | null; inviteCode: string | null };

function parse(hash: string): Route {
  const [, bandId = null, , inviteCode = null] =
    hash.match(/^#\/bands\/([^/]+)(\/join\/([^/]+))?$/) ?? [];
  return { bandId, inviteCode };
}

const base = () => `${window.location.origin}${window.location.pathname}`;
export const bandLink = (bandId: string) => `${base()}#/bands/${bandId}`;
export const inviteLink = (bandId: string, code: string) => `${bandLink(bandId)}/join/${code}`;

export function useRoute() {
  const route = ref(parse(window.location.hash));
  window.addEventListener("hashchange", () => (route.value = parse(window.location.hash)));
  const goToBand = (bandId: string) => window.location.replace(`#/bands/${bandId}`);
  return { route, goToBand };
}
