import { useEffect, useRef, useState } from "react";
import { useMutation } from "convex/react";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { HStack } from "@astryxdesign/core/Stack";
import { TextInput } from "@astryxdesign/core/TextInput";
import { api } from "../../convex/_generated/api.js";
import { href, navigate, parseRoute } from "../router.js";
import { Loading } from "./Loading.js";
import { Page } from "./Page.js";

/** Opened from an invite link: adds you to the show's crew, then opens its board. */
export function JoinShow({ showId, code }: { showId: string; code: string }) {
  const joinShow = useMutation(api.crew.join);
  const [failed, setFailed] = useState(false);
  const started = useRef(false);

  useEffect(() => {
    if (started.current) return;
    started.current = true;
    joinShow({ showId, code }).then(
      () => navigate(href.show(showId)),
      () => setFailed(true),
    );
  }, [joinShow, showId, code]);

  return (
    <Page title="Joining the crew">
      {failed ? (
        <Banner
          status="error"
          title="This invite link doesn't work any more"
          description="The crew chief may have made a new link. Ask them to send it again."
          endContent={<Button label="Back to shows" href={href.shows()} />}
        />
      ) : (
        <Loading label="Checking your invite" />
      )}
    </Page>
  );
}

/** Paste an invite link to join a show. */
export function JoinByLink() {
  const [link, setLink] = useState("");
  const route = parseRoute(link.slice(link.indexOf("#")));
  const canJoin = link.includes("#") && route.page === "join";

  return (
    <form
      onSubmit={(event) => {
        event.preventDefault();
        if (route.page === "join") navigate(href.join(route.showId, route.code));
      }}
    >
      <HStack gap={2} vAlign="end" wrap="wrap">
        <TextInput
          label="Join a show with an invite link"
          placeholder="Paste the link a crew chief sent you"
          value={link}
          onChange={setLink}
          width="min(100%, 480px)"
        />
        <Button label="Join" type="submit" isDisabled={!canJoin} />
      </HStack>
    </form>
  );
}
