import { Account, Group, Inbox, co, z } from "jazz-tools";
import {
  useAccount,
  experimental_useInboxSender as useInboxSender,
} from "jazz-tools/react";
import { useEffect, useRef, useState } from "react";
import { createCredentiallessIframe } from "../lib/createCredentiallessIframe";

export const PingPong = co.map({
  ping: z.number(),
  pong: z.number().optional(),
});
export type PingPong = co.loaded<typeof PingPong>;

function getIdParam() {
  const url = new URL(window.location.href);
  return url.searchParams.get("id") ?? undefined;
}

export function InboxPage() {
  const [id] = useState(getIdParam);
  const { me } = useAccount();
  const [pingPong, setPingPong] = useState<PingPong | null>(null);
  const iframeRef = useRef<HTMLIFrameElement>(null);

  useEffect(() => {
    if (!me) return;

    let unsubscribe = () => {};
    let unmounted = false;
    const account = me;

    async function load() {
      const inbox = await Inbox.load(account);

      if (unmounted) return;

      unsubscribe = inbox.subscribe(PingPong, async (message) => {
        const pingPong = PingPong.create(
          { ping: message.ping, pong: Date.now() },
          { owner: message.$jazz.owner },
        );
        setPingPong(pingPong);
      });
    }

    load();

    return () => {
      unmounted = true;
      unsubscribe();
    };
  }, [me]);

  const sendPingPong = useInboxSender(id);

  useEffect(() => {
    async function load() {
      if (!id) return;
      const account = await Account.load(id);

      if (!account) return;

      const group = Group.create();
      group.addMember(account, "writer");
      const pingPong = PingPong.create({ ping: Date.now() }, { owner: group });

      sendPingPong(pingPong);
    }

    load();
  }, [id]);

  const handlePingPong = () => {
    if (!me || id) return;

    iframeRef.current?.remove();

    const url = new URL(window.location.href);
    url.searchParams.set("id", me.$jazz.id);

    const iframe = createCredentiallessIframe(url.toString());
    document.body.appendChild(iframe);
    iframeRef.current = iframe;
  };

  return (
    <div>
      <h1>Inbox test</h1>
      <button onClick={handlePingPong}>Start a ping-pong</button>
      {pingPong && (
        <div data-testid="ping-pong">
          <p>Ping: {new Date(pingPong.ping).toISOString()}</p>
          <p>Pong: {new Date(pingPong.pong!).toISOString()}</p>
        </div>
      )}
    </div>
  );
}
