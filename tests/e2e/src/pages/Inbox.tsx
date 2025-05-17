import {
  useAccount,
  experimental_useInboxSender as useInboxSender,
} from "jazz-react";
import { Account, CoMap, Group, ID, Inbox, coField } from "jazz-tools";
import { useEffect, useRef, useState } from "react";
import { createCredentiallessIframe } from "../lib/createCredentiallessIframe";

export class PingPong extends CoMap {
  ping = coField.json<number>();
  pong = coField.optional.json<number>();
}

function getIdParam() {
  const url = new URL(window.location.href);
  return (url.searchParams.get("id") as ID<Account> | undefined) ?? undefined;
}

export function InboxPage() {
  const [id] = useState(getIdParam);
  const { me } = useAccount();
  const [pingPong, setPingPong] = useState<PingPong | null>(null);
  const iframeRef = useRef<HTMLIFrameElement>();

  useEffect(() => {
    let unsubscribe = () => {};
    let unmounted = false;

    async function load() {
      const inbox = await Inbox.load(me);

      if (unmounted) return;

      unsubscribe = inbox.subscribe(PingPong, async (message) => {
        const pingPong = PingPong.create(
          { ping: message.ping, pong: Date.now() },
          { owner: message._owner },
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
    url.searchParams.set("id", me.id);

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
