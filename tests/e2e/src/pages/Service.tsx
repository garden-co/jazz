import {
  useAccount,
  experimental_useServiceSender as useServiceSender,
} from "jazz-react";
import { Account, CoMap, Group, ID, Service, coField } from "jazz-tools";
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

export function ServicePage() {
  const [id] = useState(getIdParam);
  const { me } = useAccount();
  const [pingPong, setPingPong] = useState<PingPong | null>(null);
  const iframeRef = useRef<HTMLIFrameElement>(null);

  useEffect(() => {
    let unsubscribe = () => {};
    let unmounted = false;

    async function load() {
      const service = await Service.load(me);

      if (unmounted) return;

      unsubscribe = service.subscribe(PingPong, async (message) => {
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

  const sendPingPong = useServiceSender(id);

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
      <h1>Service test</h1>
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
