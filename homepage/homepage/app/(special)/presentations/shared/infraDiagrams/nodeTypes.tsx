import clsx from "clsx";
import { ArcherElement } from "react-archer";

export function BrowserNode({
  id,
  upstreamId,
  children,
  className,
  name,
  withSSR,
  withRPC,
  withIndexClient
}: {
  id: string;
  upstreamId?: string;
  children?: React.ReactNode;
  className?: string;
  name?: React.ReactNode;
  withSSR?: string;
  withRPC?: string;
  withIndexClient?: boolean;
}) {
  return (
    <BrowserContainer className={className} name={name}>
      <ArcherElement
        id={id}
        relations={
          upstreamId
            ? [
                {
                  targetId: upstreamId,
                  targetAnchor: "bottom",
                  sourceAnchor: "top",
                },
                ...(withSSR ? [
                  {
                    targetId: withSSR,
                    targetAnchor: "bottom" as const,
                    sourceAnchor: "top" as const,
                    label: <div className="whitespace-nowrap text-blue-500 pl-30 pb-20 text-xl">SSR + Data Hydration</div>,
                    style: {
                      strokeColor: "#5870F1"
                    }
                  },
                ] : []),
                ...(withRPC ? [
                  {
                    targetId: withRPC,
                    targetAnchor: "bottom" as const,
                    sourceAnchor: "top" as const,
                    label: <div className="whitespace-nowrap text-orange-500 pl-10 pb-20 text-xl">RPC + Sync</div>,
                    style: {
                      strokeColor: "oklch(70.5% 0.213 47.604)"
                    }
                  },
                ] : []),
              ]
            : []
        }
      >
        {children ? (
          <div>{children}</div>
        ) : (
          <div>
          <div className="relative m-5 rounded-md border border-white p-2 text-center">
            local state
          </div>
          {withIndexClient && <div className="text-center text-purple-500">MAINTAIN INDEX HERE</div>}
          </div>
        )}

      </ArcherElement>
    </BrowserContainer>
  );
}

export function PhoneNode({
  id,
  upstreamId,
  children,
  className,
  name,

}: {
  id: string;
  upstreamId?: string;
  children?: React.ReactNode;
  className?: string;
  name?: React.ReactNode;

}) {
  return (
    <PhoneContainer
      className={className}
      name={name}
    >

        <ArcherElement
          id={id}
          relations={
            upstreamId
              ? [
                  {
                    targetId: upstreamId,
                    targetAnchor: "bottom",
                    sourceAnchor: "top",
                  },

                ]
              : []
          }
        >
          {children ? (
            <div>{children}</div>
          ) : (
            <div className="relative m-2 rounded-md border border-white p-2 text-center">
              local state
            </div>
          )}
        </ArcherElement>
    </PhoneContainer>
  );
}

export function EdgeServerNode({
  id,
  upstreamId,
  className,
  children,
  name,
  storageName
}: {
  id: string;
  upstreamId?: string;
  className?: string;
  children?: React.ReactNode;
  name?: React.ReactNode;
  storageName?: React.ReactNode;
}) {
  return (
    <div className={clsx("h-20 w-64 rounded border relative", className)}>
      <div className="absolute -top-8 text-xl text-stone-700">
          {name || "Edge Server"}
        </div>
      <ArcherElement
        id={id}
        relations={
          upstreamId
            ? [
                {
                  targetId: upstreamId,
                  targetAnchor: "bottom",
                  sourceAnchor: "top",
                },
              ]
            : []
        }
      >
        {children ? (
          <div>{children}</div>
        ) : (
          <div className="relative rounded-md border border-white p-2 text-center m-5">
            {storageName || "SSD cache"}
          </div>
        )}
      </ArcherElement>
    </div>
  );
}

export function LambdaNode({
  id,
  upstreamId,
  className,
  children,
  name,
  storageName,
  isIndexWorker
}: {
  id: string;
  upstreamId?: string;
  className?: string;
  children?: React.ReactNode;
  name?: React.ReactNode;
  storageName?: React.ReactNode;
  isIndexWorker?: boolean;
}) {
  return (
    <div className={clsx("h-20 rounded border relative", {"w-40": !isIndexWorker}, className)}>
      <div className="absolute -top-8 text-xl text-stone-700">
          {name || "Lambda/Worker"}
        </div>
      <ArcherElement
        id={id}
        relations={
          upstreamId
            ? [
                {
                  targetId: upstreamId,
                  targetAnchor: "right",
                  sourceAnchor: "left",
                },
              ]
            : []
        }
      >
        {children ? (
          <div>{children}</div>
        ) : (
          <div className="flex">
          <div className="relative rounded-md border border-white p-2 text-center m-5">
            {storageName || "mem cache"}
          </div>
          {isIndexWorker && <div className="text-center whitespace-nowrap p-5 flex items-center text-purple-500">MAINTAIN INDEX HERE</div>}
          </div>
        )}
      </ArcherElement>
    </div>
  );
}

export function CoreIngressNode({
  id,
  shardIds,
  storageName
}: {
  id: string;
  shardIds: string[];
  storageName?: React.ReactNode;
}) {
  return (
    <div className="h-20 w-64 rounded border p-5 relative">
      <div className="absolute -top-8 text-xl text-stone-700">
          {"Core Server"}
        </div>
      <ArcherElement
        id={id}
        relations={shardIds.map((shardId) => ({
          targetId: shardId,
          targetAnchor: "bottom",
          sourceAnchor: "top",
        }))}
      >
        <div className="relative rounded-md border border-white p-2 text-center">
          {storageName || "SSD cache"}
        </div>
      </ArcherElement>
    </div>
  );
}

export function CoreShardNode({ id }: { id: string }) {
  return (
    <div className="h-40 w-24 rounded border p-5 relative">
       <div className="absolute -top-8 text-xl text-stone-700">
          {"Shard"}
        </div>
      <ArcherElement id={id}>
        <div className="relative rounded-md border border-white p-2 text-center">
          HDD
        </div>
      </ArcherElement>
    </div>
  );
}

export function BrowserContainer({
  children,
  name,
  className,
}: {
  children: React.ReactNode;
  name: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={clsx("relative h-40 w-64 rounded border-2", className)}>
      <div className="relative h-8 border-b py-1.5">
        <div className="absolute bottom-2.5 left-3 top-2.5 aspect-square rounded-full bg-stone-900"></div>
        <div className="absolute bottom-2.5 left-7 top-2.5 aspect-square rounded-full bg-stone-900"></div>
        <div className="absolute bottom-2.5 left-11 top-2.5 aspect-square rounded-full bg-stone-900"></div>
        <div className="mx-auto h-full w-1/2 rounded bg-stone-950"></div>
      </div>
      <div className="absolute -top-8 text-xl text-stone-700">
        {name || "Browser Client"}
      </div>
      {children}
    </div>
  );
}

export function PhoneContainer({
  children,
  name,
  className,
}: {
  children: React.ReactNode;
  name: React.ReactNode;
  className?: string;
}) {
  return (
    <div
      className={clsx(
        "relative flex h-40 w-24 items-stretch rounded-2xl border-2",
        className,
      )}
    >
      <div className="absolute -right-1 top-6 h-6 w-1 bg-stone-900"></div>
      <div className="m-1 rounded-[12px] border">
        <div className="absolute -top-8 text-xl text-stone-700">
          {name || "App"}
        </div>
        {children}
      </div>
    </div>
  );
}
