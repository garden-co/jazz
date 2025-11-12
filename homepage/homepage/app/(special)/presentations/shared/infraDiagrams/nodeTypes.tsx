import clsx from "clsx";
import { ArcherElement } from "react-archer";

export function BrowserNode({
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
              ]
            : []
        }
      >
        {children ? (
          <div>{children}</div>
        ) : (
          <div className="relative m-5 rounded-md border border-white p-2 text-center">
            local state
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
}: {
  id: string;
  upstreamId?: string;
  className?: string;
  children?: React.ReactNode;
  name?: React.ReactNode;
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
            SSD cache
          </div>
        )}
      </ArcherElement>
    </div>
  );
}

export function CoreIngressNode({
  id,
  shardIds,
}: {
  id: string;
  shardIds: string[];
}) {
  return (
    <div className="h-20 w-64 rounded border p-5">
      <ArcherElement
        id={id}
        relations={shardIds.map((shardId) => ({
          targetId: shardId,
          targetAnchor: "bottom",
          sourceAnchor: "top",
        }))}
      >
        <div className="relative rounded-md border border-white p-2 text-center">
          SSD cache
        </div>
      </ArcherElement>
    </div>
  );
}

export function CoreShardNode({ id }: { id: string }) {
  return (
    <div className="h-40 w-24 rounded border p-5">
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
