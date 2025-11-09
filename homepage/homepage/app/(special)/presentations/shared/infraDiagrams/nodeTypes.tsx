import clsx from "clsx";
import { ArcherElement } from "react-archer";

export function BrowserNode({
  id,
  upstreamId,
  children,
  className,
}: {
  id: string;
  upstreamId?: string;
  children?: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={clsx("h-40 w-64 rounded bg-blue-500 p-5", className)}>
      <ArcherElement
        id={id}
        relations={upstreamId ? [
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
          <div className="relative rounded-md border border-white p-2 text-center">
            local state
          </div>
        )}
      </ArcherElement>
    </div>
  );
}

export function PhoneNode({
  id,
  upstreamId,
  children,
  className,
}: {
  id: string;
  upstreamId: string;
  children?: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={clsx("h-40 w-24 rounded bg-red-500 p-5", className)}>
      <ArcherElement
        id={id}
        relations={[
          {
            targetId: upstreamId,
            targetAnchor: "bottom",
            sourceAnchor: "top",
          },
        ]}
      >
        {children ? (
          <div>{children}</div>
        ) : (
          <div className="relative rounded-md border border-white p-2 text-center">
            local state
          </div>
        )}
      </ArcherElement>
    </div>
  );
}

export function EdgeServerNode({
  id,
  upstreamId,
  className,
  children,
}: {
  id: string;
  upstreamId?: string;
  className?: string;
  children?: React.ReactNode;
}) {
  return (
    <div className={clsx("h-20 w-64 rounded bg-green-500 p-5", className)}>
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
          <div className="relative rounded-md border border-white p-2 text-center">
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
    <div className="h-20 w-64 rounded bg-green-500 p-5">
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
    <div className="h-40 w-24 rounded bg-green-500 p-5">
      <ArcherElement id={id}>
        <div className="relative rounded-md border border-white p-2 text-center">
          HDD
        </div>
      </ArcherElement>
    </div>
  );
}
