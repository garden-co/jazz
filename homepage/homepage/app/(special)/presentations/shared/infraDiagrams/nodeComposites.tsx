import { CoreIngressNode, CoreShardNode, EdgeServerNode } from "./nodeTypes";

export function EdgeServerWithClients({
  regionId,
  children,
  upstreamId,
  edgeClassName,
  edgeChildren
}: {
  regionId: string;
  children: React.ReactNode;
  upstreamId?: string;
  edgeClassName?: string;
  edgeChildren?: React.ReactNode;
}) {
  return (
    <div className="flex h-full w-full flex-col gap-20">
      <div className="flex flex-row gap-5 justify-around">
        <EdgeServerNode id={`edgeServer-${regionId}`} upstreamId={upstreamId} className={edgeClassName}>
          {edgeChildren}
        </EdgeServerNode>
      </div>
      <div className="flex flex-row gap-5 justify-around">{children}</div>
    </div>
  );
}

export function CoreWithRegions({
  nShards,
  children,
}: {
  nShards: number;
  children: React.ReactNode;
}) {
  return (
    <div className="flex h-full w-full flex-col gap-20">
      <div className="flex flex-row gap-5">
        {Array.from({ length: nShards }, (_, i) => (
          <CoreShardNode key={i} id={`core-shard-${i}`} />
        ))}
      </div>
      <div className="flex flex-row gap-5">
        {
          Array.from({ length: Array.isArray(children) ? children.length : 1 }, (_, i) => (
            <CoreIngressNode
              key={i}
              id={`core-ingress-${i}`}
              shardIds={Array.from(
                { length: nShards },
                (_, i) => `core-shard-${i}`,
              )}
            />
          ))}
      </div>
      <div className="flex flex-row gap-5 mt-40">{children}</div>
    </div>
  );
}