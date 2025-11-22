import {
  CoreIngressNode,
  CoreShardNode,
  EdgeServerNode,
  LambdaNode,
} from "./nodeTypes";

export function EdgeServerWithClients({
  regionId,
  children,
  upstreamId,
  edgeClassName,
  edgeChildren,
  edgeStorageName,
  edgeName,
  withLambda,
  withIndexWorker,
  lambdaClassName
}: {
  regionId: string;
  children: React.ReactNode;
  upstreamId?: string;
  edgeClassName?: string;
  edgeChildren?: React.ReactNode;
  edgeStorageName?: React.ReactNode;
  edgeName?: React.ReactNode;
  withLambda?: boolean;
  withIndexWorker?: boolean;
  lambdaClassName?: string;
}) {
  return (
    <div className="flex h-full w-full flex-col gap-20">
      <div className="flex flex-row gap-5">
        <EdgeServerNode
          id={`edgeServer-${regionId}`}
          upstreamId={upstreamId}
          className={edgeClassName}
          storageName={edgeStorageName}
          name={edgeName}
        >
          {edgeChildren}
        </EdgeServerNode>
        {(withLambda || withIndexWorker) && (
          <LambdaNode
            id={`lambda-${regionId}`}
            upstreamId={`edgeServer-${regionId}`}
            isIndexWorker={withIndexWorker}
            className={lambdaClassName}
          >
          </LambdaNode>
        )}
      </div>
      <div className="flex flex-row gap-5">{children}</div>
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
    <div className="flex h-full w-full flex-col gap-10">
      <div className="flex flex-row gap-5">
        {Array.from({ length: nShards }, (_, i) => (
          <CoreShardNode key={i} id={`core-shard-${i}`} />
        ))}
      </div>
      <div className="flex flex-row gap-5">
        {Array.from(
          { length: Array.isArray(children) ? children.length : 1 },
          (_, i) => (
            <CoreIngressNode
              key={i}
              id={`core-ingress-${i}`}
              shardIds={Array.from(
                { length: nShards },
                (_, i) => `core-shard-${i}`,
              )}
            />
          ),
        )}
      </div>
      <div className="mt-40 flex flex-row gap-[30%]">{children}</div>
    </div>
  );
}
