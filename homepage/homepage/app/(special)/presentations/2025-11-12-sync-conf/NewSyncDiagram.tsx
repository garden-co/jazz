"use client";

import { ArcherContainer, ArcherElement } from "react-archer";
import {
  BrowserNode,
  CoreIngressNode,
  PhoneNode,
} from "../shared/infraDiagrams/nodeTypes";
import {
  CoreWithRegions,
  EdgeServerWithClients,
} from "../shared/infraDiagrams/nodeComposites";
import clsx from "clsx";

export function EvenSimplerNewSyncDiagram() {
  return (
    <ArcherContainer
      strokeColor="white"
      startMarker={true}
      endMarker={true}
      lineStyle="curve"
      offset={5}
      svgContainerStyle={{ zIndex: 1 }}
    >
      <div className="ml-[30%] flex">
        <EdgeServerWithClients
          regionId="us-east-1"
          edgeName="Sync & Storage Server"
          edgeStorageName="SSD"
        >
          <BrowserNode
            id="browser-1-us-east-1"
            upstreamId="edgeServer-us-east-1"
          />
          <BrowserNode
            id="phone-1-us-east-1"
            upstreamId="edgeServer-us-east-1"
          />
          <PhoneNode id="phone-2-us-east-1" upstreamId="edgeServer-us-east-1" />
        </EdgeServerWithClients>
      </div>
    </ArcherContainer>
  );
}

export function SimpleNewSyncDiagram({ authority }: { authority?: 1 | 2 }) {
  return (
    <ArcherContainer
      strokeColor="white"
      startMarker={true}
      endMarker={true}
      lineStyle="curve"
      offset={5}
      svgContainerStyle={{ zIndex: 1 }}
    >
      <div className="mb-40 flex">
        <CoreIngressNode id="core" shardIds={[]} storageName="SSD" />
      </div>
      <div className="flex">
        <EdgeServerWithClients
          regionId="us-east-1"
          upstreamId="core"
          edgeClassName={clsx(
            authority === 1 ? "outline-3 outline-teal-500"  : "border-stone-500",
          )}
        >
          <BrowserNode
            id="browser-1-us-east-1"
            upstreamId="edgeServer-us-east-1"
          />
          <PhoneNode id="phone-1-us-east-1" upstreamId="edgeServer-us-east-1" />
          <BrowserNode
            id="phone-2-us-east-1"
            upstreamId="edgeServer-us-east-1"
          />
        </EdgeServerWithClients>
        <EdgeServerWithClients regionId="us-west-1" upstreamId="core"
        edgeClassName={clsx(
           authority === 2 ? "outline-3 outline-teal-500" : "border-stone-500",
        )}
        >
          <BrowserNode
            id="browser-1-us-west-1"
            upstreamId="edgeServer-us-west-1"
          />
          <PhoneNode id="phone-1-us-west-1" upstreamId="edgeServer-us-west-1" />
          <PhoneNode id="phone-2-us-west-1" upstreamId="edgeServer-us-west-1" />
        </EdgeServerWithClients>
      </div>
    </ArcherContainer>
  );
}

export function SimpleNewSyncDiagramWithLambda({ authority }: { authority?: boolean }) {
  return (
    <ArcherContainer
      strokeColor="white"
      startMarker={true}
      endMarker={true}
      lineStyle="curve"
      offset={5}
      svgContainerStyle={{ zIndex: 1 }}
    >
      <div className="mb-40 flex">
        <CoreIngressNode id="core" shardIds={[]} storageName="SSD" />
      </div>
      <div className="flex">
        <EdgeServerWithClients
          regionId="us-east-1"
          upstreamId="core"
          withLambda={true}
          lambdaClassName={clsx(authority ? "outline-3 outline-teal-500" : "border-stone-500")}
        >
          <BrowserNode
            id="browser-1-us-east-1"
            upstreamId="edgeServer-us-east-1"
          />
          <PhoneNode id="phone-1-us-east-1" upstreamId="edgeServer-us-east-1" />
          <BrowserNode
            id="phone-2-us-east-1"
            upstreamId="edgeServer-us-east-1"
          />
        </EdgeServerWithClients>
        <EdgeServerWithClients regionId="us-west-1" upstreamId="core">
          <BrowserNode
            id="browser-1-us-west-1"
            upstreamId="edgeServer-us-west-1"
          />
          <PhoneNode id="phone-1-us-west-1" upstreamId="edgeServer-us-west-1" />
          <PhoneNode id="phone-2-us-west-1" upstreamId="edgeServer-us-west-1" />
        </EdgeServerWithClients>
      </div>
    </ArcherContainer>
  );
}

export function SimpleNewSyncDiagramWithLambdaAndSSR() {
  return (
    <ArcherContainer
      strokeColor="white"
      startMarker={true}
      endMarker={true}
      lineStyle="curve"
      offset={5}
      svgContainerStyle={{ zIndex: 1 }}
    >
      <div className="mb-40 flex">
        <CoreIngressNode id="core" shardIds={[]} storageName="SSD" />
      </div>
      <div className="flex">
        <EdgeServerWithClients
          regionId="us-east-1"
          upstreamId="core"
          withLambda={true}
        >
          <BrowserNode
            id="browser-1-us-east-1"
            upstreamId="edgeServer-us-east-1"
          />
          <PhoneNode id="phone-1-us-east-1" upstreamId="edgeServer-us-east-1" />
          <BrowserNode
            id="phone-2-us-east-1"
            upstreamId="edgeServer-us-east-1"
            withSSR="lambda-us-east-1"
          />
        </EdgeServerWithClients>
        <EdgeServerWithClients regionId="us-west-1" upstreamId="core">
          <BrowserNode
            id="browser-1-us-west-1"
            upstreamId="edgeServer-us-west-1"
          />
          <PhoneNode id="phone-1-us-west-1" upstreamId="edgeServer-us-west-1" />
          <PhoneNode id="phone-2-us-west-1" upstreamId="edgeServer-us-west-1" />
        </EdgeServerWithClients>
      </div>
    </ArcherContainer>
  );
}

export function SimpleNewSyncDiagramWithLambdaAndRPC() {
  return (
    <ArcherContainer
      strokeColor="white"
      startMarker={true}
      endMarker={true}
      lineStyle="curve"
      offset={5}
      svgContainerStyle={{ zIndex: 1 }}
    >
      <div className="mb-40 flex">
        <CoreIngressNode id="core" shardIds={[]} storageName="SSD" />
      </div>
      <div className="flex">
        <EdgeServerWithClients
          regionId="us-east-1"
          upstreamId="core"
          withLambda={true}
        >
          <BrowserNode
            id="browser-1-us-east-1"
            upstreamId="edgeServer-us-east-1"
          />
          <PhoneNode id="phone-1-us-east-1" upstreamId="edgeServer-us-east-1" />
          <BrowserNode
            id="phone-2-us-east-1"
            upstreamId="edgeServer-us-east-1"
            withRPC="lambda-us-east-1"
          />
        </EdgeServerWithClients>
        <EdgeServerWithClients regionId="us-west-1" upstreamId="core">
          <BrowserNode
            id="browser-1-us-west-1"
            upstreamId="edgeServer-us-west-1"
          />
          <PhoneNode id="phone-1-us-west-1" upstreamId="edgeServer-us-west-1" />
          <PhoneNode id="phone-2-us-west-1" upstreamId="edgeServer-us-west-1" />
        </EdgeServerWithClients>
      </div>
    </ArcherContainer>
  );
}

export function SimpleNewSyncDiagramIndexWorker() {
  return (
    <ArcherContainer
      strokeColor="white"
      startMarker={true}
      endMarker={true}
      lineStyle="curve"
      offset={5}
      svgContainerStyle={{ zIndex: 1 }}
    >
      <div className="mb-40 flex">
        <CoreIngressNode id="core" shardIds={[]} storageName="SSD" />
      </div>
      <div className="flex">
        <EdgeServerWithClients
          regionId="us-east-1"
          upstreamId="core"
          withIndexWorker={true}
        >
          <BrowserNode
            id="browser-1-us-east-1"
            upstreamId="edgeServer-us-east-1"
          />
          <PhoneNode id="phone-1-us-east-1" upstreamId="edgeServer-us-east-1" />
          <BrowserNode
            id="phone-2-us-east-1"
            upstreamId="edgeServer-us-east-1"
          />
        </EdgeServerWithClients>
        <EdgeServerWithClients regionId="us-west-1" upstreamId="core">
          <BrowserNode
            id="browser-1-us-west-1"
            upstreamId="edgeServer-us-west-1"
          />
          <PhoneNode id="phone-1-us-west-1" upstreamId="edgeServer-us-west-1" />
          <PhoneNode id="phone-2-us-west-1" upstreamId="edgeServer-us-west-1" />
        </EdgeServerWithClients>
      </div>
    </ArcherContainer>
  );
}

export function SimpleNewSyncDiagramIndexClient() {
  return (
    <ArcherContainer
      strokeColor="white"
      startMarker={true}
      endMarker={true}
      lineStyle="curve"
      offset={5}
      svgContainerStyle={{ zIndex: 1 }}
    >
      <div className="mb-40 flex">
        <CoreIngressNode id="core" shardIds={[]} storageName="SSD" />
      </div>
      <div className="flex">
        <EdgeServerWithClients regionId="us-east-1" upstreamId="core">
          <BrowserNode
            id="browser-1-us-east-1"
            upstreamId="edgeServer-us-east-1"
            withIndexClient={true}
          />
          <PhoneNode id="phone-1-us-east-1" upstreamId="edgeServer-us-east-1" />
          <BrowserNode
            id="phone-2-us-east-1"
            upstreamId="edgeServer-us-east-1"
            withIndexClient={true}
          />
        </EdgeServerWithClients>
        <EdgeServerWithClients regionId="us-west-1" upstreamId="core">
          <BrowserNode
            id="browser-1-us-west-1"
            upstreamId="edgeServer-us-west-1"
            withIndexClient={true}
          />
          <PhoneNode id="phone-1-us-west-1" upstreamId="edgeServer-us-west-1" />
          <PhoneNode id="phone-2-us-west-1" upstreamId="edgeServer-us-west-1" />
        </EdgeServerWithClients>
      </div>
    </ArcherContainer>
  );
}

export function NewSyncDiagram() {
  return (
    <ArcherContainer
      strokeColor="white"
      startMarker={true}
      endMarker={true}
      lineStyle="curve"
      offset={5}
      svgContainerStyle={{ zIndex: 1 }}
    >
      <CoreWithRegions nShards={8}>
        <EdgeServerWithClients
          regionId="us-east-1"
          upstreamId="core-ingress-0"
          edgeClassName="mb-20"
        >
          <BrowserNode
            id="browser-1-us-east-1"
            upstreamId="edgeServer-us-east-1"
          />
          <PhoneNode id="phone-1-us-east-1" upstreamId="edgeServer-us-east-1" />
          <BrowserNode
            id="phone-2-us-east-1"
            upstreamId="edgeServer-us-east-1"
          />
        </EdgeServerWithClients>
        <EdgeServerWithClients
          regionId="us-west-1"
          upstreamId="core-ingress-1"
          edgeClassName="mb-20"
        >
          <BrowserNode
            id="browser-1-us-west-1"
            upstreamId="edgeServer-us-west-1"
          />
          <PhoneNode id="phone-1-us-west-1" upstreamId="edgeServer-us-west-1" />
          <PhoneNode id="phone-2-us-west-1" upstreamId="edgeServer-us-west-1" />
        </EdgeServerWithClients>
      </CoreWithRegions>
      <div className="absolute bottom-60 left-0 right-0 border-b-2 border-t-2 bg-stone-950 py-2 text-center text-2xl text-stone-200">
        IP ANYCAST
      </div>
    </ArcherContainer>
  );
}

export function TradDBDiagram({ authority }: { authority?: boolean }) {
  return (
    <ArcherContainer
      strokeColor="white"
      strokeDasharray="10,20"
      startMarker={true}
      endMarker={true}
      lineStyle="curve"
      offset={5}
      svgContainerStyle={{ zIndex: 1 }}
    >
      <div className="mb-30 flex px-40">
        <ArcherElement id="trad-db">
          <div
            className={clsx(
              "w-[20rem] rounded border p-5",
              authority ? "outline-3 outline-teal-500" : "border-stone-500",
            )}
          >
            Traditional DB
          </div>
        </ArcherElement>
      </div>
      <div className="mb-[30rem] flex px-40">
        <ArcherElement
          id="trad-backend"
          relations={[
            {
              targetId: "trad-db",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
          ]}
        >
          <div className="w-[20rem] rounded border border-stone-500 p-5">
            Traditional Backend
          </div>
        </ArcherElement>
      </div>
      <div className="flex px-40">
        <ArcherElement
          id="trad-client1"
          relations={[
            {
              targetId: "trad-backend",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
          ]}
        >
          <div className="rounded border border-stone-500 p-5">
            Traditional Client 1
          </div>
        </ArcherElement>
        <ArcherElement
          id="trad-client2"
          relations={[
            {
              targetId: "trad-backend",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
          ]}
        >
          <div className="ml-[50rem] rounded border border-stone-500 p-5">
            Traditional Client 2
          </div>
        </ArcherElement>
      </div>
    </ArcherContainer>
  );
}

export function SyncEngineDiagram({ authority }: { authority?: boolean }) {
  return (
    <ArcherContainer
      strokeColor="white"
      strokeDasharray="10,20"
      startMarker={false}
      endMarker={true}
      lineStyle="curve"
      offset={5}
      svgContainerStyle={{ zIndex: 1 }}
    >
      <div className="mb-14 flex px-40">
        <ArcherElement
          id="trad-db"
          relations={[
            {
              targetId: "sync-engine1",
              targetAnchor: "top",
              sourceAnchor: "bottom",
              style: {
                strokeDasharray: "5,0",
              },
            },
            {
              targetId: "sync-engine2",
              targetAnchor: "top",
              sourceAnchor: "bottom",
              style: {
                strokeDasharray: "5,0",
              },
            },
          ]}
        >
          <div
            className={clsx(
              "w-[20rem] rounded border p-5",
              authority ? "outline-3 outline-teal-500" : "border-stone-500",
            )}
          >
            Traditional DB
          </div>
        </ArcherElement>
      </div>
      <div className="mb-[20rem] flex px-40">
        <ArcherElement
          id="trad-backend"
          relations={[
            {
              targetId: "trad-db",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
          ]}
        >
          <div className="w-[20rem] rounded border border-stone-500 p-5">
            Backend
          </div>
        </ArcherElement>
      </div>
      <div className="mb-[10rem] flex px-40">
        <ArcherElement id="sync-engine1" relations={[]}>
          <div className="ml-60 w-[20rem] rounded border border-stone-500 p-5">
            Sync Engine
          </div>
        </ArcherElement>
        <ArcherElement id="sync-engine2" relations={[]}>
          <div className="ml-80 w-[20rem] rounded border border-stone-500 p-5">
            Sync Engine
          </div>
        </ArcherElement>
      </div>
      <div className="flex px-40">
        <ArcherElement
          id="trad-client1"
          relations={[
            {
              targetId: "trad-backend",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
            {
              targetId: "sync-engine1",
              targetAnchor: "bottom",
              sourceAnchor: "top",
              style: {
                strokeDasharray: "5,0",
                startMarker: true,
              },
            },
          ]}
        >
          <div className="rounded border border-stone-500 p-5">Client 1</div>
        </ArcherElement>
        <ArcherElement
          id="trad-client2"
          relations={[
            {
              targetId: "trad-backend",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
            {
              targetId: "sync-engine2",
              targetAnchor: "bottom",
              sourceAnchor: "top",
              style: {
                strokeDasharray: "5,0",
                startMarker: true,
              },
            },
          ]}
        >
          <div className="ml-[50rem] rounded border border-stone-500 p-5">
            Client 2
          </div>
        </ArcherElement>
      </div>
    </ArcherContainer>
  );
}

export function DurableObjectsDiagram() {
  return (
    <ArcherContainer
      strokeColor="white"
      startMarker={true}
      endMarker={true}
      lineStyle="curve"
      offset={5}
      svgContainerStyle={{ zIndex: 1 }}
    >
      <div className="mb-30 mb-14 flex gap-5 px-40">
        <ArcherElement id="do1">
          <div className="w-[10rem] rounded border border-stone-500 p-5">
            Durable Object
          </div>
        </ArcherElement>
        <ArcherElement id="do2">
          <div className="w-[10rem] rounded border border-stone-500 p-5">
            Durable Object
          </div>
        </ArcherElement>
      </div>

      <div className="flex gap-5 px-40">
        <ArcherElement
          id="trad-client1"
          relations={[
            {
              targetId: "do1",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
            {
              targetId: "do2",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
          ]}
        >
          <div className="rounded border border-stone-500 p-5">Client 1</div>
        </ArcherElement>
        <ArcherElement
          id="trad-client2"
          relations={[
            {
              targetId: "do1",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
            {
              targetId: "do2",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
          ]}
        >
          <div className="rounded border border-stone-500 p-5">Client 2</div>
        </ArcherElement>
      </div>
    </ArcherContainer>
  );
}

export function DurableObjectsDiagram2() {
  return (
    <ArcherContainer
      strokeColor="white"
      startMarker={true}
      endMarker={true}
      lineStyle="curve"
      offset={5}
      svgContainerStyle={{ zIndex: 1 }}
    >
      <div className="mb-30 mb-14 flex gap-5 px-40">
        <ArcherElement id="do1">
          <div className="w-[10rem] rounded border border-stone-500 p-5">
            Durable Object
          </div>
        </ArcherElement>
        <ArcherElement id="do2">
          <div className="w-[10rem] rounded border border-stone-500 p-5">
            Durable Object
          </div>
        </ArcherElement>
      </div>

      <div className="flex gap-5 px-40">
        <ArcherElement
          id="trad-client1"
          relations={[
            {
              targetId: "do1",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
            {
              targetId: "do2",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
          ]}
        >
          <div className="rounded border border-stone-500 p-5">Client 1</div>
        </ArcherElement>
        <ArcherElement
          id="trad-client2"
          relations={[
            {
              targetId: "do1",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
            {
              targetId: "do2",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
          ]}
        >
          <div className="ml-auto rounded border border-stone-500 p-5">
            Client 2
          </div>
        </ArcherElement>
      </div>
    </ArcherContainer>
  );
}

export function DurableObjectsDiagram3({ authority }: { authority?: boolean }) {
  return (
    <ArcherContainer
      strokeColor="white"
      startMarker={true}
      endMarker={true}
      lineStyle="curve"
      offset={5}
      svgContainerStyle={{ zIndex: 1 }}
    >
      <div className="mb-30 mb-14 flex justify-center gap-5 px-40">
        <ArcherElement id="do1">
          <div
            className={clsx(
              "w-[10rem] rounded border p-5",
              authority ? "outline-3 outline-teal-500" : "border-stone-500",
            )}
          >
            Durable Object
          </div>
        </ArcherElement>
        <ArcherElement id="do2">
          <div
            className={clsx(
              "w-[10rem] rounded border p-5",
              authority ? "outline-3 outline-teal-500" : "border-stone-500",
            )}
          >
            Durable Object
          </div>
        </ArcherElement>
      </div>

      <div className="flex gap-5 px-40">
        <ArcherElement
          id="trad-client1"
          relations={[
            {
              targetId: "do1",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
            {
              targetId: "do2",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
          ]}
        >
          <div className="rounded border border-stone-500 p-5">Client 1</div>
        </ArcherElement>
        <ArcherElement
          id="trad-client2"
          relations={[
            {
              targetId: "do1",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
            {
              targetId: "do2",
              targetAnchor: "bottom",
              sourceAnchor: "top",
            },
          ]}
        >
          <div className="ml-auto rounded border border-stone-500 p-5">
            Client 2
          </div>
        </ArcherElement>
      </div>
    </ArcherContainer>
  );
}
