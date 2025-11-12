"use client";

import { ArcherContainer } from "react-archer";
import {
  BrowserNode,
  CoreIngressNode,
  PhoneNode,
} from "../shared/infraDiagrams/nodeTypes";
import {
  CoreWithRegions,
  EdgeServerWithClients,
} from "../shared/infraDiagrams/nodeComposites";

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
      <EdgeServerWithClients regionId="us-east-1">
        <BrowserNode
          id="browser-1-us-east-1"
          upstreamId="edgeServer-us-east-1"
        />
        <BrowserNode id="phone-1-us-east-1" upstreamId="edgeServer-us-east-1" />
        <PhoneNode id="phone-2-us-east-1" upstreamId="edgeServer-us-east-1" />
      </EdgeServerWithClients>
    </ArcherContainer>
  );
}

export function SimpleNewSyncDiagram() {
  return (
    <ArcherContainer
      strokeColor="white"
      startMarker={true}
      endMarker={true}
      lineStyle="curve"
      offset={5}
      svgContainerStyle={{ zIndex: 1 }}
    >
      <div className="mb-40 flex justify-center">
        <CoreIngressNode id="core" shardIds={[]} />
      </div>
      <div className="flex">
        <EdgeServerWithClients regionId="us-east-1" upstreamId="core">
          <BrowserNode
            id="browser-1-us-east-1"
            upstreamId="edgeServer-us-east-1"
          />
          <PhoneNode id="phone-1-us-east-1" upstreamId="edgeServer-us-east-1" />
          <PhoneNode id="phone-2-us-east-1" upstreamId="edgeServer-us-east-1" />
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
        <EdgeServerWithClients regionId="us-east-1" upstreamId="core-ingress-0">
          <BrowserNode
            id="browser-1-us-east-1"
            upstreamId="edgeServer-us-east-1"
          />
          <PhoneNode id="phone-1-us-east-1" upstreamId="edgeServer-us-east-1" />
          <PhoneNode id="phone-2-us-east-1" upstreamId="edgeServer-us-east-1" />
        </EdgeServerWithClients>
        <EdgeServerWithClients regionId="us-west-1" upstreamId="core-ingress-1">
          <BrowserNode
            id="browser-1-us-west-1"
            upstreamId="edgeServer-us-west-1"
          />
          <PhoneNode id="phone-1-us-west-1" upstreamId="edgeServer-us-west-1" />
          <PhoneNode id="phone-2-us-west-1" upstreamId="edgeServer-us-west-1" />
        </EdgeServerWithClients>
      </CoreWithRegions>
    </ArcherContainer>
  );
}
