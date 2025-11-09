"use client";

import { ArcherContainer } from "react-archer";
import { BrowserNode, PhoneNode } from "../shared/infraDiagrams/nodeTypes";
import { CoreWithRegions, EdgeServerWithClients } from "../shared/infraDiagrams/nodeComposites";

export function NewSyncDiagram() {
  return (
    <ArcherContainer
      strokeColor="white"
      startMarker={true}
      endMarker={true}
      lineStyle="curve"
      offset={5}
    >
      <CoreWithRegions nShards={8}>
        <EdgeServerWithClients regionId="us-east-1" upstreamId="core-ingress-0">
          <BrowserNode id="browser-1-us-east-1" upstreamId="edgeServer-us-east-1" />
          <PhoneNode id="phone-1-us-east-1" upstreamId="edgeServer-us-east-1" />
          <PhoneNode id="phone-2-us-east-1" upstreamId="edgeServer-us-east-1" />
        </EdgeServerWithClients>
        <EdgeServerWithClients regionId="us-west-1" upstreamId="core-ingress-1">
          <BrowserNode id="browser-1-us-west-1" upstreamId="edgeServer-us-west-1" />
          <PhoneNode id="phone-1-us-west-1" upstreamId="edgeServer-us-west-1" />
          <PhoneNode id="phone-2-us-west-1" upstreamId="edgeServer-us-west-1" />
        </EdgeServerWithClients>
      </CoreWithRegions>
    </ArcherContainer>
  );
}

