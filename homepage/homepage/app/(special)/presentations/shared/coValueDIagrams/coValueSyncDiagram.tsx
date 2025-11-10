"use client";
import { ArcherContainer } from "react-archer";
import { BrowserNode, PhoneNode } from "../infraDiagrams/nodeTypes";
import { Scenario } from "../scenarios";
import { EdgeServerWithClients } from "../infraDiagrams/nodeComposites";
import { CoValueCoreDiagram } from "./diagrams";
import { useState } from "react";

export function CoValueSyncDiagram({
  scenario,
  timestampIdx,
  aliceConnection,
  serverEncrypted
}: {
  scenario: Scenario;
  timestampIdx: number;
  aliceConnection: "offline" | number;
  serverEncrypted?: boolean;
}) {
  const [currentTimestampIdx, setCurrentTimestampIdx] = useState(timestampIdx);
  const currentTimestamp = scenario.timestamps[currentTimestampIdx];

  const filteredSessionsAlice = Object.fromEntries(
    Object.entries(scenario.sessions).flatMap(([key, session]) => {
      if ((aliceConnection === "offline" || aliceConnection < 3) && !key.startsWith("alice")) {
        return [];
      }
      const filteredSession = session.filter(
        (entry) => entry.t <= currentTimestamp,
      );

      if (filteredSession.length === 0) {
        return [];
      }

      return [[key, session.filter((entry) => entry.t <= currentTimestamp)]];
    }),
  );

  const filteredSessionsSyncServer = Object.fromEntries(
    Object.entries(scenario.sessions).flatMap(([key, session]) => {
      if ((aliceConnection === "offline" || aliceConnection < 4) && key.startsWith("alice")) {
        return [];
      }
      const filteredSession = session.filter(
        (entry) => entry.t <= currentTimestamp,
      );

      if (filteredSession.length === 0) {
        return [];
      }

      return [[key, session.filter((entry) => entry.t <= currentTimestamp)]];
    }),
  );

  const filteredSessionsBob = Object.fromEntries(
    Object.entries(scenario.sessions).flatMap(([key, session]) => {
      if ((aliceConnection === "offline" || aliceConnection < 5) &&  key.startsWith("alice")) {
        return [];
      }
      const filteredSession = session.filter(
        (entry) => entry.t <= currentTimestamp,
      );

      if (filteredSession.length === 0) {
        return [];
      }

      return [[key, session.filter((entry) => entry.t <= currentTimestamp)]];
    }),
  );

  return (
    <ArcherContainer
      strokeColor="white"
      startMarker={true}
      endMarker={true}
      lineStyle="curve"
      offset={5}
    >
      <EdgeServerWithClients
        regionId="us-east-1"
        upstreamId="core-ingress-0"
        edgeClassName="w-lg h-auto p-10"
        edgeChildren={
          <div className="max-h-50 border border-transparent pt-5">
            <div className="w-[200%] origin-top-left scale-[0.5]">
              <CoValueCoreDiagram
                header={scenario.header}
                sessions={filteredSessionsSyncServer}
                showView={false}
                showCore={true}
                showHashAndSignature={false}
                encryptedItems={!!serverEncrypted}
                showEditor={false}
                currentTimestamp={scenario.timestamps[timestampIdx]}
              />
            </div>
          </div>
        }
      >
        <BrowserNode
          id="browser-1-us-east-1"
          upstreamId={aliceConnection !== "offline" ? "edgeServer-us-east-1" : undefined}
          className="w-lg h-auto max-h-80 p-10"
        >
          <div className="border border-transparent pt-5">
            <div className="w-[200%] origin-top-left scale-[0.5]">
              <CoValueCoreDiagram
                header={scenario.header}
                sessions={filteredSessionsAlice}
                showView={true}
                showCore={true}
                showHashAndSignature={false}
                encryptedItems={false}
                showEditor={false}
                currentTimestamp={scenario.timestamps[timestampIdx]}
              />
            </div>
          </div>
        </BrowserNode>
        <PhoneNode
          id="phone-1-us-east-1"
          upstreamId="edgeServer-us-east-1"
          className="w-lg h-auto max-h-80 p-10"
        >
          <div className="border border-transparent pt-5">
            <div className="w-[200%] origin-top-left scale-[0.5]">
              <CoValueCoreDiagram
                header={scenario.header}
                sessions={filteredSessionsBob}
                showView={true}
                showCore={true}
                showHashAndSignature={false}
                encryptedItems={false}
                showEditor={false}
                currentTimestamp={scenario.timestamps[timestampIdx]}
              />
            </div>
          </div>
        </PhoneNode>
        <PhoneNode
          id="phone-2-us-east-1"
          upstreamId="edgeServer-us-east-1"
          className="w-lg h-auto max-h-80 p-10"
        >
          <div className="border border-transparent pt-5">
            <div className="w-[200%] origin-top-left scale-[0.5]">
              <CoValueCoreDiagram
                header={scenario.header}
                sessions={filteredSessionsBob}
                showView={true}
                showCore={true}
                showHashAndSignature={false}
                encryptedItems={false}
                showEditor={false}
                currentTimestamp={scenario.timestamps[timestampIdx]}
              />
            </div>
          </div>
        </PhoneNode>
      </EdgeServerWithClients>
    </ArcherContainer>
  );
}
