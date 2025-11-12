"use client";
import { ArcherContainer } from "react-archer";
import { BrowserNode, PhoneNode } from "../infraDiagrams/nodeTypes";
import { Scenario } from "../scenarios";
import { EdgeServerWithClients } from "../infraDiagrams/nodeComposites";
import { CoValueCoreDiagram } from "./diagrams";
import { useState } from "react";
import { userColors } from "./helpers";

export function CoValueSyncDiagram({
  scenario,
  timestampIdx,
  bob1Connection,
  serverEncrypted
}: {
  scenario: Scenario;
  timestampIdx: number;
  bob1Connection: "offline" | number;
  serverEncrypted?: boolean;
}) {
  const [currentTimestampIdx, setCurrentTimestampIdx] = useState(timestampIdx);
  const currentTimestamp = scenario.timestamps[currentTimestampIdx];

  const filteredSessionsBob1 = Object.fromEntries(
    Object.entries(scenario.sessions).flatMap(([key, session]) => {
      if ((bob1Connection === "offline" || bob1Connection < 3) && key !== "bob_session_1") {
        if (key.startsWith("alice")) {
          return [[key, session.slice(0, Math.min(timestampIdx, 2))]];
        }
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
      if ((bob1Connection === "offline" || bob1Connection < 4) && key === "bob_session_1") {
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

  const filteredSessionsAliceAndBob2 = Object.fromEntries(
    Object.entries(scenario.sessions).flatMap(([key, session]) => {
      if ((bob1Connection === "offline" || bob1Connection < 5) &&  key === "bob_session_1") {
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
      svgContainerStyle={{ zIndex: 1 }}
    >
      <EdgeServerWithClients
        regionId="us-east-1"
        upstreamId="core-ingress-0"
        edgeClassName="w-xl h-auto mb-20"
        edgeChildren={
          <div className="max-h-50 border border-transparent mt-5">
            <div className="w-[166%] origin-top-left scale-[0.6]">
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
          upstreamId={"edgeServer-us-east-1"}
          className="w-lg h-auto max-h-80"
          name={<><span className={userColors["alice"]}>Alice</span>'s Browser</>}
        >
          <div className="border border-transparent mt-5">
            <div className="w-[200%] origin-top-left scale-[0.5] p-10">
              <CoValueCoreDiagram
                header={scenario.header}
                sessions={filteredSessionsAliceAndBob2}
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
        <BrowserNode
          id="phone-1-us-east-1"
          upstreamId={bob1Connection !== "offline" ? "edgeServer-us-east-1" : undefined}
          className="w-lg h-auto max-h-80 ml-10"
          name={<><span className={userColors["bob"]}>Bob</span>'s Browser</>}
        >
          <div className="border border-transparent mt-5">
            <div className="w-[200%] origin-top-left scale-[0.5] p-10">
              <CoValueCoreDiagram
                header={scenario.header}
                sessions={filteredSessionsBob1}
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
          id="phone-2-us-east-1"
          upstreamId="edgeServer-us-east-1"
          className="w-lg h-auto max-h-80"
          name={<><span className={userColors["bob"]}>Bob</span>'s Tablet</>}
        >
          <div className="border border-transparent mt-5">
            <div className="w-[200%] origin-top-left scale-[0.5] p-10">
              <CoValueCoreDiagram
                header={scenario.header}
                sessions={filteredSessionsAliceAndBob2}
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
