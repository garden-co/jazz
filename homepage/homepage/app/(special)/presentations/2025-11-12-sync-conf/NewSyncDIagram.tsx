"use client";

import { AnimatedSvgEdge } from "@/components/animated-svg-edge";
import { Background, Handle, Position, ReactFlow } from "@xyflow/react";
import "@xyflow/react/dist/style.css";

const edgeTypes = {
  animatedSvgEdge: AnimatedSvgEdge,
};

const nodeTypes = {
  browser: BrowserNode,
  phone: PhoneNode,
  edgeServer: EdgeServerNode,
  shardServer: ShardServerNode,
  lambda: LambdaNode,
};

export function NewSyncDiagram() {
  const defaultNodes = [
    {
      id: "browser",
      type: "browser",
      position: { x: 200, y: 800 },
      data: { label: "A" },
    },
    {
      id: "phone",
      type: "phone",
      position: { x: 600, y: 800 },
      data: { label: "B" },
    },
    {
      id: "lambda",
      type: "lambda",
      position: { x: 500, y: 350 },
      data: { label: "lambda" },
    },
    {
      id: "edgeServer",
      type: "edgeServer",
      position: { x: 300, y: 200 },
      data: { label: "C" },
    },
    {
      id: "ingressServer",
      type: "edgeServer",
      position: { x: 300, y: -200 },
      data: { label: "D" },
    },
    {
      id: "shardServer1",
      type: "shardServer",
      position: { x: 225, y: -400 },
      data: { label: "E" },
    },
    {
      id: "shardServer2",
      type: "shardServer",
      position: { x: 325, y: -400 },
      data: { label: "F" },
    },
    {
      id: "shardServer3",
      type: "shardServer",
      position: { x: 425, y: -400 },
      data: { label: "G" },
    },
    {
      id: "shardServer4",
      type: "shardServer",
      position: { x: 525, y: -400 },
      data: { label: "H" },
    },
  ];

  const defaultEdges = [
    {
      id: "browser->edgeServer",
      source: "browser",
      target: "edgeServer",
      type: "animatedSvgEdge",
      data: {
        duration: 0.3,
        shape: "circle",
        direction: "alternate",
        path: "bezier",
      },
    } satisfies AnimatedSvgEdge,
    {
      id: "phone->edgeServer",
      source: "phone",
      target: "edgeServer",
      type: "animatedSvgEdge",
      data: {
        duration: 0.3,
        shape: "circle",
        direction: "alternate",
        path: "bezier",
      },
    } satisfies AnimatedSvgEdge,
    {
      id: "lambda->edgeServer",
      source: "lambda",
      target: "edgeServer",
      type: "animatedSvgEdge",
      data: {
        duration: 0.3,
        shape: "circle",
        direction: "alternate",
        path: "bezier",
      },
    } satisfies AnimatedSvgEdge,
    {
      id: "edgeServer->ingressServer",
      source: "edgeServer",
      target: "ingressServer",
      type: "animatedSvgEdge",
      data: {
        duration: 0.3,
        shape: "circle",
        direction: "alternate",
        path: "bezier",
      },
    } satisfies AnimatedSvgEdge,
    {
      id: "ingressServer->shardServer1",
      source: "ingressServer",
      target: "shardServer1",
      type: "animatedSvgEdge",
      data: {
        duration: 0.3,
        shape: "circle",
        direction: "alternate",
        path: "bezier",
      },
    } satisfies AnimatedSvgEdge,
    {
      id: "ingressServer->shardServer2",
      source: "ingressServer",
      target: "shardServer2",
      type: "animatedSvgEdge",
      data: {
        duration: 0.3,
        shape: "circle",
        direction: "alternate",
        path: "bezier",
      },
    } satisfies AnimatedSvgEdge,
    {
      id: "ingressServer->shardServer3",
      source: "ingressServer",
      target: "shardServer3",
      type: "animatedSvgEdge",
      data: {
        duration: 0.3,
        shape: "circle",
        direction: "alternate",
        path: "bezier",
      },
    } satisfies AnimatedSvgEdge,
    {
      id: "ingressServer->shardServer4",
      source: "ingressServer",
      target: "shardServer4",
      type: "animatedSvgEdge",
      data: {
        duration: 0.3,
        shape: "circle",
        direction: "alternate",
        path: "bezier",
      },
    } satisfies AnimatedSvgEdge,
  ];

  return (
    <div className="h-full w-full">
      <ReactFlow
        nodeTypes={nodeTypes}
        defaultNodes={defaultNodes}
        edgeTypes={edgeTypes}
        defaultEdges={defaultEdges}
        fitView
      >
        <Background />
      </ReactFlow>
    </div>
  );
}

export function BrowserNode() {
  return (
    <div className="h-40 w-64 rounded bg-blue-500 p-5 -z-1">
      <div className="rounded-md border border-white p-2 relative text-center">
        <Handle type="source" position={Position.Top} />
        local state
      </div>
    </div>
  );
}

export function PhoneNode() {
  return (
    <div className="h-40 w-24 rounded bg-red-500 p-5 -z-1">
      <div className="rounded-md border border-white p-2 relative text-center">
        <Handle type="source" position={Position.Top} />
        local state
      </div>
    </div>
  );
}

export function LambdaNode() {
  return (
    <div className="h-30 w-40 rounded bg-yellow-500 p-5 -z-1">
      <div className="rounded-md border border-white p-2 relative text-center">
        <Handle type="source" position={Position.Top} />
        <Handle type="target" position={Position.Bottom} />
        local state
      </div>
    </div>
  );
}

export function EdgeServerNode() {
  return (
    <div className="h-20 w-64 rounded bg-green-500 p-5 -z-1">
      <div className="rounded-md border border-white p-2 relative text-center">
        <Handle type="source" position={Position.Top} />
        <Handle type="target" position={Position.Bottom} />
        SSD cache
      </div>
    </div>
  );
}

export function ShardServerNode() {
  return (
    <div className="h-40 w-24 rounded bg-green-500 p-5 -z-1">
      <div className="rounded-md border border-white p-2 relative text-center">
        <Handle type="source" position={Position.Top} />
        <Handle type="target" position={Position.Bottom} />
        HDD
      </div>
    </div>
  );
}