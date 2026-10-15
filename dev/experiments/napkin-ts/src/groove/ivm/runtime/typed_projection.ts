// Reusable, source-independent projection compilation.
import type { IvmRuntime, RecordDescriptor } from "../../../_extern/groove-ivm-plan.js";
import type { ProjectField } from "../graph.js";
import { type ProjectionExpr, structKey } from "../op_types.js";
import { projectDescriptor, projectFieldExpr } from "./record_projection.js";

const PLAN_SLOTS = 512;
// Bound retained key payload as well as entry count.
const MAX_KEY_BYTES = 8 * 1024;

export class ProjectionPlan {
  private cached?: ProjectionExpr[];
  constructor(
    readonly input: RecordDescriptor,
    readonly fields: ProjectField[],
    readonly key: string,
    readonly output: RecordDescriptor,
  ) {}

  expressions(): ProjectionExpr[] {
    // Inferring an output alone must not validate execution.
    return (this.cached ??= this.fields.map((field) => ({
      expression: projectFieldExpr(this.input, field),
      outputName: field.outputName,
      outputIdentity: field.outputIdentity,
    })));
  }
}

function hashKey(key: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < key.length; i++) h = Math.imul(h ^ key.charCodeAt(i), 0x01000193);
  return h >>> 0;
}

export class ProjectionPlans {
  // Direct mapping bounds metadata; a hash collision is a miss, never equivalence.
  private slots: (ProjectionPlan | undefined)[] = new Array(PLAN_SLOTS);

  get(input: RecordDescriptor, fields: ProjectField[]): ProjectionPlan {
    const key = structKey([input, fields]);
    const slot = hashKey(key) % PLAN_SLOTS;
    const hit = this.slots[slot];
    if (hit && hit.input === input && hit.key === key) return hit;
    const plan = new ProjectionPlan(input, fields.slice(), key, projectDescriptor(input, fields));
    if (key.length <= MAX_KEY_BYTES) this.slots[slot] = plan;
    return plan;
  }
}

export function projectionPlan(
  rt: IvmRuntime,
  input: RecordDescriptor,
  fields: ProjectField[],
): ProjectionPlan {
  return (rt.projectionPlans as ProjectionPlans).get(input, fields);
}
