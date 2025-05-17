/**
 * Learn about schemas here:
 * https://jazz.tools/docs/react/schemas/covalues
 */

import { CoList, CoMap, CoPlainText, coField } from "jazz-tools";

export class Issue extends CoMap {
  title = coField.string;
  description = coField.ref(CoPlainText);
  estimate = coField.number;
  status? = coField.literal("backlog", "in progress", "done");
}

export class ListOfIssues extends CoList.Of(coField.ref(Issue)) {}

export class Project extends CoMap {
  name = coField.string;
  issues = coField.ref(ListOfIssues);
}
