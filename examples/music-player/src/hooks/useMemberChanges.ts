import { useCallback, useReducer } from "react";
import { produce } from "immer";
import type { Account, Group } from "jazz-tools";

export type MemberRole = "reader" | "writer" | "manager";
export type PendingMemberChange =
  | { type: "setRole"; role: MemberRole }
  | { type: "remove" };

type State = {
  pendingByMemberId: Record<string, PendingMemberChange>;
};

type Action =
  | {
      type: "stageRole";
      memberId: string;
      currentRole: string | undefined;
      newRole: MemberRole;
    }
  | { type: "toggleRemove"; memberId: string }
  | { type: "discard" }
  | { type: "reset" };

const initialState: State = {
  pendingByMemberId: {},
};

function reducer(state: State, action: Action): State {
  return produce(state, (draft) => {
    switch (action.type) {
      case "stageRole": {
        const prev = draft.pendingByMemberId[action.memberId];
        if (prev?.type === "remove") return;

        // If it matches currentRole, clear any pending role change.
        if (action.currentRole === action.newRole) {
          delete draft.pendingByMemberId[action.memberId];
          return;
        }

        draft.pendingByMemberId[action.memberId] = {
          type: "setRole",
          role: action.newRole,
        };
        return;
      }
      case "toggleRemove": {
        const prev = draft.pendingByMemberId[action.memberId];
        if (prev?.type === "remove") {
          delete draft.pendingByMemberId[action.memberId];
          return;
        }
        draft.pendingByMemberId[action.memberId] = { type: "remove" };
        return;
      }
      case "discard": {
        draft.pendingByMemberId = {};
        return;
      }
      case "reset": {
        return initialState;
      }
    }
  });
}

export function useMemberChanges() {
  const [state, dispatch] = useReducer(reducer, initialState);

  const hasPendingChanges = Object.keys(state.pendingByMemberId).length > 0;

  const stageRoleChange = useCallback(
    (args: {
      memberId: string;
      currentRole: string | undefined;
      newRole: MemberRole;
    }) => {
      dispatch({ type: "stageRole", ...args });
    },
    [],
  );

  const toggleRemove = useCallback((memberId: string) => {
    dispatch({ type: "toggleRemove", memberId });
  }, []);

  const discard = useCallback(() => {
    dispatch({ type: "discard" });
  }, []);

  const reset = useCallback(() => {
    dispatch({ type: "reset" });
  }, []);

  const apply = useCallback(
    async (args: { group: Group; members: Account[] }) => {
      if (!hasPendingChanges) return;

      const byId = new Map(args.members.map((m) => [m.$jazz.id, m] as const));
      const toApply = Object.entries(state.pendingByMemberId);

      for (const [memberId, change] of toApply) {
        const member = byId.get(memberId);

        if (!member) continue;
        if (change.type === "remove") {
          args.group.removeMember(member);
        } else if (change.type === "setRole") {
          args.group.addMember(member, change.role);
        }
      }

      dispatch({ type: "discard" });
    },
    [hasPendingChanges, state.pendingByMemberId],
  );

  return {
    pendingByMemberId: state.pendingByMemberId,
    hasPendingChanges,
    stageRoleChange,
    toggleRemove,
    discard,
    reset,
    apply,
  };
}
