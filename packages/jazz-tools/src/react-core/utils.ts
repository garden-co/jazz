import { Account, JazzContextManager } from "jazz-tools";

export function getCurrentAccountFromContextManager<Acc extends Account>(
  contextManager: JazzContextManager<Acc, any>,
) {
  const context = contextManager.getCurrentValue();

  if (!context) {
    throw new Error("No context found");
  }

  return "me" in context ? context.me : context.guest;
}

export function subscribeToContextManager<Acc extends Account>(
  contextManager: JazzContextManager<Acc, any>,
  callback: () => () => void,
) {
  let unsub = () => {};

  const handler = () => {
    unsub();
    unsub = callback();
  };

  handler();
  return contextManager.subscribe(handler);
}
