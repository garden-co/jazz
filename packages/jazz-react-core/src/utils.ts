import { Account, JazzContextManager } from "jazz-tools";

export function getCurrentAccountFromContextManager<Acc extends Account>(
  contextManager: JazzContextManager<Acc, any>,
) {
  const context = contextManager.getCurrentValue();

  if (!context) {
    return null;
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
