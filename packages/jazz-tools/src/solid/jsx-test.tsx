import { createSignal } from "solid-js";

export const HelloFromJSX = () => {
  const [count, setCount] = createSignal(0);
  const increment = () => setCount(count() + 1);

  return (
    <div>
      Hello From JSX
      <button onClick={increment}>Click ME {count()}</button>
    </div>
  );
};
