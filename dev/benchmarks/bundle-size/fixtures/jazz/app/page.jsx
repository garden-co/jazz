"use client";

import { useState } from "react";
import { JazzProvider } from "jazz-tools/react";

// The default thin client, as an app adopting Jazz in React would add it.
export default function Page() {
  const [count, setCount] = useState(0);
  return (
    <JazzProvider signedOut={<p>Signed out</p>}>
      <button onClick={() => setCount(count + 1)}>{count}</button>
    </JazzProvider>
  );
}
