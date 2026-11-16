"use client";

import { useState } from "react";

// The same page as ../jazz without Jazz: the difference is what Jazz adds.
export default function Page() {
  const [count, setCount] = useState(0);
  return <button onClick={() => setCount(count + 1)}>{count}</button>;
}
