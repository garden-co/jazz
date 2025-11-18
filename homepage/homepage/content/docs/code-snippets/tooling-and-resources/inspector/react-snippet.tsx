// #region Install
import { JazzInspector } from "jazz-tools/inspector";
import { JazzReactProvider } from "jazz-tools/react";

function App() {
  return (
    // @ts-expect-error No sync prop
    <JazzReactProvider>
      {/* [!code ++] */}
      <JazzInspector />
    </JazzReactProvider>
  );
}
// #endregion
function App2() {
  // prettier-ignore
  return (
<>
{/* #region Position */}
<JazzInspector position="bottom left" />
{/* #endregion */}
</>
);
}
