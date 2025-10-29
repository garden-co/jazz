// #region Install
import { JazzInspector } from "jazz-tools/inspector";
import { JazzReactProvider } from "jazz-tools/react";

// @ts-expect-error No sync prop
<JazzReactProvider>
  // [!code ++]
  <JazzInspector />
</JazzReactProvider>;
// #endregion

// #region Position
<JazzInspector position="bottom left" />;
// #endregion
