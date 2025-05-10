import { Route, Routes } from "react-router";
import CrossDeviceAccountTransferHandlerSourcePage from "./components/pages/CrossDeviceAccountTransferHandlerSource.tsx";
import CrossDeviceAccountTransferHandlerTargetPage from "./components/pages/CrossDeviceAccountTransferHandlerTarget.tsx";
import HomePage from "./components/pages/HomePage.tsx";

function App() {
  return (
    <Routes>
      <Route path="/" element={<HomePage />} />

      <Route
        path="/accept-account-transfer/:transferId/:inviteSecret"
        element={<CrossDeviceAccountTransferHandlerTargetPage />}
      />
      <Route
        path="/share-current-account/:transferId/:inviteSecret"
        element={<CrossDeviceAccountTransferHandlerSourcePage />}
      />
    </Routes>
  );
}

export default App;
