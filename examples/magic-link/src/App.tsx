import { Route, Routes } from "react-router";
import HomePage from "./components/pages/HomePage.tsx";
import MagicLinkHandlerConsumerPage from "./components/pages/MagicLinkHandlerConsumer.tsx";
import MagicLinkHandlerProviderPage from "./components/pages/MagicLinkHandlerProvider.tsx";

function App() {
  return (
    <Routes>
      <Route path="/" element={<HomePage />} />

      <Route
        path="/magic-link-handler-consumer/:transferId/:inviteSecret"
        element={<MagicLinkHandlerConsumerPage />}
      />
      <Route
        path="/magic-link-handler-provider/:transferId/:inviteSecret"
        element={<MagicLinkHandlerProviderPage />}
      />
    </Routes>
  );
}

export default App;
