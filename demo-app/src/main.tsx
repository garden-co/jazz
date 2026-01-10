import ReactDOM from 'react-dom/client';
import App from './App';
import SyncTest from './SyncTest';
import './index.css';

// Check if we're in sync test mode via URL param
const urlParams = new URLSearchParams(window.location.search);
const isSyncTest = urlParams.has('sync-test');

// Note: StrictMode disabled because the WASM database doesn't handle
// double-mount well (refs persist but WASM state doesn't)
ReactDOM.createRoot(document.getElementById('root')!).render(
  isSyncTest ? <SyncTest /> : <App />
);
