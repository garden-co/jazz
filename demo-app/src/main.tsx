import ReactDOM from 'react-dom/client';
import App from './App';
import './index.css';

// Note: StrictMode disabled because the WASM database doesn't handle
// double-mount well (refs persist but WASM state doesn't)
ReactDOM.createRoot(document.getElementById('root')!).render(<App />);
