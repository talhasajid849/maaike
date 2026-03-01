import { io } from 'socket.io-client';

// In dev, connect directly to Flask (port 5000) to avoid Vite proxy ECONNABORTED errors.
// In production Flask serves the frontend, so same-origin works.
const SOCKET_URL = import.meta.env.DEV ? 'http://localhost:5000' : undefined;
const SOCKET_OPTS = import.meta.env.DEV
  // Werkzeug + Flask-SocketIO(threading) does not support websocket transport reliably.
  // Force polling in dev to avoid 500 errors on /socket.io?transport=websocket.
  ? { transports: ['polling'], reconnectionAttempts: 5 }
  : { transports: ['websocket', 'polling'], reconnectionAttempts: 5 };

let socket = null;

export function getSocket() {
  if (!socket) {
    socket = io(SOCKET_URL, SOCKET_OPTS);
  }
  return socket;
}

export function disconnectSocket() {
  socket?.disconnect();
  socket = null;
}
