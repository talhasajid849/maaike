import { io } from 'socket.io-client';

// In dev, connect directly to Flask (port 5000) to avoid Vite proxy ECONNABORTED errors.
// In production Flask serves the frontend, so same-origin works.
const SOCKET_URL = import.meta.env.DEV ? 'http://localhost:5000' : undefined;

let socket = null;

export function getSocket() {
  if (!socket) {
    socket = io(SOCKET_URL, { transports: ['websocket', 'polling'], reconnectionAttempts: 5 });
  }
  return socket;
}

export function disconnectSocket() {
  socket?.disconnect();
  socket = null;
}
