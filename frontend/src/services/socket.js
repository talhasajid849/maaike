/**
 * services/socket.js
 * ===================
 * Singleton WebSocket connection.
 */
import { io } from 'socket.io-client';

const SOCKET_URL  = import.meta.env.DEV ? 'http://localhost:5000' : undefined;
const SOCKET_OPTS = import.meta.env.DEV
  ? { transports: ['polling'], reconnectionAttempts: 5 }
  : { transports: ['websocket', 'polling'], reconnectionAttempts: 5 };

let socket = null;

export function getSocket() {
  if (!socket) socket = io(SOCKET_URL, SOCKET_OPTS);
  return socket;
}

export function disconnectSocket() {
  socket?.disconnect();
  socket = null;
}