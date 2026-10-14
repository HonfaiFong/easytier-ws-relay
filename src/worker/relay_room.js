import { Buffer } from 'node:buffer';
import { parseHeader } from './core/packet.js';
import { PacketType, HEADER_SIZE, MY_PEER_ID } from './core/constants.js';
import { loadProtos } from './core/protos.js';
import { handleHandshake, handlePing, handleForwarding } from './core/basic_handlers.js';
import { handleRpcReq, handleRpcResp } from './core/rpc_handler.js';
import { PeerManager } from './core/peer_manager.js';
import { randomU64String } from './core/crypto.js';
import { authorizeRelay, positiveLimit } from './core/security.js';

const VALID_PACKET_TYPES = new Set(Object.values(PacketType).filter(t => t !== PacketType.Invalid));

function getWebSocketPath(env) {
  const path = String(env.WS_PATH || 'ws');
  return path.startsWith('/') ? path : `/${path}`;
}

export class RelayRoom {
  constructor(state, env = {}) {
    this.state = state;
    this.env = env;
    this.types = loadProtos();
    this.peerManager = new PeerManager(env);
    this.peerManager.setTypes(this.types);
    this.peerManager.sendToSocket = (ws, bytes) => this.sendToSocket(ws, bytes);
    this.maxConnections = positiveLimit(env, 'EASYTIER_MAX_CONNECTIONS', 128, this.peerManager.maxRoutePeers);
    this.maxFrameBytes = positiveLimit(env, 'EASYTIER_MAX_FRAME_BYTES', 1024 * 1024);
    this.maxMessagesPerSecond = positiveLimit(env, 'EASYTIER_MAX_MESSAGES_PER_SECOND', 1000, 10000);
    this.maxBytesPerSecond = positiveLimit(env, 'EASYTIER_MAX_BYTES_PER_SECOND', 8 * 1024 * 1024, 32 * 1024 * 1024);
    this.maxOutboundFrameBytes = positiveLimit(env, 'EASYTIER_MAX_OUTBOUND_FRAME_BYTES', 1024 * 1024);
    this.maxOutboundBytesPerSecond = positiveLimit(env, 'EASYTIER_MAX_OUTBOUND_BYTES_PER_SECOND', 8 * 1024 * 1024);
    this.maxRoomOutboundBytesPerSecond = positiveLimit(env, 'EASYTIER_MAX_ROOM_OUTBOUND_BYTES_PER_SECOND', 32 * 1024 * 1024);
    this.maxOutboundBufferedBytes = positiveLimit(env, 'EASYTIER_MAX_OUTBOUND_BUFFERED_BYTES', 4 * 1024 * 1024);
    this.maxRpcResponseBytes = positiveLimit(env, 'EASYTIER_MAX_RPC_RESPONSE_BYTES', 256 * 1024);
    this.maxGlobalMapRequestsPerSecond = positiveLimit(env, 'EASYTIER_MAX_GLOBAL_MAP_REQUESTS_PER_SECOND', 4);
    this.outboundWindowStart = Date.now();
    this.outboundWindowBytes = 0;
    state.getWebSockets().forEach(ws => this._restoreSocket(ws));
  }

  async fetch(request) {
    const url = new URL(request.url);
    const wsPath = getWebSocketPath(this.env);
    if (url.pathname !== wsPath && url.pathname !== wsPath + '/') return new Response('Not found', { status: 404 });
    if (request.headers.get('Upgrade')?.toLowerCase() !== 'websocket') return new Response('Expected websocket', { status: 400 });
    if (!authorizeRelay(request, this.env)) return new Response('Unauthorized', { status: 401 });
    if (this.state.getWebSockets().filter(ws => ws.readyState === 1).length >= this.maxConnections) {
      return new Response('Room capacity exceeded', { status: 503 });
    }
    const pair = new WebSocketPair();
    await this.handleSession(pair[1]);
    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  async handleSession(ws) {
    this.state.acceptWebSocket(ws);
    this._initSocket(ws);
  }

  async webSocketMessage(ws, message) {
    if (ws.rejected) return;
    try {
      let buffer;
      if (message instanceof ArrayBuffer) buffer = Buffer.from(message);
      else if (ArrayBuffer.isView(message)) buffer = Buffer.from(message.buffer, message.byteOffset, message.byteLength);
      else return this.rejectSocket(ws, 1003, 'Binary packets required');
      if (buffer.length > this.maxFrameBytes) return this.rejectSocket(ws, 1009, 'Frame too large');
      const header = parseHeader(buffer);
      if (!header || !VALID_PACKET_TYPES.has(header.packetType)) return this.rejectSocket(ws, 1002, 'Invalid header');
      // Encryption adds a trailer; compression retains the original length in the header.
      const opaquePayload = (header.flags & 0x11) !== 0;
      if (header.len > this.maxFrameBytes - HEADER_SIZE ||
          (!opaquePayload && header.len !== buffer.length - HEADER_SIZE) ||
          (opaquePayload && buffer.length === HEADER_SIZE)) {
        return this.rejectSocket(ws, 1002, 'Invalid packet length');
      }
      const forServer = header.toPeerId === MY_PEER_ID || header.toPeerId === 0;
      if (opaquePayload && (header.packetType === PacketType.HandShake || forServer)) {
        return this.rejectSocket(ws, 1003, 'Encoded control packets unsupported');
      }
      const now = Date.now();
      if (now - ws.rateWindowStart >= 1000) {
        ws.rateWindowStart = now;
        ws.rateWindowCount = 0;
        ws.rateWindowBytes = 0;
      }
      if (++ws.rateWindowCount > this.maxMessagesPerSecond) return this.rejectSocket(ws, 1008, 'Message rate exceeded');
      ws.rateWindowBytes += buffer.length;
      if (ws.rateWindowBytes > this.maxBytesPerSecond) return this.rejectSocket(ws, 1008, 'Byte rate exceeded');
      ws.lastSeen = now;
      if (header.packetType === PacketType.HandShake) {
        handleHandshake(ws, header, buffer.subarray(HEADER_SIZE), this.types, this);
        return;
      }
      if (!ws.handshakeComplete || header.fromPeerId !== ws.peerId ||
          this.peerManager.getPeerWs(ws.peerId, ws.groupKey) !== ws) {
        return this.rejectSocket(ws, 1008, 'Unauthorized peer');
      }
      if (ws.routeResyncPending) {
        ws.routeResyncPending = false;
        this.peerManager.pushRouteUpdateTo(ws.peerId, ws, this.types, { forceFull: true });
      }
      const payload = buffer.subarray(HEADER_SIZE);
      if (header.packetType === PacketType.Ping && forServer) handlePing(ws, header, payload, this);
      else if (header.packetType === PacketType.RpcReq && forServer) handleRpcReq(ws, header, payload, this.types, this);
      else if (header.packetType === PacketType.RpcResp && forServer) handleRpcResp(ws, header, payload, this.types, this);
      else handleForwarding(ws, header, buffer, this.types, this);
    } catch (e) {
      console.warn('Packet rejected:', e.message);
      if (!ws.rejected) this.rejectSocket(ws, 1002, 'Invalid packet');
    }
  }

  sendToSocket(ws, bytes) {
    if (ws.readyState !== 1) throw new Error('Socket closed');
    if (bytes.length > this.maxOutboundFrameBytes) {
      this.rejectSocket(ws, 1009, 'Outbound frame too large');
      throw new Error('Outbound frame too large');
    }
    if (typeof ws.bufferedAmount === 'number' &&
        ws.bufferedAmount + bytes.length > this.maxOutboundBufferedBytes) {
      this.rejectSocket(ws, 1008, 'Slow consumer');
      throw new Error('Slow consumer');
    }
    const now = Date.now();
    if (now - this.outboundWindowStart >= 1000) {
      this.outboundWindowStart = now;
      this.outboundWindowBytes = 0;
    }
    if (now - ws.outboundWindowStart >= 1000) {
      ws.outboundWindowStart = now;
      ws.outboundWindowBytes = 0;
    }
    if (ws.outboundWindowBytes + bytes.length > this.maxOutboundBytesPerSecond) {
      this.rejectSocket(ws, 1008, 'Outbound byte rate exceeded');
      throw new Error('Outbound byte rate exceeded');
    }
    if (this.outboundWindowBytes + bytes.length > this.maxRoomOutboundBytesPerSecond) {
      throw new Error('Room outbound byte rate exceeded');
    }
    ws.send(bytes);
    ws.outboundWindowBytes += bytes.length;
    this.outboundWindowBytes += bytes.length;
  }

  checkExpensiveRpc(ws) {
    const now = Date.now();
    if (now - ws.expensiveRpcWindowStart >= 1000) {
      ws.expensiveRpcWindowStart = now;
      ws.expensiveRpcWindowCount = 0;
    }
    if (++ws.expensiveRpcWindowCount > this.maxGlobalMapRequestsPerSecond) {
      throw new Error('Global map request rate exceeded');
    }
  }

  rejectSocket(ws, code, reason) {
    ws.rejected = true;
    ws.handshakeComplete = false;
    this.saveSocketMeta(ws);
    this.webSocketClose(ws);
    try { ws.close(code, reason); } catch (_) { }
  }

  async webSocketClose(ws) {
    if (this.peerManager.removePeer(ws)) this.peerManager.broadcastRouteUpdate(this.types, ws.groupKey);
  }

  async webSocketError(ws) {
    this.rejectSocket(ws, 1011, 'WebSocket error');
  }

  saveSocketMeta(ws) {
    ws.serializeAttachment({
      schemaVersion: 1,
      peerId: ws.peerId, groupKey: ws.groupKey, domainName: ws.domainName,
      networkDigestHex: ws.networkDigestHex,
      handshakeComplete: ws.handshakeComplete, rejected: ws.rejected,
      serverSessionId: ws.serverSessionId, weAreInitiator: ws.weAreInitiator,
    });
  }

  _initSocket(ws, meta = {}) {
    ws.peerId = meta.peerId ?? null;
    ws.groupKey = meta.groupKey ?? null;
    ws.domainName = meta.domainName ?? null;
    ws.networkDigestHex = meta.networkDigestHex ?? null;
    ws.handshakeComplete = meta.handshakeComplete === true;
    ws.rejected = meta.rejected === true;
    ws.lastSeen = Date.now();
    ws.rateWindowStart = ws.lastSeen;
    ws.rateWindowCount = 0;
    ws.rateWindowBytes = 0;
    ws.outboundWindowStart = ws.lastSeen;
    ws.outboundWindowBytes = 0;
    ws.expensiveRpcWindowStart = ws.lastSeen;
    ws.expensiveRpcWindowCount = 0;
    ws.serverSessionId = meta.serverSessionId || randomU64String();
    ws.weAreInitiator = meta.weAreInitiator === true;
    ws.crypto = { enabled: false };
    this.saveSocketMeta(ws);
  }

  _restoreSocket(ws) {
    if (ws.readyState !== 1) return;
    const meta = ws.deserializeAttachment() || {};
    this._initSocket(ws, meta);
    if (meta.schemaVersion === 1 && !ws.handshakeComplete && !ws.rejected && !ws.peerId) return;
    if (ws.rejected || !ws.handshakeComplete || !ws.peerId || !ws.groupKey) {
      this.rejectSocket(ws, 1008, 'Reconnect required');
      return;
    }
    if (!this.peerManager.addPeer(ws.peerId, ws)) {
      this.rejectSocket(ws, 1008, 'Duplicate peer ID');
      return;
    }
    this.peerManager.networkDigestRegistry.set(ws.domainName, ws.networkDigestHex);
    // Session caches were evicted. A new ID forces clients to send their full routes.
    ws.serverSessionId = randomU64String();
    ws.routeResyncPending = true;
    this.saveSocketMeta(ws);
  }
}
