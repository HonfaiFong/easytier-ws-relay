import { Buffer } from 'node:buffer';
import { parseHeader } from './core/packet.js';
import { PacketType, HEADER_SIZE, MY_PEER_ID, MAGIC, VERSION } from './core/constants.js';
import { loadProtos } from './core/protos.js';
import { handleHandshake, handlePing, handleForwarding } from './core/basic_handlers.js';
import { handleRpcReq, handleRpcResp } from './core/rpc_handler.js';
import { PeerManager } from './core/peer_manager.js';
import { randomU64String } from './core/crypto.js';
import { authorizeRelay, positiveLimit } from './core/security.js';
import { OutboundError, RateLimitError, SocketClosedError } from './core/errors.js';

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
    this.peerManager.sendToSocket = (ws, bytes) => this.sendToSocket(ws, bytes, { control: true });
    this.maxConnections = positiveLimit(env, 'EASYTIER_MAX_CONNECTIONS', 128, this.peerManager.maxRoutePeers);
    this.maxFrameBytes = positiveLimit(env, 'EASYTIER_MAX_FRAME_BYTES', 1024 * 1024);
    this.maxMessagesPerSecond = positiveLimit(env, 'EASYTIER_MAX_MESSAGES_PER_SECOND', 1000, 10000);
    this.maxBytesPerSecond = positiveLimit(env, 'EASYTIER_MAX_BYTES_PER_SECOND', 8 * 1024 * 1024, 32 * 1024 * 1024);
    this.maxOutboundFrameBytes = positiveLimit(env, 'EASYTIER_MAX_OUTBOUND_FRAME_BYTES', 1024 * 1024);
    this.maxOutboundBytesPerSecond = positiveLimit(env, 'EASYTIER_MAX_OUTBOUND_BYTES_PER_SECOND', 8 * 1024 * 1024);
    this.maxRoomOutboundBytesPerSecond = positiveLimit(env, 'EASYTIER_MAX_ROOM_OUTBOUND_BYTES_PER_SECOND', 32 * 1024 * 1024);
    this.maxOutboundBufferedBytes = positiveLimit(env, 'EASYTIER_MAX_OUTBOUND_BUFFERED_BYTES', 4 * 1024 * 1024);
    this.handshakeResponseBody = this.types.HandshakeRequest.encode({
      magic: MAGIC, myPeerId: MY_PEER_ID, version: VERSION,
      features: ['node-server-v1'],
      networkName: this.peerManager.getEnv('EASYTIER_PUBLIC_SERVER_NETWORK_NAME', 'public_server'),
      networkSecretDigest: new Uint8Array(32),
    }).finish();
    const minimumControlBytes = HEADER_SIZE + this.handshakeResponseBody.length;
    for (const [name, capacity] of [
      ['EASYTIER_MAX_OUTBOUND_FRAME_BYTES', this.maxOutboundFrameBytes],
      ['EASYTIER_MAX_OUTBOUND_BYTES_PER_SECOND', this.maxOutboundBytesPerSecond],
      ['EASYTIER_MAX_ROOM_OUTBOUND_BYTES_PER_SECOND', this.maxRoomOutboundBytesPerSecond],
      ['EASYTIER_MAX_OUTBOUND_BUFFERED_BYTES', this.maxOutboundBufferedBytes],
    ]) {
      if (capacity < minimumControlBytes) throw new Error(`${name} must allow a ${minimumControlBytes}-byte handshake response`);
    }
    // Reserved inside the total budget, so data cannot exhaust local control traffic.
    this.controlReserveBytes = Math.min(64 * 1024, Math.floor(this.maxOutboundBytesPerSecond / 8));
    this.roomControlReserveBytes = Math.min(256 * 1024, Math.floor(this.maxRoomOutboundBytesPerSecond / 8));
    this.pendingControls = new Map();
    this.pendingControlBytes = 0;
    this.controlRetryTimer = null;
    this.maxRpcResponseBytes = positiveLimit(env, 'EASYTIER_MAX_RPC_RESPONSE_BYTES', 256 * 1024);
    this.maxGlobalMapRequestsPerSecond = positiveLimit(env, 'EASYTIER_MAX_GLOBAL_MAP_REQUESTS_PER_SECOND', 4);
    this.maxRoomGlobalMapRequestsPerSecond = positiveLimit(env, 'EASYTIER_MAX_ROOM_GLOBAL_MAP_REQUESTS_PER_SECOND', 32);
    this.globalMapWindowStart = Date.now();
    this.globalMapWindowCount = 0;
    this.maxPongPayloadBytes = positiveLimit(env, 'EASYTIER_MAX_PONG_PAYLOAD_BYTES', 4096);
    this.maxPongsPerSecond = positiveLimit(env, 'EASYTIER_MAX_PONGS_PER_SECOND', 4);
    this.maxRoomPongsPerSecond = positiveLimit(env, 'EASYTIER_MAX_ROOM_PONGS_PER_SECOND', 256);
    this.maxPongBytesPerSecond = positiveLimit(env, 'EASYTIER_MAX_PONG_BYTES_PER_SECOND', 16 * 1024);
    this.maxRoomPongBytesPerSecond = positiveLimit(env, 'EASYTIER_MAX_ROOM_PONG_BYTES_PER_SECOND', 64 * 1024);
    this.pongWindowStart = Date.now();
    this.pongWindowCount = 0;
    this.pongWindowBytes = 0;
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
      if (ws.routeResyncPending && !this.peerManager.pendingRouteUpdates.has(ws)) {
        this.peerManager.pushRouteUpdateTo(ws.peerId, ws, this.types, { forceFull: true });
      }
      const payload = buffer.subarray(HEADER_SIZE);
      if (header.packetType === PacketType.Ping && forServer) handlePing(ws, header, payload, this);
      else if (header.packetType === PacketType.RpcReq && forServer) handleRpcReq(ws, header, payload, this.types, this);
      else if (header.packetType === PacketType.RpcResp && forServer) handleRpcResp(ws, header, payload, this.types, this);
      else handleForwarding(ws, header, buffer, this.types, this);
    } catch (e) {
      if (e instanceof OutboundError) return;
      console.warn('Packet rejected:', e.message);
      if (!ws.rejected) this.rejectSocket(ws, 1002, 'Invalid packet');
    }
  }

  sendToSocket(ws, bytes, { control = false, fromQueue = false } = {}) {
    if (ws.readyState !== 1 || ws.rejected) throw new SocketClosedError('Socket closed');
    if (bytes.length > this.maxOutboundFrameBytes) {
      throw new OutboundError('Outbound frame too large');
    }
    const now = Date.now();
    const peerLimit = this.maxOutboundBytesPerSecond - (control ? 0 : this.controlReserveBytes);
    const roomLimit = this.maxRoomOutboundBytesPerSecond - (control ? 0 : this.roomControlReserveBytes);
    // A frame that can never fit is not a transient failure; never retry it forever.
    if (bytes.length > Math.min(peerLimit, roomLimit, this.maxOutboundBufferedBytes)) {
      throw new OutboundError('Outbound frame exceeds delivery capacity');
    }
    if (control && !fromQueue && this.pendingControls.has(ws)) {
      throw new RateLimitError('Control delivery pending', now + 1000);
    }
    if (typeof ws.bufferedAmount === 'number' && ws.bufferedAmount < this.maxOutboundBufferedBytes) {
      ws.slowConsumerSince = null;
    }
    if (typeof ws.bufferedAmount === 'number' &&
        ws.bufferedAmount + bytes.length > this.maxOutboundBufferedBytes) {
      if (ws.bufferedAmount >= this.maxOutboundBufferedBytes) {
        ws.slowConsumerSince ??= now;
        if (now - ws.slowConsumerSince >= 1000) {
          this.rejectSocket(ws, 1008, 'Slow consumer');
          throw new SocketClosedError('Slow consumer');
        }
      }
      throw new RateLimitError('Outbound buffer full', now + 1000);
    }
    if (now - this.outboundWindowStart >= 1000) {
      this.outboundWindowStart = now;
      this.outboundWindowBytes = 0;
    }
    if (now - ws.outboundWindowStart >= 1000) {
      ws.outboundWindowStart = now;
      ws.outboundWindowBytes = 0;
    }
    if (ws.outboundWindowBytes + bytes.length > peerLimit) {
      throw new RateLimitError('Outbound byte rate exceeded', ws.outboundWindowStart + 1000);
    }
    if (this.outboundWindowBytes + bytes.length > roomLimit) {
      throw new RateLimitError('Room outbound byte rate exceeded', this.outboundWindowStart + 1000);
    }
    try { ws.send(bytes); }
    catch (_) {
      this.rejectSocket(ws, 1011, 'WebSocket send failed');
      throw new SocketClosedError('WebSocket send failed');
    }
    ws.outboundWindowBytes += bytes.length;
    this.outboundWindowBytes += bytes.length;
  }

  sendControlToSocket(ws, bytes) {
    try {
      this.sendToSocket(ws, bytes, { control: true });
      return;
    } catch (e) {
      if (!(e instanceof RateLimitError)) throw e;
      const queue = this.pendingControls.get(ws) || { frames: [], bytes: 0 };
      // Bound both retained bytes and tiny-frame amplification. Clients can retry
      // RPCs when this queue is full; that is still a resource failure, not 1002.
      if (queue.frames.length >= 32 || queue.bytes + bytes.length > 1024 * 1024 ||
          this.pendingControlBytes + bytes.length > 4 * 1024 * 1024) throw e;
      queue.frames.push(Buffer.from(bytes));
      queue.bytes += bytes.length;
      this.pendingControlBytes += bytes.length;
      this.pendingControls.set(ws, queue);
      this.scheduleControlRetry();
    }
  }

  scheduleControlRetry() {
    if (!this.controlRetryTimer) {
      this.controlRetryTimer = setTimeout(() => this.flushControlQueue(), 1000);
    }
  }

  flushControlQueue() {
    if (this.controlRetryTimer) clearTimeout(this.controlRetryTimer);
    this.controlRetryTimer = null;
    const blocked = new Set();
    let progressed;
    do {
      progressed = false;
      // One frame per socket per round. Rotate served sockets behind waiters so
      // a busy RPC client cannot monopolize each recovered room window.
      for (const [ws, queue] of Array.from(this.pendingControls)) {
        if (blocked.has(ws)) continue;
        const frame = queue.frames[0];
        try { this.sendToSocket(ws, frame, { control: true, fromQueue: true }); }
        catch (e) {
          if (e instanceof RateLimitError) blocked.add(ws);
          else this.dropControlQueue(ws);
          continue;
        }
        queue.frames.shift();
        queue.bytes -= frame.length;
        this.pendingControlBytes -= frame.length;
        progressed = true;
        this.pendingControls.delete(ws);
        if (queue.frames.length) this.pendingControls.set(ws, queue);
      }
    } while (progressed);
    if (this.pendingControls.size) this.scheduleControlRetry();
  }

  dropControlQueue(ws) {
    const queue = this.pendingControls.get(ws);
    if (queue) this.pendingControlBytes -= queue.bytes;
    this.pendingControls.delete(ws);
    if (!this.pendingControls.size && this.controlRetryTimer) {
      clearTimeout(this.controlRetryTimer);
      this.controlRetryTimer = null;
    }
  }

  checkExpensiveRpc(ws) {
    const now = Date.now();
    if (now - ws.expensiveRpcWindowStart >= 1000) {
      ws.expensiveRpcWindowStart = now;
      ws.expensiveRpcWindowCount = 0;
    }
    if (++ws.expensiveRpcWindowCount > this.maxGlobalMapRequestsPerSecond) {
      this.rejectSocket(ws, 1008, 'Global map request rate exceeded');
      throw new RateLimitError('Global map request rate exceeded', ws.expensiveRpcWindowStart + 1000);
    }
    if (now - this.globalMapWindowStart >= 1000) {
      this.globalMapWindowStart = now;
      this.globalMapWindowCount = 0;
    }
    // Shared saturation is not evidence of an abusive individual client.
    if (this.globalMapWindowCount >= this.maxRoomGlobalMapRequestsPerSecond) {
      throw new RateLimitError('Room global map request rate exceeded', this.globalMapWindowStart + 1000);
    }
    this.globalMapWindowCount++;
  }

  allowPong(ws, payloadBytes) {
    if (payloadBytes > this.maxPongPayloadBytes) return false;
    const now = Date.now();
    if (now - ws.pongWindowStart >= 1000) {
      ws.pongWindowStart = now;
      ws.pongWindowCount = 0;
      ws.pongWindowBytes = 0;
    }
    if (now - this.pongWindowStart >= 1000) {
      this.pongWindowStart = now;
      this.pongWindowCount = 0;
      this.pongWindowBytes = 0;
    }
    if (ws.pongWindowCount >= this.maxPongsPerSecond ||
        this.pongWindowCount >= this.maxRoomPongsPerSecond ||
        ws.pongWindowBytes + HEADER_SIZE + payloadBytes > this.maxPongBytesPerSecond ||
        this.pongWindowBytes + HEADER_SIZE + payloadBytes > this.maxRoomPongBytesPerSecond) return false;
    ws.pongWindowCount++;
    this.pongWindowCount++;
    ws.pongWindowBytes += HEADER_SIZE + payloadBytes;
    this.pongWindowBytes += HEADER_SIZE + payloadBytes;
    return true;
  }

  rejectSocket(ws, code, reason) {
    ws.rejected = true;
    ws.handshakeComplete = false;
    this.saveSocketMeta(ws);
    this.webSocketClose(ws);
    try { ws.close(code, reason); } catch (_) { }
  }

  async webSocketClose(ws) {
    this.dropControlQueue(ws);
    this.peerManager.dropRouteRetry(ws);
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
    ws.slowConsumerSince = null;
    ws.expensiveRpcWindowStart = ws.lastSeen;
    ws.expensiveRpcWindowCount = 0;
    ws.pongWindowStart = ws.lastSeen;
    ws.pongWindowCount = 0;
    ws.pongWindowBytes = 0;
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
