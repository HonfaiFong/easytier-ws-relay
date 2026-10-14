import { Buffer } from 'buffer';
import { MY_PEER_ID, PacketType } from './constants.js';
import { createHeader } from './packet.js';
import { wrapPacket, randomU64String, sha256 } from './crypto.js';
import { positiveLimit } from './security.js';
import { RateLimitError } from './errors.js';
import { RPC_METHODS } from './rpc_methods.js';

const WS_OPEN = 1; // WebSocket.OPEN in CF runtime

function parseIpv4ToU32Be(ip) {
  const parts = String(ip).trim().split('.').map(x => Number(x));
  if (parts.length !== 4 || parts.some(x => !Number.isInteger(x) || x < 0 || x > 255)) {
    throw new Error(`Invalid IPv4: ${ip}`);
  }
  return ((parts[0] << 24) >>> 0) + (parts[1] << 16) + (parts[2] << 8) + parts[3];
}

function mask32FromLen(len) {
  const l = Number(len);
  if (!Number.isFinite(l) || l <= 0) return 0;
  if (l >= 32) return 0xFFFFFFFF >>> 0;
  return (0xFFFFFFFF << (32 - l)) >>> 0;
}

function deriveSameNetworkIpv4(peerAddr, networkLength, myPeerId) {
  const mask = mask32FromLen(networkLength);
  const net = (peerAddr >>> 0) & mask;
  const hostBits = 32 - Number(networkLength);
  if (!Number.isFinite(hostBits) || hostBits <= 1 || hostBits > 30) {
    return null;
  }
  const hostMax = (1 << hostBits) >>> 0;
  const peerHost = (peerAddr >>> 0) & (~mask >>> 0);
  let host = (Number(myPeerId) % 250) + 2;
  if (host >= hostMax) {
    host = (Number(myPeerId) % Math.max(hostMax - 2, 1)) + 1;
  }
  if (host === peerHost) {
    host = (host + 1) % hostMax;
    if (host === 0) host = 1;
  }
  return (net | host) >>> 0;
}

function makeInstId() {
  return {
    part1: Number(BigInt.asUintN(32, BigInt(randomU64String()))),
    part2: Number(BigInt.asUintN(32, BigInt(randomU64String()))),
    part3: Number(BigInt.asUintN(32, BigInt(randomU64String()))),
    part4: Number(BigInt.asUintN(32, BigInt(randomU64String()))),
  };
}

function makeStubPeerInfo(peerId, networkLength) {
  return {
    peerId,
    version: 1,
    lastUpdate: { seconds: Math.floor(Date.now() / 1000), nanos: 0 },
    instId: makeInstId(),
    cost: 1,
    hostname: "",
    easytierVersion: "cf-ws-relay",
    featureFlag: { isPublicServer: false, avoidRelayData: false, kcpInput: false, noRelayKcp: false },
    networkLength: Number(networkLength || 24),
    peerRouteId: randomU64String(),
    groups: [],
    udpNatType: 0,
  };
}

export class PeerManager {
  constructor(env = {}) {
    this.peersByGroup = new Map(); // groupKey -> Map(peerId -> ws)
    this.peerInfosByGroup = new Map(); // groupKey -> Map(peerId -> peerInfo)
    this.routeSessions = new Map(); // groupKey -> peerId -> session state
    this.peerConnVersions = new Map(); // groupKey -> peerId -> version
    this.types = null;
    this.runtimeEnv = env;
    this.networkDigestRegistry = new Map();
    this.peerCenterStateByGroup = new Map();
    this.lastPeerCenterClean = 0;
    this.maxRoutePeers = positiveLimit(env, 'EASYTIER_MAX_ROUTE_PEERS', 256, 1024);
    this.maxTotalRoutePeers = positiveLimit(env, 'EASYTIER_MAX_TOTAL_ROUTE_PEERS', 1024, 4096);
    this.maxPeerInfoBytes = positiveLimit(env, 'EASYTIER_MAX_PEER_INFO_BYTES', 4096, 16384);
    this.pendingRouteBroadcasts = new Map();
    this.routeBroadcastTimer = null;
    this.pendingRouteUpdates = new Map(); // physical socket -> latest route intent
    this.routeRetryTimer = null;
    this.routeBroadcastIntervalMs = positiveLimit(env, 'EASYTIER_ROUTE_BROADCAST_INTERVAL_MS', 100, 1000);

    this.allowVirtualIP = false;
    this.ipConfiguredByEnv = !!this.getEnv('EASYTIER_IPV4_ADDR');
    this.netConfiguredByEnv = this.getEnv('EASYTIER_NETWORK_LENGTH') !== undefined;
    this.ipAutoAssigned = false;
    this.myInfo = null; // lazily initialized to avoid random in global scope
    this.sessionTtlMs = positiveLimit(env, 'EASYTIER_SESSION_TTL_MS', 180_000, 3600_000);
    this.lastSessionCleanup = 0;

    this.pureP2PMode = (this.getEnv('EASYTIER_DISABLE_RELAY') === '1');
  }

  getEnv(name, fallback = undefined) {
    if (this.runtimeEnv && this.runtimeEnv[name] !== undefined && this.runtimeEnv[name] !== null) {
      return this.runtimeEnv[name];
    }
    return fallback;
  }

  applyRuntimeEnv(env = {}) {
    this.runtimeEnv = env || {};
    this.ipConfiguredByEnv = !!this.getEnv('EASYTIER_IPV4_ADDR');
    this.netConfiguredByEnv = this.getEnv('EASYTIER_NETWORK_LENGTH') !== undefined;
    this.sessionTtlMs = positiveLimit(env, 'EASYTIER_SESSION_TTL_MS', 180_000, 3600_000);
    this.setPureP2PMode(this.getEnv('EASYTIER_DISABLE_RELAY') === '1');
  }

  setTypes(types) {
    this.types = types;
  }

  ensureMyInfo() {
    if (this.myInfo) return this.myInfo;
    const myInfo = {
      peerId: MY_PEER_ID,
      instId: makeInstId(),
      cost: 1,
      version: 1,
      featureFlag: {
        isPublicServer: true,
        avoidRelayData: this.pureP2PMode,
        kcpInput: false,
        noRelayKcp: this.pureP2PMode
      },
      networkLength: Number(this.getEnv('EASYTIER_NETWORK_LENGTH', 24)),
      easytierVersion: this.getEnv('EASYTIER_VERSION', "cf-ws-relay"),
      lastUpdate: { seconds: Math.floor(Date.now() / 1000), nanos: 0 },
      hostname: this.getEnv('EASYTIER_HOSTNAME', "PublicServer_WorkerRelay"),
      udpNatType: 0,
      peerRouteId: randomU64String(),
      groups: [],
    };

    if (this.allowVirtualIP) {
      const ipEnv = this.getEnv('EASYTIER_IPV4_ADDR');
      if (ipEnv) {
        myInfo.ipv4Addr = { addr: parseIpv4ToU32Be(ipEnv) };
        this.ipAutoAssigned = false;
      } else if (this.getEnv('EASYTIER_AUTO_IPV4_ADDR') === '1') {
        const lastOctet = (Number(MY_PEER_ID) % 250) + 2;
        myInfo.ipv4Addr = { addr: parseIpv4ToU32Be(`10.0.0.${lastOctet}`) };
        this.ipAutoAssigned = true;
      }
    }

    this.myInfo = myInfo;
    return this.myInfo;
  }

  bumpMyInfoVersion() {
    const myInfo = this.ensureMyInfo();
    myInfo.version = (myInfo.version || 0) + 1;
    myInfo.lastUpdate = { seconds: Math.floor(Date.now() / 1000), nanos: 0 };
  }

  _getPeerConnVersionMap(groupKey, create = false) {
    const k = String(groupKey || '');
    let m = this.peerConnVersions.get(k);
    if (!m && create) {
      m = new Map();
      this.peerConnVersions.set(k, m);
    }
    return m;
  }

  bumpPeerConnVersion(groupKey, peerId) {
    const m = this._getPeerConnVersionMap(groupKey, true);
    const current = m.get(peerId) || 0;
    const next = current + 1;
    m.set(peerId, next);
    return next;
  }

  getPeerConnVersion(groupKey, peerId) {
    const m = this._getPeerConnVersionMap(groupKey, false);
    return m ? (m.get(peerId) || 0) : 0;
  }

  bumpAllPeerConnVersions(groupKey) {
    const allPeers = new Set(this.listPeerIdsInGroup(groupKey));
    const infos = this._getPeerInfosMap(groupKey, false);
    if (infos) {
      for (const pid of infos.keys()) {
        allPeers.add(pid);
      }
    }
    allPeers.add(MY_PEER_ID);
    for (const pid of allPeers) {
      this.bumpPeerConnVersion(groupKey, pid);
    }
  }

  setPublicServerFlag(isPublicServer) {
    const myInfo = this.ensureMyInfo();
    const next = !!isPublicServer;
    const prev = !!(myInfo.featureFlag && myInfo.featureFlag.isPublicServer);
    myInfo.featureFlag = {
      ...myInfo.featureFlag,
      isPublicServer: next,
    };
    if (next !== prev) {
      this.bumpMyInfoVersion();
    }
  }

  setPureP2PMode(enabled) {
    const next = !!enabled;
    if (next === this.pureP2PMode) return;
    this.pureP2PMode = next;
    const myInfo = this.myInfo;
    if (myInfo) {
      myInfo.featureFlag = {
        ...myInfo.featureFlag,
        avoidRelayData: this.pureP2PMode,
        noRelayKcp: this.pureP2PMode,
      };
      this.bumpMyInfoVersion();
    }
  }

  isPureP2PMode() {
    return !!this.pureP2PMode;
  }

  _getPeersMap(groupKey, create = false) {
    const k = String(groupKey || '');
    let m = this.peersByGroup.get(k);
    if (!m && create) {
      m = new Map();
      this.peersByGroup.set(k, m);
    }
    return m;
  }

  _getPeerInfosMap(groupKey, create = false) {
    const k = String(groupKey || '');
    let m = this.peerInfosByGroup.get(k);
    if (!m && create) {
      m = new Map();
      this.peerInfosByGroup.set(k, m);
    }
    return m;
  }

  _getSession(groupKey, peerId, create = false) {
    const now = Date.now();
    if (now - this.lastSessionCleanup > Math.max(30_000, Math.min(this.sessionTtlMs / 2, 120_000))) {
      this.cleanupSessions(now);
    }
    const gk = String(groupKey || '');
    let g = this.routeSessions.get(gk);
    if (!g && create) {
      g = new Map();
      this.routeSessions.set(gk, g);
    }
    if (!g) return null;
    let s = g.get(peerId);
    if (!s && create) {
      s = {
        mySessionId: null,
        dstSessionId: null,
        weAreInitiator: false,
        peerInfoVerMap: new Map(),
        connBitmapVerMap: new Map(),
        foreignNetVer: 0,
        lastForeignSignature: null,
        lastForeignUpdateSeconds: 0,
        lastTouch: Date.now(),
        lastConnBitmapSig: null,
      };
      g.set(peerId, s);
    }
    if (s) s.lastTouch = Date.now();
    return s;
  }

  cleanupSessions(nowTs = Date.now()) {
    this.lastSessionCleanup = nowTs;
    const ttl = this.sessionTtlMs;
    for (const [gk, m] of this.routeSessions.entries()) {
      for (const [pid, s] of m.entries()) {
        if (nowTs - (s.lastTouch || 0) > ttl) {
          m.delete(pid);
        }
      }
      if (m.size === 0) this.routeSessions.delete(gk);
    }
  }

  onRouteSessionAck(groupKey, peerId, theirSessionId, weAreInitiator) {
    theirSessionId = String(theirSessionId);
    const s = this._getSession(groupKey, peerId, true);
    if (s.dstSessionId !== theirSessionId) {
      s.peerInfoVerMap.clear();
      s.connBitmapVerMap.clear();
      s.foreignNetVer = 0;
      s.lastForeignSignature = null;
      s.lastForeignUpdateSeconds = 0;
      s.lastConnBitmapSig = null;
    }
    s.dstSessionId = theirSessionId;
    if (typeof weAreInitiator === 'boolean') {
      s.weAreInitiator = weAreInitiator;
    }
  }

  addPeer(peerId, ws) {
    const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
    const peers = this._getPeersMap(groupKey, true);
    const previous = peers.get(peerId);
    if (previous && previous !== ws && previous.readyState === WS_OPEN) return false;
    const isNewPeer = !peers.has(peerId);
    if (previous && previous !== ws) {
      this.dropRouteRetry(previous);
      this.peerCenterStateByGroup.get(groupKey)?.globalPeerMap.delete(String(peerId));
      for (const report of this.peerCenterStateByGroup.get(groupKey)?.globalPeerMap.values() || []) {
        delete report.directPeers[String(peerId)];
      }
      this.routeSessions.get(groupKey)?.delete(peerId);
      // The replacement starts a new route generation, even if it reuses the
      // same peer ID and reports a lower PeerInfo version.
      this._getPeerInfosMap(groupKey, false)?.delete(peerId);
      for (const session of this.routeSessions.get(groupKey)?.values() || []) {
        session.peerInfoVerMap.delete(peerId);
      }
    }
    peers.set(peerId, ws);
    const peerCenterState = this.peerCenterStateByGroup.get(groupKey);
    if (peerCenterState) peerCenterState.responseCache = null;
    if (isNewPeer || (previous && previous !== ws)) {
      this.bumpAllPeerConnVersions(groupKey);
    }
    return true;
  }

  removePeer(ws) {
    this.dropRouteRetry(ws);
    const peerId = ws && ws.peerId;
    const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
    if (!peerId) return false;
    const peers = this._getPeersMap(groupKey, false);
    if (peers?.get(peerId) !== ws) return false;
    peers.delete(peerId);
    const peerCenterState = this.peerCenterStateByGroup.get(groupKey);
    if (peerCenterState) peerCenterState.responseCache = null;
    this.peerCenterStateByGroup.get(groupKey)?.globalPeerMap.delete(String(peerId));
    for (const report of this.peerCenterStateByGroup.get(groupKey)?.globalPeerMap.values() || []) {
      delete report.directPeers[String(peerId)];
    }
    const infos = this._getPeerInfosMap(groupKey, false);
    if (infos) infos.delete(peerId);
    const sessions = this.routeSessions.get(groupKey);
    if (sessions) {
      sessions.delete(peerId);
      if (sessions.size === 0) this.routeSessions.delete(groupKey);
    }
    const connVers = this._getPeerConnVersionMap(groupKey, false);
    if (connVers) connVers.delete(peerId);

    if (peers.size > 0) {
      this.bumpAllPeerConnVersions(groupKey);
    }

    if (peers && peers.size === 0) {
      this.peersByGroup.delete(groupKey);
      this.peerInfosByGroup.delete(groupKey);
      this.peerConnVersions.delete(groupKey);
      this.peerCenterStateByGroup.delete(groupKey);
      this.networkDigestRegistry.delete(ws.domainName);
    }
    return true;
  }

  getPeerWs(peerId, groupKey) {
    const peers = this._getPeersMap(groupKey, false);
    return peers ? peers.get(peerId) : undefined;
  }

  listPeerIdsInGroup(groupKey) {
    const peers = this._getPeersMap(groupKey, false);
    return peers ? Array.from(peers.keys()) : [];
  }

  listPeersInGroup(groupKey) {
    const peers = this._getPeersMap(groupKey, false);
    return peers ? Array.from(peers.entries()) : [];
  }

  updatePeerInfo(groupKey, peerId, info) {
    if (peerId === MY_PEER_ID) return false;
    const infos = this._getPeerInfosMap(groupKey, true);
    const total = Array.from(this.peerInfosByGroup.values()).reduce((sum, m) => sum + m.size, 0);
    if (!infos.has(peerId) && (infos.size >= this.maxRoutePeers || total >= this.maxTotalRoutePeers)) {
      throw new Error('Route peer limit exceeded');
    }
    if (this.types && this.types.RoutePeerInfo.encode(info).finish().length > this.maxPeerInfoBytes) {
      throw new Error('Peer info size limit exceeded');
    }
    const previous = infos.get(peerId);
    const previousVersion = previous && previous.version ? Number(previous.version) : 0;
    const nextVersion = info && info.version ? Number(info.version) : 0;
    const isNew = !previous;
    const changed = isNew || nextVersion > previousVersion || JSON.stringify(previous || {}) !== JSON.stringify(info || {});

    infos.set(peerId, info);
    if (changed) {
      this.bumpAllPeerConnVersions(groupKey);
    }

    if (this.allowVirtualIP && !this.ipConfiguredByEnv && this.ipAutoAssigned) {
      const myInfo = this.ensureMyInfo();
      const peerIpv4 = info && info.ipv4Addr && typeof info.ipv4Addr.addr === 'number' ? (info.ipv4Addr.addr >>> 0) : null;
      const peerNetLen = info && (info.networkLength || info.network_length);
      const netLen = Number(peerNetLen || myInfo.networkLength || 24);
      if (peerIpv4 !== null && Number.isFinite(netLen) && netLen > 0) {
        const derived = deriveSameNetworkIpv4(peerIpv4, netLen, MY_PEER_ID);
        if (derived !== null) {
          let myInfoChanged = false;
          if (!this.netConfiguredByEnv) {
            if (myInfo.networkLength !== netLen) {
              myInfo.networkLength = netLen;
              myInfoChanged = true;
            }
          }
          const prevAddr = myInfo.ipv4Addr && typeof myInfo.ipv4Addr.addr === 'number'
            ? (myInfo.ipv4Addr.addr >>> 0)
            : null;
          if (prevAddr !== derived) {
            myInfo.ipv4Addr = { addr: derived };
            myInfoChanged = true;
          }

          if (myInfoChanged) {
            this.bumpMyInfoVersion();
            this.ipAutoAssigned = false;
          }
        }
      }
    }

    return changed;
  }

  broadcastRouteUpdate(types, groupKey, excludePeerId, opts = {}) {
    const pending = this.pendingRouteBroadcasts.get(groupKey);
    this.pendingRouteBroadcasts.set(groupKey, {
      types,
      excludePeerId: pending && pending.excludePeerId !== excludePeerId ? undefined : excludePeerId,
      forceFull: !!opts.forceFull || !!pending?.forceFull,
    });
    if (!this.routeBroadcastTimer) {
      this.routeBroadcastTimer = setTimeout(() => this.flushRouteUpdates(), this.routeBroadcastIntervalMs);
    }
  }

  flushRouteUpdates() {
    if (this.routeBroadcastTimer) clearTimeout(this.routeBroadcastTimer);
    this.routeBroadcastTimer = null;
    const pending = this.pendingRouteBroadcasts;
    this.pendingRouteBroadcasts = new Map();
    for (const [groupKey, update] of pending) {
      this._broadcastRouteUpdateNow(update.types, groupKey, update.excludePeerId, update);
    }
  }

  _broadcastRouteUpdateNow(types, groupKey, excludePeerId, opts = {}) {
    const forceFull = !!opts.forceFull;
    if (groupKey !== undefined) {
      const peers = this._getPeersMap(groupKey, false);
      if (!peers) return;
      for (const [peerId, ws] of peers.entries()) {
        if (peerId === excludePeerId) continue;
        if (ws.readyState === WS_OPEN) {
          this.pushRouteUpdateTo(peerId, ws, types, { forceFull });
        }
      }
      return;
    }
    for (const [gk, peers] of this.peersByGroup.entries()) {
      for (const [peerId, ws] of peers.entries()) {
        if (peerId === excludePeerId) continue;
        if (ws.readyState === WS_OPEN) {
          this.pushRouteUpdateTo(peerId, ws, types, { forceFull });
        }
      }
    }
  }

  dropRouteRetry(ws) {
    this.pendingRouteUpdates.delete(ws);
    if (!this.pendingRouteUpdates.size && this.routeRetryTimer) {
      clearTimeout(this.routeRetryTimer);
      this.routeRetryTimer = null;
    }
  }

  scheduleRouteRetry() {
    if (!this.routeRetryTimer) {
      this.routeRetryTimer = setTimeout(() => this.retryRouteUpdates(), 1000);
    }
  }

  retryRouteUpdates() {
    if (this.routeRetryTimer) clearTimeout(this.routeRetryTimer);
    this.routeRetryTimer = null;
    for (const [ws, update] of Array.from(this.pendingRouteUpdates)) {
      // Regenerate from current topology instead of retaining an obsolete frame.
      this.pushRouteUpdateTo(ws.peerId, ws, this.types, update);
    }
  }

  pushRouteUpdateTo(targetPeerId, ws, types, opts = {}) {
    const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
    if (!ws || ws.readyState !== WS_OPEN || this.getPeerWs(targetPeerId, groupKey) !== ws) {
      this.dropRouteRetry(ws);
      return false;
    }
    const forceFull = !!opts.forceFull || !!ws.routeResyncPending ||
      !!this.pendingRouteUpdates.get(ws)?.forceFull;
    const session = this._getSession(groupKey, targetPeerId, true);
    const myInfo = this.ensureMyInfo();
    if (!ws.serverSessionId) {
      ws.serverSessionId = randomU64String();
    }
    session.mySessionId = ws.serverSessionId;
    const forceFullLocal = forceFull || !session.dstSessionId;
    const nextPeerInfoVersions = new Map();

    // Only a live WebSocket is a route through this relay. Cached PeerInfo is
    // not evidence that the relay can forward to that peer.
    const allPeers = new Set(this.listPeersInGroup(groupKey)
      .filter(([, peerWs]) => peerWs.readyState === WS_OPEN)
      .map(([pid]) => pid));
    const relevantPeers = [MY_PEER_ID, ...Array.from(allPeers).filter(p => p !== MY_PEER_ID).sort((a, b) => Number(a) - Number(b))];
    const defaultNetLen = myInfo.networkLength || 24;

    const peerInfosItems = [];
    for (const pid of relevantPeers) {
      let info = (pid === MY_PEER_ID)
        ? myInfo
        : (this._getPeerInfosMap(groupKey, false)?.get(pid));
      if (!info) {
        info = makeStubPeerInfo(pid, defaultNetLen);
        this.updatePeerInfo(groupKey, pid, info);
      }
      const version = info && info.version ? info.version : 1;
      const prev = forceFullLocal ? 0 : (session.peerInfoVerMap.get(pid) || 0);
      if (forceFullLocal || version > prev) {
        peerInfosItems.push(info);
        nextPeerInfoVersions.set(pid, version);
      }
    }

    let connBitmap = null;
    let nextBitmapSig = session.lastConnBitmapSig;
    let nextBitmapVersion = null;
    if (relevantPeers.length > 0) {
      const connVersions = this._getPeerConnVersionMap(groupKey, true);
      const peerIdVersions = relevantPeers.map((pid) => {
        const existing = connVersions.get(pid) || 1;
        return { peerId: pid, version: existing };
      });
      const N = peerIdVersions.length;
      const bitmapSize = Math.ceil((N * N) / 8);
      const bitmap = new Uint8Array(bitmapSize);
      const idxByPeerId = new Map();
      for (let i = 0; i < peerIdVersions.length; i++) {
        idxByPeerId.set(peerIdVersions[i].peerId, i);
      }
      const setBit = (row, col) => {
        const idx = row * N + col;
        bitmap[Math.floor(idx / 8)] |= (1 << (idx % 8));
      };
      for (let i = 0; i < peerIdVersions.length; i++) {
        setBit(i, i);
      }
      const serverIdx = idxByPeerId.get(MY_PEER_ID);
      if (serverIdx !== undefined) {
        for (let i = 0; i < peerIdVersions.length; i++) {
          if (i === serverIdx) continue;
          if (allPeers.has(peerIdVersions[i].peerId)) {
            setBit(serverIdx, i);
            setBit(i, serverIdx);
          }
        }
      }
      const bitmapBuf = Buffer.from(bitmap);
      const sig = sha256().update(peerIdVersions.map(p => `${p.peerId}:${p.version}`).join(','))
        .update(bitmapBuf).digest('hex');
      const connVersion = session.connBitmapVerMap.get(targetPeerId) || 0;
      const nextConnVersion = connVersion || Math.max(...peerIdVersions.map(p => p.version));
      if (forceFullLocal || sig !== session.lastConnBitmapSig) {
        nextBitmapVersion = nextConnVersion;
        nextBitmapSig = sig;
        connBitmap = { peerIds: peerIdVersions, bitmap: bitmapBuf, version: nextConnVersion };
      }
    }

    let nextForeignState = null;
    const foreignNetworkInfos = (() => {
      const mode = String(this.getEnv('EASYTIER_HANDSHAKE_MODE', 'foreign')).toLowerCase();
      if (mode === 'same' || mode === 'same_network') return null;
      const networkName = this.getEnv('EASYTIER_PUBLIC_SERVER_NETWORK_NAME', 'dev-websocket-relay');
      const foreignPeerIds = Array.from(allPeers).sort((a, b) => Number(a) - Number(b));
      const signature = JSON.stringify([networkName, foreignPeerIds]);
      const changed = signature !== session.lastForeignSignature;
      if (!changed && !forceFullLocal) return null;
      const version = changed ? session.foreignNetVer + 1 : session.foreignNetVer;
      const updatedAt = changed ? Math.floor(Date.now() / 1000) : session.lastForeignUpdateSeconds;
      nextForeignState = { signature, version, updatedAt };
      return {
        infos: [{
          key: {
            peerId: MY_PEER_ID,
            networkName
          },
          value: {
            foreignPeerIds,
            lastUpdate: { seconds: updatedAt, nanos: 0 },
            version,
            networkSecretDigest: Buffer.alloc(32),
            myPeerIdForThisNetwork: MY_PEER_ID
          }
        }]
      };
    })();

    const t = this.types;
    if (!t) {
      throw new Error('PeerManager types not set');
    }
    const rawPeerInfos = peerInfosItems.length > 0
      ? peerInfosItems.map(info => t.RoutePeerInfo.encode(info).finish())
      : null;

    const reqPayload = {
      myPeerId: MY_PEER_ID,
      mySessionId: ws.serverSessionId,
      isInitiator: !!ws.weAreInitiator,
      peerInfos: peerInfosItems.length > 0 ? { items: peerInfosItems } : null,
      rawPeerInfos: rawPeerInfos,
      connBitmap: connBitmap,
      foreignNetworkInfos: foreignNetworkInfos
    };

    const reqBytes = t.SyncRouteInfoRequest.encode(reqPayload).finish();
    const rpcRequestPayload = { request: reqBytes, timeoutMs: 5000 };
    const rpcRequestBytes = t.RpcRequest.encode(rpcRequestPayload).finish();

    const rpcReqPacket = {
      fromPeer: MY_PEER_ID,
      toPeer: targetPeerId,
      transactionId: Number(BigInt.asUintN(32, BigInt(randomU64String()))),
      descriptor: {
        domainName: ws.domainName || "public_server",
        protoName: 'OspfRouteRpc',
        serviceName: 'OspfRouteRpc',
        methodIndex: RPC_METHODS.OspfRouteRpc.SyncRouteInfo
      },
      body: rpcRequestBytes,
      isRequest: true,
      totalPieces: 1,
      pieceIdx: 0,
      traceId: 0,
      compressionInfo: { algo: 1, acceptedAlgo: 1 }
    };

    const rpcPacketBytes = t.RpcPacket.encode(rpcReqPacket).finish();
    try {
      const frame = wrapPacket(createHeader, MY_PEER_ID, targetPeerId, PacketType.RpcReq, rpcPacketBytes, ws);
      if (this.sendToSocket) this.sendToSocket(ws, frame);
      else ws.send(frame);
      for (const [pid, version] of nextPeerInfoVersions) session.peerInfoVerMap.set(pid, version);
      session.lastConnBitmapSig = nextBitmapSig;
      if (nextBitmapVersion !== null) session.connBitmapVerMap.set(targetPeerId, nextBitmapVersion);
      if (nextForeignState) {
        session.foreignNetVer = nextForeignState.version;
        session.lastForeignSignature = nextForeignState.signature;
        session.lastForeignUpdateSeconds = nextForeignState.updatedAt;
      }
      ws.routeResyncPending = false;
      this.dropRouteRetry(ws);
      return true;
    } catch (e) {
      if (e instanceof RateLimitError) {
        this.pendingRouteUpdates.set(ws, { forceFull });
        this.scheduleRouteRetry();
      } else {
        this.dropRouteRetry(ws);
        console.warn(`Route delivery to ${targetPeerId} failed: ${e.message}`);
      }
      return false;
    }
  }
}
