import { Buffer } from 'node:buffer';
import { MY_PEER_ID, PacketType } from './constants.js';
import { createHeader } from './packet.js';
import { wrapPacket, randomU64String, sha256 } from './crypto.js';
import { positiveLimit } from './security.js';
import { OutboundError } from './errors.js';

// Helper to convert transactionId to proper format for protobuf int64
function toLongForProto(value) {
  if (value === null || value === undefined) return value;

  if (value && typeof value === 'object' && typeof value.low === 'number' && typeof value.high === 'number') {
    return value;
  }

  if (value && typeof value === 'object' && value.constructor && value.constructor.name === 'Long') {
    return value;
  }

  if (typeof value === 'string') {
    try {
      const big = BigInt(value);
      const low = Number(big & 0xffffffffn);
      const high = Number((big >> 32n) & 0xffffffffn);
      return { low, high, unsigned: false };
    } catch (e) {
      console.warn(`Failed to parse transactionId string as BigInt: ${value}`);
      return value;
    }
  }

  if (typeof value === 'number') {
    const low = value | 0;
    const high = Math.floor(value / 4294967296);
    return { low, high, unsigned: false };
  }

  if (typeof value === 'bigint') {
    const low = Number(value & 0xffffffffn);
    const high = Number((value >> 32n) & 0xffffffffn);
    return { low, high, unsigned: false };
  }

  return value;
}

function getPeerCenterState(pm, groupKey) {
  const k = String(groupKey || '');
  let s = pm.peerCenterStateByGroup.get(k);
  if (!s) {
    s = {
      globalPeerMap: new Map(),
      digest: '0',
    };
    pm.peerCenterStateByGroup.set(k, s);
  }
  const now = Date.now();
  s.lastTouch = now;
  if (now - pm.lastPeerCenterClean > 30_000) {
    cleanPeerCenterState(pm, now);
  }
  return s;
}

function cleanPeerCenterState(pm, now = Date.now()) {
  pm.lastPeerCenterClean = now;
  const ttl = positiveLimit(pm.runtimeEnv, 'EASYTIER_PEER_CENTER_TTL_MS', 180_000, 3600_000);
  for (const [gk, s] of pm.peerCenterStateByGroup.entries()) {
    for (const [pid, info] of s.globalPeerMap.entries()) {
      if (now - (info.lastSeen || 0) > ttl) {
        s.globalPeerMap.delete(pid);
      }
    }
    if (now - (s.lastTouch || 0) > ttl && s.globalPeerMap.size === 0) {
      pm.peerCenterStateByGroup.delete(gk);
    }
  }
}

function calcPeerCenterDigestFromMap(mapObj) {
  const h = sha256();
  const keys = Object.keys(mapObj).sort();
  for (const k of keys) {
    h.update(JSON.stringify(k));
    const directPeers = mapObj[k].directPeers || {};
    const dKeys = Object.keys(directPeers).sort();
    for (const dk of dKeys) {
      h.update(JSON.stringify(dk));
      const v = directPeers[dk];
      h.update(JSON.stringify(v?.latencyMs ?? 0));
    }
  }
  const b = h.digest();
  let x = 0n;
  for (let i = 0; i < 8; i++) {
    x = (x << 8n) | BigInt(b[i]);
  }
  const u64 = x & 0xFFFFFFFFFFFFFFFFn;
  return u64.toString();
}

function buildPeerCenterResponseMap(pm, groupKey, state) {
  const out = {};
  const set = new Set(pm.listPeersInGroup(groupKey)
    .filter(([, peerWs]) => peerWs.readyState === 1)
    .map(([pid]) => pid));
  const now = Date.now();
  const ttl = positiveLimit(pm.runtimeEnv, 'EASYTIER_PEER_CENTER_TTL_MS', 180_000, 3600_000);
  for (const peerId of set) {
    const key = String(peerId);
    const existing = state.globalPeerMap.get(key);
    const directPeers = {};
    const fresh = existing && now - existing.lastSeen <= ttl;
    for (const [reportedPeerId, link] of Object.entries(fresh ? existing.directPeers : {})) {
      // v2.6.4 reports directed links. Optional mutual confirmation is a relay
      // policy, rather than a change to the upstream default wire semantics.
      const reverseReport = state.globalPeerMap.get(reportedPeerId);
      const reciprocal = reverseReport && now - reverseReport.lastSeen <= ttl &&
        reverseReport.directPeers?.[key];
      if (set.has(Number(reportedPeerId)) &&
          (pm.getEnv('EASYTIER_PEER_CENTER_REQUIRE_MUTUAL') !== '1' || reciprocal)) {
        directPeers[reportedPeerId] = link;
      }
    }
    out[key] = { directPeers };
    out[key].directPeers[String(MY_PEER_ID)] = { latencyMs: 0 };
  }
  return out;
}

function sendRpcResponse(ws, toPeerId, reqRpcPacket, types, responseBodyBytes, room) {
  if (!ws || ws.readyState !== 1) {
    console.error(`sendRpcResponse aborted: socket not open (readyState=${ws ? ws.readyState : 'nil'}) toPeer=${toPeerId}`);
    return;
  }
  const responseBody = responseBodyBytes;
  if (responseBody.length > room.maxRpcResponseBytes) {
    throw new OutboundError('RPC response too large');
  }
  const compressionInfo = { algo: 1, acceptedAlgo: 1 };

  const rpcResponsePayload = {
    response: responseBody,
    error: null,
    runtimeUs: 0,
  };
  const rpcResponseBytes = types.RpcResponse.encode(rpcResponsePayload).finish();

  const txIdForEncoding = toLongForProto(reqRpcPacket.transactionId);

  const rpcRespPacket = {
    fromPeer: MY_PEER_ID,
    toPeer: toPeerId,
    transactionId: txIdForEncoding,
    descriptor: reqRpcPacket.descriptor,
    body: rpcResponseBytes,
    isRequest: false,
    totalPieces: 1,
    pieceIdx: 0,
    traceId: reqRpcPacket.traceId,
    compressionInfo,
  };
  const rpcPacketBytes = types.RpcPacket.encode(rpcRespPacket).finish();
  const buf = wrapPacket(createHeader, MY_PEER_ID, toPeerId, PacketType.RpcResp, rpcPacketBytes, ws);
  room.sendControlToSocket(ws, buf);
}

export function handleRpcReq(ws, header, payload, types, room) {
  try {
    const pm = room.peerManager;
    const rpcPacket = types.RpcPacket.decode(payload);
    if (!validateRpcPacket(ws, header, rpcPacket, true, room)) return;

    const descriptor = rpcPacket.descriptor;

    let innerReqBody = rpcPacket.body;
    try {
      const rpcReqWrapper = types.RpcRequest.decode(rpcPacket.body);
      if (rpcReqWrapper.request && rpcReqWrapper.request.length > 0) {
        innerReqBody = rpcReqWrapper.request;
      }
    } catch (e) {
      // Older clients may send a raw method body instead of an RpcRequest wrapper.
    }

    if ((descriptor.serviceName === 'peer_rpc.PeerCenterRpc' || descriptor.serviceName === 'PeerCenterRpc')
      && (descriptor.protoName === 'peer_rpc' || !descriptor.protoName)) {
      const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
      const state = getPeerCenterState(pm, groupKey);
      if (descriptor.methodIndex === 0) {
        const req = types.ReportPeersRequest.decode(innerReqBody);
        const myPeerId = req.myPeerId;
        if (myPeerId !== ws.peerId) throw new Error('PeerCenter identity mismatch');
        const peers = req.peerInfos || { directPeers: {} };
        if (Object.keys(peers.directPeers || {}).length > pm.maxRoutePeers) throw new Error('Direct peer limit exceeded');

        const directPeers = {};
        if (peers.directPeers) {
          for (const [dstPeerId, info] of Object.entries(peers.directPeers)) {
            if (Number(dstPeerId) === myPeerId) continue;
            if (pm.getPeerWs(Number(dstPeerId), groupKey)?.readyState !== 1) continue;
            directPeers[String(dstPeerId)] = { latencyMs: (info && typeof info.latencyMs === 'number') ? info.latencyMs : 0 };
          }
        }
        state.globalPeerMap.set(String(myPeerId), { directPeers, lastSeen: Date.now() });

        const snapshot = buildPeerCenterResponseMap(pm, groupKey, state);
        state.digest = calcPeerCenterDigestFromMap(snapshot);

        const respBytes = types.ReportPeersResponse.encode({}).finish();
        sendRpcResponse(ws, header.fromPeerId, rpcPacket, types, respBytes, room);
        return;
      }

      if (descriptor.methodIndex === 1) {
        room.checkExpensiveRpc(ws);
        const req = types.GetGlobalPeerMapRequest.decode(innerReqBody);
        const reqDigest = req.digest !== undefined && req.digest !== null ? String(req.digest) : '0';
        const snapshot = buildPeerCenterResponseMap(pm, groupKey, state);
        state.digest = calcPeerCenterDigestFromMap(snapshot);
        if (reqDigest === state.digest && reqDigest !== '0') {
          const respBytes = types.GetGlobalPeerMapResponse.encode({}).finish();
          sendRpcResponse(ws, header.fromPeerId, rpcPacket, types, respBytes, room);
          return;
        }

        const respBytes = types.GetGlobalPeerMapResponse.encode({
          globalPeerMap: snapshot,
          digest: state.digest,
        }).finish();
        sendRpcResponse(ws, header.fromPeerId, rpcPacket, types, respBytes, room);
        return;
      }

      return;
    }

    if ((descriptor.serviceName === 'peer_rpc.OspfRouteRpc' || descriptor.serviceName === 'OspfRouteRpc')
      && (descriptor.protoName === 'peer_rpc' || descriptor.protoName === 'peer_rpc.OspfRouteRpc' || descriptor.protoName === 'OspfRouteRpc' || !descriptor.protoName)) {
      const req = types.SyncRouteInfoRequest.decode(innerReqBody);
      const fromPeerId = header.fromPeerId;
      if (descriptor.methodIndex === 0 || descriptor.methodIndex === 1) {
        handleSyncRouteInfo(ws, fromPeerId, rpcPacket, req, types, room);
        return;
      }
      return;
    }

  } catch (e) {
    if (e instanceof OutboundError) return;
    console.warn('RPC rejected:', e.message);
    if (!ws.rejected) room.rejectSocket(ws, 1002, 'Invalid RPC request');
  }
}

export function handleRpcResp(ws, header, payload, types, room) {
  try {
    const pm = room.peerManager;
    const rpcPacket = types.RpcPacket.decode(payload);
    if (!validateRpcPacket(ws, header, rpcPacket, false, room)) return;

    const descriptor = rpcPacket.descriptor || {};
    let rpcRespBody = rpcPacket.body;
    let rpcResponseDecoded = null;
    try {
      rpcResponseDecoded = types.RpcResponse.decode(rpcRespBody);
      rpcRespBody = rpcResponseDecoded.response || rpcRespBody;
    } catch (e) {
      console.warn(`RpcResp wrapper decode failed from ${header.fromPeerId}: ${e.message}`);
    }

    if ((descriptor.serviceName === 'peer_rpc.OspfRouteRpc' || descriptor.serviceName === 'OspfRouteRpc')
      && (descriptor.protoName === 'peer_rpc' || descriptor.protoName === 'peer_rpc.OspfRouteRpc' || descriptor.protoName === 'OspfRouteRpc' || !descriptor.protoName)) {
      try {
        const resp = types.SyncRouteInfoResponse.decode(rpcRespBody);
        const sessionId = resp && resp.sessionId ? resp.sessionId : null;
        if (sessionId && ws && ws.groupKey !== undefined) {
          pm.onRouteSessionAck(ws.groupKey, header.fromPeerId, String(sessionId), ws.weAreInitiator);
        }
      } catch (e) {
        console.error(`Decode SyncRouteInfoResponse failed from ${header.fromPeerId}: ${e.message}`);
      }
      return;
    }

    if (rpcResponseDecoded) {
      if (rpcResponseDecoded.error) {
        console.warn(`RpcResp error from ${header.fromPeerId}:`, rpcResponseDecoded.error);
      }
    }
  } catch (e) {
    console.warn('RPC response rejected:', e.message);
    room.rejectSocket(ws, 1002, 'Invalid RPC response');
  }
}

function handleSyncRouteInfo(ws, fromPeerId, reqRpcPacket, syncReq, types, room) {
  const pm = room.peerManager;
  const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
  if (syncReq.myPeerId !== ws.peerId) throw new Error('Route identity mismatch');
  const items = syncReq.peerInfos?.items || [];
  const connPeerInfos = syncReq.connPeerList?.peerConnInfos || [];
  const known = pm._getPeerInfosMap(groupKey, false);
  const ids = new Set(known?.keys() || []);
  const existingCount = ids.size;
  for (const info of items) {
    if (!info.peerId) throw new Error('Invalid route peer');
    if (info.peerId !== ws.peerId) throw new Error('Unauthorized route peer');
    if (types.RoutePeerInfo.encode(info).finish().length > pm.maxPeerInfoBytes) throw new Error('Peer info size limit exceeded');
    ids.add(info.peerId);
  }
  if (items.length > pm.maxRoutePeers || ids.size > pm.maxRoutePeers ||
      (syncReq.connBitmap?.peerIds.length || 0) > pm.maxRoutePeers + 1 ||
      connPeerInfos.length > pm.maxRoutePeers + 1 ||
      connPeerInfos.some(info => info.connectedPeerIds.length > pm.maxRoutePeers) ||
      (syncReq.foreignNetworkInfos?.infos.length || 0) > pm.maxRoutePeers) {
    throw new Error('Route peer limit exceeded');
  }
  const total = Array.from(pm.peerInfosByGroup.values()).reduce((sum, m) => sum + m.size, 0);
  if (total + ids.size - existingCount > pm.maxTotalRoutePeers) throw new Error('Room route peer limit exceeded');

  if (!ws.serverSessionId) {
    ws.serverSessionId = randomU64String();
  }

  if (syncReq && typeof syncReq.isInitiator === 'boolean') {
    ws.weAreInitiator = !syncReq.isInitiator;
  }
  room.saveSocketMeta(ws);
  pm.onRouteSessionAck(groupKey, fromPeerId, String(syncReq.mySessionId), ws.weAreInitiator);

  let routeChanged = false;
  if (syncReq.peerInfos && syncReq.peerInfos.items) {
    syncReq.peerInfos.items.forEach(info => {
      if (pm.updatePeerInfo(groupKey, ws.peerId, info)) routeChanged = true;
    });
  }

  const respPayload = {
    isInitiator: !syncReq.isInitiator,
    sessionId: ws.serverSessionId
  };
  const respBytes = types.SyncRouteInfoResponse.encode(respPayload).finish();

  try {
    sendRpcResponse(ws, fromPeerId, reqRpcPacket, types, respBytes, room);
  } catch (e) {
    console.error(`CRITICAL: Failed to send SyncRouteInfoResponse to peer ${fromPeerId}: ${e.message}`);
  }

  try {
    pm.pushRouteUpdateTo(fromPeerId, ws, types);
  } catch (e) {
    console.error(`Failed to push route update to peer ${fromPeerId}:`, e);
  }

  if (routeChanged) {
    try {
      pm.broadcastRouteUpdate(types, groupKey, fromPeerId);
    } catch (e) {
      console.error(`Failed to broadcast route update for group ${groupKey}:`, e);
    }
  }
}

function validateRpcPacket(ws, header, packet, isRequest, room) {
  if (packet.fromPeer !== ws.peerId || packet.toPeer !== header.toPeerId ||
      packet.isRequest !== isRequest || !packet.descriptor ||
      packet.totalPieces > 1 || packet.pieceIdx !== 0) {
    room.rejectSocket(ws, 1002, 'Invalid RPC envelope');
    return false;
  }
  if (packet.compressionInfo && packet.compressionInfo.algo !== 1) {
    room.rejectSocket(ws, 1003, 'RPC compression unsupported; use None');
    return false;
  }
  return true;
}
