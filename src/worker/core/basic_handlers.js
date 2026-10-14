import { Buffer } from 'node:buffer';
import { MAGIC, VERSION, MY_PEER_ID, PacketType, HEADER_SIZE } from './constants.js';
import { createHeader } from './packet.js';
import { wrapPacket } from './crypto.js';

const WS_OPEN = 1;
const P2P_SIGNAL_TYPES = new Set([PacketType.RpcReq, PacketType.RpcResp, PacketType.Ping, PacketType.Pong]);

export function handleHandshake(ws, header, payload, types, room) {
  try {
    const req = types.HandshakeRequest.decode(payload);
    const pm = room.peerManager;
    const clientNetworkName = req.networkName || '';
    const clientDigest = Buffer.from(req.networkSecretDigest || []);
    if (ws.handshakeComplete || req.magic !== MAGIC || !req.myPeerId ||
        req.myPeerId === MY_PEER_ID || req.myPeerId !== header.fromPeerId ||
        clientNetworkName.length > 128 || ![0, 32].includes(clientDigest.length)) {
      room.rejectSocket(ws, 1008, 'Invalid handshake');
      return;
    }
    const digestHex = clientDigest.toString('hex');
    if (pm.networkDigestRegistry.has(clientNetworkName) &&
        pm.networkDigestRegistry.get(clientNetworkName) !== digestHex) {
      room.rejectSocket(ws, 1008, 'Network digest mismatch');
      return;
    }
    // A digest partitions networks; it does not prove possession of a network secret.
    const groupKey = `${clientNetworkName}:${digestHex}`;
    if (pm.getPeerWs(req.myPeerId, groupKey)?.readyState === WS_OPEN) {
      room.rejectSocket(ws, 1008, 'Duplicate peer ID');
      return;
    }
    if (!pm._getPeerInfosMap(groupKey, false)?.has(req.myPeerId) &&
        (pm._getPeerInfosMap(groupKey, false)?.size || 0) >= pm.maxRoutePeers) {
      room.rejectSocket(ws, 1008, 'Route peer limit exceeded');
      return;
    }
    ws.domainName = clientNetworkName;
    ws.networkDigestHex = digestHex;
    ws.groupKey = groupKey;
    ws.peerId = req.myPeerId;
    if (!pm.addPeer(req.myPeerId, ws)) {
      room.rejectSocket(ws, 1008, 'Duplicate peer ID');
      return;
    }
    pm.networkDigestRegistry.set(clientNetworkName, digestHex);
    pm.updatePeerInfo(groupKey, req.myPeerId, {
      peerId: req.myPeerId, version: 1,
      lastUpdate: { seconds: Math.floor(Date.now() / 1000), nanos: 0 },
      instId: { part1: 0, part2: 0, part3: 0, part4: 0 },
      networkLength: Number(pm.getEnv('EASYTIER_NETWORK_LENGTH', 24)),
    });
    ws.handshakeComplete = true;
    room.saveSocketMeta(ws);
    const respBuffer = types.HandshakeRequest.encode({
      magic: MAGIC, myPeerId: MY_PEER_ID, version: VERSION,
      features: ['node-server-v1'],
      networkName: pm.getEnv('EASYTIER_PUBLIC_SERVER_NETWORK_NAME', 'public_server'),
      networkSecretDigest: new Uint8Array(32),
    }).finish();
    room.sendToSocket(ws, Buffer.concat([createHeader(MY_PEER_ID, req.myPeerId, PacketType.HandShake, respBuffer.length), Buffer.from(respBuffer)]));
    pm.pushRouteUpdateTo(req.myPeerId, ws, types, { forceFull: true });
    pm.broadcastRouteUpdate(types, groupKey, req.myPeerId);
  } catch (e) {
    console.warn('Handshake rejected:', e.message);
    if (!ws.rejected) room.rejectSocket(ws, 1008, 'Invalid handshake');
  }
}

export function handlePing(ws, header, payload, room) {
  room.sendToSocket(ws, wrapPacket(createHeader, MY_PEER_ID, header.fromPeerId, PacketType.Pong, payload, ws));
}

export function handleForwarding(sourceWs, header, fullMessage, types, room) {
  const pm = room.peerManager;
  // Only control traffic passes in pure P2P mode, including no opaque foreign/KCP data.
  if (pm.isPureP2PMode() && !P2P_SIGNAL_TYPES.has(header.packetType)) return;
  if (!(header.flags & 0x11) &&
      (header.packetType === PacketType.RpcReq || header.packetType === PacketType.RpcResp)) {
    try {
      const rpc = types.RpcPacket.decode(fullMessage.subarray(HEADER_SIZE));
      if ((Object.hasOwn(rpc, 'fromPeer') && rpc.fromPeer !== header.fromPeerId) ||
          (Object.hasOwn(rpc, 'toPeer') && rpc.toPeer !== header.toPeerId)) {
        room.rejectSocket(sourceWs, 1002, 'Forwarded RPC identity mismatch');
        return;
      }
    } catch (_) {
      // Opaque signaling from older clients remains end-to-end data.
    }
  }
  const targetWs = pm.getPeerWs(header.toPeerId, sourceWs.groupKey);
  if (!targetWs || targetWs.readyState !== WS_OPEN || targetWs.groupKey !== sourceWs.groupKey) return;
  try {
    room.sendToSocket(targetWs, fullMessage);
  } catch (e) {
    console.warn('Forward failed:', e.message);
    if (e.message !== 'Room outbound byte rate exceeded' && !targetWs.rejected) {
      room.rejectSocket(targetWs, 1011, 'Forward failed');
    }
  }
}
