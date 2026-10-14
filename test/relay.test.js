import test from 'node:test';
import assert from 'node:assert/strict';
import Long from 'long';
import worker from '../src/worker.js';
import { RelayRoom } from '../src/worker/relay_room.js';
import { PeerManager } from '../src/worker/core/peer_manager.js';
import { loadProtos } from '../src/worker/core/protos.js';
import { createHeader, parseHeader } from '../src/worker/core/packet.js';
import { MAGIC, MY_PEER_ID, PacketType, HEADER_SIZE } from '../src/worker/core/constants.js';
import { RateLimitError, OutboundError, SocketClosedError } from '../src/worker/core/errors.js';

const types = loadProtos();
class Socket {
  readyState = 1;
  sent = [];
  attachment = null;
  serializeAttachment(meta) { this.attachment = structuredClone(meta); }
  deserializeAttachment() { return structuredClone(this.attachment); }
  send(bytes) {
    if (this.readyState !== 1) throw new Error('Socket closed');
    this.sent.push(Buffer.from(bytes));
  }
  close(code, reason) { this.closeCode = code; this.closeReason = reason; this.readyState = 2; }
}
function room(t, env = {}, sockets = []) {
  const state = { getWebSockets: () => sockets, acceptWebSocket: ws => sockets.push(ws) };
  const value = new RelayRoom(state, env);
  t.after(() => {
    value.peerManager.flushRouteUpdates();
    clearTimeout(value.peerManager.routeRetryTimer);
    clearTimeout(value.controlRetryTimer);
  });
  return value;
}
function packet(id, to, type, body = Buffer.alloc(0)) {
  return Buffer.concat([createHeader(id, to, type, body.length), Buffer.from(body)]);
}
async function handshake(r, id, options = {}) {
  const ws = new Socket();
  await r.handleSession(ws);
  await r.webSocketMessage(ws, packet(id, MY_PEER_ID, PacketType.HandShake,
    types.HandshakeRequest.encode({ magic: MAGIC, myPeerId: id, networkName: 'test', networkSecretDigest: Buffer.alloc(32), ...options }).finish()));
  return ws;
}
async function rpc(r, ws, service, method, type, value, overrides = {}, response = false) {
  const request = type.encode(value).finish();
  const body = response ? types.RpcResponse.encode({ response: request }).finish() : types.RpcRequest.encode({ request }).finish();
  const data = types.RpcPacket.encode({
    fromPeer: ws.peerId, toPeer: MY_PEER_ID, transactionId: Long.fromString('9007199254740993'),
    descriptor: { domainName: ws.domainName, serviceName: service, protoName: 'peer_rpc', methodIndex: method },
    body, isRequest: !response, totalPieces: 1, pieceIdx: 0,
    compressionInfo: { algo: 1, acceptedAlgo: 1 }, ...overrides,
  }).finish();
  await r.webSocketMessage(ws, packet(ws.peerId, MY_PEER_ID, response ? PacketType.RpcResp : PacketType.RpcReq, data));
}
function lastResponse(ws) {
  const data = ws.sent.findLast(b => parseHeader(b).packetType === PacketType.RpcResp);
  assert.ok(data, 'Expected an RPC response');
  const envelope = types.RpcPacket.decode(data.subarray(HEADER_SIZE));
  return { envelope, body: types.RpcResponse.decode(envelope.body).response };
}
function lastRoute(ws) {
  const data = ws.sent.findLast(b => parseHeader(b).packetType === PacketType.RpcReq);
  const envelope = types.RpcPacket.decode(data.subarray(HEADER_SIZE));
  return types.SyncRouteInfoRequest.decode(types.RpcRequest.decode(envelope.body).request);
}

test('handshake persists connection identity and uses room env', async t => {
  const r = room(t, { EASYTIER_PUBLIC_SERVER_NETWORK_NAME: 'configured', EASYTIER_NETWORK_LENGTH: '20' });
  const ws = await handshake(r, 123);
  assert.equal(ws.readyState, 1);
  assert.equal(ws.attachment.peerId, 123);
  assert.equal(ws.attachment.handshakeComplete, true);
  assert.equal(ws.attachment.groupKey, ws.groupKey);
  assert.equal(types.HandshakeRequest.decode(ws.sent[0].subarray(HEADER_SIZE)).networkName, 'configured');
  assert.equal(r.peerManager._getPeerInfosMap(ws.groupKey).get(123).networkLength, 20);
});

test('hibernation rebuilds peers, rotates sessions and resumes forwarding', async t => {
  const r = room(t);
  const a = await handshake(r, 123), b = await handshake(r, 456);
  r.peerManager.flushRouteUpdates();
  const previousSession = a.serverSessionId;
  // Create fresh JS sockets retaining only the runtime attachment and physical connection.
  const fresh = [a, b].map(old => Object.assign(new Socket(), { attachment: structuredClone(old.attachment) }));
  const restored = room(t, {}, fresh);
  const frame = packet(123, 456, PacketType.Data, Buffer.from('payload'));
  await restored.webSocketMessage(fresh[0], frame);
  assert.deepEqual(fresh[1].sent.at(-1), frame);
  assert.equal(restored.peerManager.getPeerWs(123, fresh[0].groupKey), fresh[0]);
  assert.notEqual(fresh[0].serverSessionId, previousSession);
  assert.equal(lastRoute(fresh[0]).peerInfos.items.length, 3);
  await restored.webSocketClose(fresh[0]);
  assert.equal(restored.peerManager.getPeerWs(123, fresh[0].groupKey), undefined);
});

test('restoring unhandshaken sockets requires reconnect', t => {
  const ws = new Socket();
  ws.attachment = { peerId: null, groupKey: null };
  room(t, {}, [ws]);
  assert.equal(ws.closeCode, 1008);
});

test('hibernation between upgrade and handshake preserves a fresh connection', async t => {
  const original = room(t), ws = new Socket();
  await original.handleSession(ws);
  const fresh = Object.assign(new Socket(), { attachment: structuredClone(ws.attachment) });
  const restored = room(t, {}, [fresh]);
  await restored.webSocketMessage(fresh, packet(123, MY_PEER_ID, PacketType.HandShake,
    types.HandshakeRequest.encode({ magic: MAGIC, myPeerId: 123, networkName: 'test', networkSecretDigest: Buffer.alloc(32) }).finish()));
  assert.equal(fresh.readyState, 1);
  assert.equal(fresh.attachment.handshakeComplete, true);
});

test('oversized individual route info is rejected', async t => {
  const r = room(t), ws = await handshake(r, 123);
  await rpc(r, ws, 'OspfRouteRpc', 1, types.SyncRouteInfoRequest,
    { myPeerId: 123, peerInfos: { items: [{ peerId: 123, version: 2, hostname: 'a'.repeat(5000) }] } });
  assert.equal(ws.closeCode, 1002);
});

test('aggregate route quota applies across network groups', async t => {
  const r = room(t, { EASYTIER_MAX_TOTAL_ROUTE_PEERS: '2' });
  const a = await handshake(r, 123), b = await handshake(r, 456, { networkName: 'other' });
  await rpc(r, a, 'OspfRouteRpc', 1, types.SyncRouteInfoRequest,
    { myPeerId: 123, peerInfos: { items: [{ peerId: 789, version: 1 }] } });
  assert.equal(a.closeCode, 1002);
  assert.ok(r.peerManager._getPeerInfosMap(b.groupKey).has(456));
});

test('byte rate is bounded independently of message count', async t => {
  const r = room(t, { EASYTIER_MAX_BYTES_PER_SECOND: '256' }), ws = await handshake(r, 123);
  await r.webSocketMessage(ws, packet(123, 456, PacketType.Data, Buffer.alloc(200)));
  assert.equal(ws.closeCode, 1008);
  assert.equal(ws.closeReason, 'Byte rate exceeded');
});

test('live duplicate peer is rejected without deleting the original', async t => {
  const r = room(t);
  const old = await handshake(r, 123), duplicate = await handshake(r, 123);
  assert.equal(duplicate.closeCode, 1008);
  await r.webSocketClose(duplicate);
  assert.equal(r.peerManager.getPeerWs(123, old.groupKey), old);
});

test('late close of old socket cannot remove a reconnect or its routes', async t => {
  const r = room(t);
  const old = await handshake(r, 123);
  old.readyState = 2;
  const next = await handshake(r, 123);
  assert.equal(next.readyState, 1);
  await r.webSocketClose(old);
  assert.equal(r.peerManager.getPeerWs(123, next.groupKey), next);
  assert.ok(r.peerManager._getPeerInfosMap(next.groupKey).has(123));
});

test('reconnect with a lower PeerInfo version reaches established route sessions', async t => {
  const r = room(t);
  const old = await handshake(r, 123), observer = await handshake(r, 456);
  await rpc(r, observer, 'OspfRouteRpc', 1, types.SyncRouteInfoRequest,
    { myPeerId: 456, mySessionId: 42, peerInfos: { items: [{ peerId: 456, version: 2 }] } });
  await rpc(r, old, 'OspfRouteRpc', 1, types.SyncRouteInfoRequest,
    { myPeerId: 123, peerInfos: { items: [{ peerId: 123, version: 50 }] } });
  r.peerManager.flushRouteUpdates();
  assert.equal(r.peerManager._getSession(old.groupKey, 456).peerInfoVerMap.get(123), 50);

  old.readyState = 2;
  const replacement = await handshake(r, 123);
  await r.webSocketClose(old);
  r.peerManager.flushRouteUpdates();
  assert.equal(replacement.readyState, 1);
  assert.equal(lastRoute(observer).peerInfos.items.find(info => info.peerId === 123)?.version, 1);
});

test('rooms isolate peers, digests, config and PeerCenter data', async t => {
  const a = room(t, { EASYTIER_DISABLE_RELAY: '1' }), b = room(t);
  const wa = await handshake(a, 123), wb = await handshake(b, 123, { networkSecretDigest: Buffer.alloc(32, 1) });
  assert.equal(wb.readyState, 1);
  assert.notEqual(a.peerManager, b.peerManager);
  assert.equal(a.peerManager.isPureP2PMode(), true);
  assert.equal(b.peerManager.isPureP2PMode(), false);
  await rpc(a, wa, 'PeerCenterRpc', 0, types.ReportPeersRequest, { myPeerId: 123, peerInfos: { directPeers: { 999: { latencyMs: 12 } } } });
  assert.equal(b.peerManager.peerCenterStateByGroup.size, 0);
});

test('network digest mismatch is rejected and registry is cleaned after last close', async t => {
  const r = room(t);
  const first = await handshake(r, 123, { networkSecretDigest: Buffer.alloc(0) });
  const bad = await handshake(r, 456);
  assert.equal(bad.closeCode, 1008);
  await r.webSocketClose(first);
  assert.equal(r.peerManager.networkDigestRegistry.size, 0);
  assert.equal((await handshake(r, 456)).readyState, 1);
});

test('different networks cannot forward to each other', async t => {
  const r = room(t);
  const a = await handshake(r, 123), b = await handshake(r, 456, { networkName: 'other' });
  const before = b.sent.length;
  await r.webSocketMessage(a, packet(123, 456, PacketType.Data));
  assert.equal(b.sent.length, before);
});

for (const [label, override] of [['magic', { magic: 0 }], ['identity', { myPeerId: 456 }], ['reserved ID', { myPeerId: MY_PEER_ID }], ['digest length', { networkSecretDigest: Buffer.alloc(3) }]]) {
  test(`reject invalid handshake ${label}`, async t => {
    const ws = await handshake(room(t), 123, override);
    assert.equal(ws.closeCode, 1008);
  });
}

test('repeated handshake cannot move a peer into another group', async t => {
  const r = room(t), ws = await handshake(r, 123);
  await r.webSocketMessage(ws, packet(123, MY_PEER_ID, PacketType.HandShake, types.HandshakeRequest.encode({ magic: MAGIC, myPeerId: 123, networkName: 'other' }).finish()));
  assert.equal(ws.closeCode, 1008);
  assert.equal(r.peerManager.peersByGroup.size, 0);
});

test('unhandshaken and forged-source packets are rejected', async t => {
  const r = room(t), raw = new Socket();
  await r.handleSession(raw);
  await r.webSocketMessage(raw, packet(123, MY_PEER_ID, PacketType.Ping));
  assert.equal(raw.closeCode, 1008);
  const ws = await handshake(r, 123);
  await r.webSocketMessage(ws, packet(999, MY_PEER_ID, PacketType.Ping));
  assert.equal(ws.closeCode, 1008);
});

test('short header, invalid type and wrong length are rejected', async t => {
  for (const frame of [Buffer.alloc(2), packet(123, 456, 255), Buffer.concat([packet(123, 456, PacketType.Data), Buffer.from('extra')])]) {
    const r = room(t), ws = await handshake(r, 123);
    await r.webSocketMessage(ws, frame);
    assert.equal(ws.closeCode, 1002);
  }
});

test('frames and message rates have enforced quotas', async t => {
  const r = room(t, { EASYTIER_MAX_FRAME_BYTES: '256', EASYTIER_MAX_MESSAGES_PER_SECOND: '2' });
  const big = await handshake(r, 123);
  await r.webSocketMessage(big, packet(123, 456, PacketType.Data, Buffer.alloc(256)));
  assert.equal(big.closeCode, 1009);
  const ws = await handshake(r, 456);
  await r.webSocketMessage(ws, packet(456, MY_PEER_ID, PacketType.Ping));
  await r.webSocketMessage(ws, packet(456, MY_PEER_ID, PacketType.Ping));
  assert.equal(ws.closeCode, 1008);
});

test('typed-array views use their exact byte range', async t => {
  const r = room(t), ws = await handshake(r, 123);
  const ping = packet(123, MY_PEER_ID, PacketType.Ping, Buffer.from('ping'));
  const padded = Buffer.concat([Buffer.from('before'), ping, Buffer.from('after')]);
  await r.webSocketMessage(ws, new DataView(padded.buffer, padded.byteOffset + 6, ping.length));
  assert.equal(parseHeader(ws.sent.at(-1)).packetType, PacketType.Pong);
  assert.deepEqual(ws.sent.at(-1).subarray(HEADER_SIZE), Buffer.from('ping'));
});

test('encrypted and compressed peer data is relayed with original lengths', async t => {
  const r = room(t), a = await handshake(r, 123), b = await handshake(r, 456);
  for (const flags of [1, 16, 17]) {
    const frame = packet(123, 456, PacketType.Data, Buffer.alloc(28));
    frame[9] = flags;
    frame.writeUInt32LE(flags === 1 ? 0 : 100, 12);
    await r.webSocketMessage(a, frame);
    assert.equal(a.readyState, 1);
    assert.deepEqual(b.sent.at(-1), frame);
  }
});

test('multiple senders exhaust a receiver budget without disconnecting it', async t => {
  const r = room(t, { EASYTIER_MAX_OUTBOUND_BYTES_PER_SECOND: '1024' });
  const source = await handshake(r, 123), other = await handshake(r, 789), target = await handshake(r, 456);
  target.outboundWindowBytes = 0;
  r.outboundWindowBytes = 0;
  const data = packet(123, 456, PacketType.Data, Buffer.alloc(600));
  await r.webSocketMessage(source, data);
  assert.deepEqual(target.sent.at(-1), data);
  const before = target.sent.length;
  const otherData = packet(789, 456, PacketType.Data, Buffer.alloc(600));
  await r.webSocketMessage(other, otherData);
  assert.equal(target.sent.length, before);
  for (const ws of [source, other, target]) assert.equal(ws.readyState, 1);
  target.outboundWindowStart -= 1001;
  await r.webSocketMessage(other, otherData);
  assert.deepEqual(target.sent.at(-1), otherData);
});

test('only a persistently full transport buffer closes a slow consumer', async t => {
  const r = room(t, { EASYTIER_MAX_OUTBOUND_BUFFERED_BYTES: '1024' });
  const source = await handshake(r, 123), target = await handshake(r, 456);
  target.bufferedAmount = 1024;
  const before = target.sent.length;
  await r.webSocketMessage(source, packet(123, 456, PacketType.Data, Buffer.alloc(10)));
  assert.equal(target.readyState, 1);
  assert.equal(target.sent.length, before);
  target.slowConsumerSince -= 1001;
  await r.webSocketMessage(source, packet(123, 456, PacketType.Data, Buffer.alloc(10)));
  assert.equal(target.closeReason, 'Slow consumer');
  assert.equal(target.sent.length, before);
});

test('room outbound budget drops excess forwarding without disconnecting peers', async t => {
  const r = room(t, { EASYTIER_MAX_ROOM_OUTBOUND_BYTES_PER_SECOND: '2048' });
  const source = await handshake(r, 123), target = await handshake(r, 456);
  r.outboundWindowBytes = 0;
  const frame = packet(123, 456, PacketType.Data, Buffer.alloc(700));
  const before = target.sent.length;
  for (let i = 0; i < 3; i++) await r.webSocketMessage(source, frame);
  assert.equal(target.sent.length, before + 2);
  assert.equal(target.readyState, 1);
});

test('pure P2P drops data, foreign and KCP traffic but keeps RPC signaling', async t => {
  const r = room(t, { EASYTIER_DISABLE_RELAY: '1' }), a = await handshake(r, 123), b = await handshake(r, 456);
  assert.equal(r.peerManager.ensureMyInfo().featureFlag.noRelayKcp, true);
  for (const type of [PacketType.Data, PacketType.ForeignNetworkPacket, PacketType.KcpSrc, PacketType.KcpDst]) {
    const before = b.sent.length;
    await r.webSocketMessage(a, packet(123, 456, type));
    assert.equal(b.sent.length, before);
  }
  for (const type of [PacketType.RpcReq, PacketType.RpcResp]) {
    const frame = packet(123, 456, type, types.RpcPacket.encode({
      fromPeer: 123, toPeer: 456, isRequest: type === PacketType.RpcReq, totalPieces: 1,
      descriptor: { serviceName: 'UdpHolePunchRpc', protoName: 'peer_rpc', methodIndex: 1 },
    }).finish());
    await r.webSocketMessage(a, frame);
    assert.deepEqual(b.sent.at(-1), frame);
  }
});

test('forwarded plaintext RPC must agree with the outer peer identities', async t => {
  const r = room(t), source = await handshake(r, 123), target = await handshake(r, 456);
  const before = target.sent.length;
  const body = types.RpcPacket.encode({
    fromPeer: 999, toPeer: 456, isRequest: true, totalPieces: 1,
    descriptor: { serviceName: 'PeerCenterRpc', methodIndex: 0 },
  }).finish();
  await r.webSocketMessage(source, packet(123, 456, PacketType.RpcReq, body));
  assert.equal(source.closeCode, 1002);
  assert.equal(target.sent.length, before);
});

test('RPC responses above 256 bytes stay uncompressed and retain int64 transactions', async t => {
  const r = room(t), ws = await handshake(r, 123);
  const directPeers = Object.fromEntries(Array.from({ length: 40 }, (_, i) => [i + 500, { latencyMs: i + 1 }]));
  for (const id of Object.keys(directPeers)) await handshake(r, Number(id));
  await rpc(r, ws, 'PeerCenterRpc', 0, types.ReportPeersRequest, { myPeerId: 123, peerInfos: { directPeers } });
  await rpc(r, ws, 'PeerCenterRpc', 1, types.GetGlobalPeerMapRequest, {});
  const { envelope, body } = lastResponse(ws);
  assert.ok(body.length > 256);
  assert.equal(envelope.compressionInfo.algo, 1);
  assert.equal(envelope.compressionInfo.acceptedAlgo, 1);
  assert.equal(envelope.transactionId.toString(), '9007199254740993');
  assert.equal(types.GetGlobalPeerMapResponse.decode(body).globalPeerMap[123].directPeers[500].latencyMs, 1);
});

test('global map requests have an independent rate limit', async t => {
  const r = room(t, { EASYTIER_MAX_GLOBAL_MAP_REQUESTS_PER_SECOND: '2' });
  const ws = await handshake(r, 123);
  for (let i = 0; i < 3; i++) {
    await rpc(r, ws, 'PeerCenterRpc', 1, types.GetGlobalPeerMapRequest, {});
  }
  assert.equal(ws.closeCode, 1008);
  assert.equal(ws.expensiveRpcWindowCount, 3);
});

test('global map responses are capped before transmission', async t => {
  const r = room(t, { EASYTIER_MAX_RPC_RESPONSE_BYTES: '256' });
  const ws = await handshake(r, 123);
  const directPeers = Object.fromEntries(Array.from({ length: 40 }, (_, i) => [i + 500, { latencyMs: i + 1 }]));
  for (const id of Object.keys(directPeers)) await handshake(r, Number(id));
  await rpc(r, ws, 'PeerCenterRpc', 0, types.ReportPeersRequest, { myPeerId: 123, peerInfos: { directPeers } });
  const before = ws.sent.length;
  await rpc(r, ws, 'PeerCenterRpc', 1, types.GetGlobalPeerMapRequest, {});
  assert.equal(ws.readyState, 1);
  assert.equal(ws.sent.length, before);
});

for (const response of [false, true]) {
  test(`compressed RPC ${response ? 'response' : 'request'} is explicitly rejected`, async t => {
    const r = room(t), ws = await handshake(r, 123);
    await rpc(r, ws, 'PeerCenterRpc', 1, types.GetGlobalPeerMapRequest, {}, { compressionInfo: { algo: 2, acceptedAlgo: 1 } }, response);
    assert.equal(ws.closeCode, 1003);
    assert.match(ws.closeReason, /compression unsupported/);
  });
}

test('RPC inner source, report identity and route identity cannot be forged', async t => {
  for (const mode of ['envelope', 'report', 'route']) {
    const r = room(t), ws = await handshake(r, 123);
    if (mode === 'route') await rpc(r, ws, 'OspfRouteRpc', 1, types.SyncRouteInfoRequest, { myPeerId: 999 });
    else await rpc(r, ws, 'PeerCenterRpc', 0, types.ReportPeersRequest, { myPeerId: mode === 'report' ? 999 : 123 }, mode === 'envelope' ? { fromPeer: 999 } : {});
    assert.equal(ws.closeCode, 1002);
  }
});

test('oversized route lists are rejected before adding any advertised routes', async t => {
  const r = room(t, { EASYTIER_MAX_ROUTE_PEERS: '2' }), ws = await handshake(r, 123);
  await rpc(r, ws, 'OspfRouteRpc', 1, types.SyncRouteInfoRequest, {
    myPeerId: 123, peerInfos: { items: [{ peerId: 201 }, { peerId: 202 }, { peerId: 203 }] },
  });
  assert.equal(ws.closeCode, 1002);
  assert.equal(r.peerManager.peerInfosByGroup.size, 0);
});

test('a peer cannot advertise an unconnected route or create a relay link to it', async t => {
  const r = room(t), ws = await handshake(r, 123);
  await rpc(r, ws, 'OspfRouteRpc', 1, types.SyncRouteInfoRequest,
    { myPeerId: 123, peerInfos: { items: [{ peerId: 999, version: 1 }] } });
  assert.equal(ws.closeCode, 1002);
  assert.equal(r.peerManager._getPeerInfosMap(ws.groupKey, false)?.has(999) ?? false, false);

  const live = await handshake(r, 456);
  r.peerManager.updatePeerInfo(live.groupKey, 999, { peerId: 999, version: 1 });
  r.peerManager.pushRouteUpdateTo(456, live, types, { forceFull: true });
  const route = lastRoute(live);
  assert.equal(route.peerInfos.items.some(info => info.peerId === 999), false);
  assert.equal(route.connBitmap.peerIds.some(info => info.peerId === 999), false);
  await rpc(r, live, 'PeerCenterRpc', 0, types.ReportPeersRequest,
    { myPeerId: 456, peerInfos: { directPeers: { 999: { latencyMs: 1 } } } });
  await rpc(r, live, 'PeerCenterRpc', 1, types.GetGlobalPeerMapRequest, {});
  assert.equal(types.GetGlobalPeerMapResponse.decode(lastResponse(live).body).globalPeerMap[999], undefined);
  assert.equal(types.GetGlobalPeerMapResponse.decode(lastResponse(live).body).globalPeerMap[456].directPeers[999], undefined);
});

test('v2.6.4 route identity fields survive decoding and rebroadcast', async t => {
  const r = room(t), source = await handshake(r, 123), target = await handshake(r, 456);
  const identity = Buffer.alloc(32, 7);
  await rpc(r, source, 'OspfRouteRpc', 1, types.SyncRouteInfoRequest, {
    myPeerId: 123,
    peerInfos: { items: [{ peerId: 123, version: 2, noiseStaticPubkey: identity,
      trustedCredentialPubkeys: [{ credential: { pubkey: identity, groups: ['team'] }, credentialHmac: Buffer.alloc(32, 8) }] }] },
    connPeerList: { peerConnInfos: [{ peerId: { peerId: 123, version: 2 }, connectedPeerIds: [456] }] },
  });
  r.peerManager.flushRouteUpdates();
  const info = lastRoute(target).peerInfos.items.find(item => item.peerId === 123);
  assert.deepEqual(Buffer.from(info.noiseStaticPubkey), identity);
  assert.deepEqual(Buffer.from(info.trustedCredentialPubkeys[0].credential.pubkey), identity);
  assert.equal(source.readyState, 1);
});

test('route session acknowledgements enable deltas and persist initiator metadata', async t => {
  const r = room(t), ws = await handshake(r, 123);
  const req = { myPeerId: 123, mySessionId: Long.fromString('42', true), isInitiator: true, peerInfos: { items: [{ peerId: 123, version: 2 }] } };
  await rpc(r, ws, 'OspfRouteRpc', 1, types.SyncRouteInfoRequest, req);
  await rpc(r, ws, 'OspfRouteRpc', 1, types.SyncRouteInfoRequest, req);
  assert.equal(ws.readyState, 1);
  assert.equal(ws.attachment.weAreInitiator, false);
  assert.equal(lastRoute(ws).peerInfos, null);
  assert.equal(lastRoute(ws).connBitmap, null);
});

test('unchanged foreign network routes retain their version', async t => {
  const r = room(t), ws = await handshake(r, 123);
  const initial = lastRoute(ws).foreignNetworkInfos.infos[0].value;
  r.peerManager.pushRouteUpdateTo(123, ws, types, { forceFull: true });
  const same = lastRoute(ws).foreignNetworkInfos.infos[0].value;
  assert.equal(same.version, initial.version);
  assert.equal(same.lastUpdate.seconds.toString(), initial.lastUpdate.seconds.toString());

  await handshake(r, 456);
  r.peerManager.flushRouteUpdates();
  const changed = lastRoute(ws).foreignNetworkInfos.infos[0].value;
  assert.equal(changed.version, initial.version + 1);
});

test('coalesced broadcasts include every peer when updates have different origins', async t => {
  const r = room(t), a = await handshake(r, 123), b = await handshake(r, 456);
  r.peerManager.flushRouteUpdates();
  const beforeA = a.sent.length, beforeB = b.sent.length;
  r.peerManager.broadcastRouteUpdate(types, a.groupKey, 123);
  r.peerManager.broadcastRouteUpdate(types, a.groupKey, 456);
  r.peerManager.flushRouteUpdates();
  assert.equal(a.sent.length, beforeA + 1);
  assert.equal(b.sent.length, beforeB + 1);
});

test('PeerCenter digest reflects membership changes before returning an unchanged response', async t => {
  const r = room(t), ws = await handshake(r, 123);
  await rpc(r, ws, 'PeerCenterRpc', 1, types.GetGlobalPeerMapRequest, {});
  const digest = types.GetGlobalPeerMapResponse.decode(lastResponse(ws).body).digest;
  await handshake(r, 456);
  await rpc(r, ws, 'PeerCenterRpc', 1, types.GetGlobalPeerMapRequest, { digest });
  const response = types.GetGlobalPeerMapResponse.decode(lastResponse(ws).body);
  assert.ok(response.globalPeerMap[456]);
  assert.notEqual(response.digest.toString(), digest.toString());
});

test('Worker checks admission before looking up a room and preserves health route', async () => {
  const env = { EASYTIER_RELAY_TOKEN: 'secret', RELAY_ROOM: {
    idFromName: name => name,
    get: id => ({ fetch: req => new Response(id + new URL(req.url).pathname) }),
  } };
  const req = url => new Request(url, { headers: { Upgrade: 'websocket' } });
  assert.equal((await worker.fetch(req('https://relay/ws'), env)).status, 401);
  assert.equal(await (await worker.fetch(req('https://relay/ws?token=secret&room=alpha'), env)).text(), 'alpha/ws');
  assert.equal(await (await worker.fetch(new Request('https://relay/healthz'), env)).text(), 'ok');
  assert.equal((await worker.fetch(new Request('https://relay/ws'), env)).status, 400);
  const bearer = new Request('https://relay/ws', { headers: { Upgrade: 'websocket', Authorization: 'Bearer secret' } });
  assert.equal((await worker.fetch(bearer, env)).status, 200);
});

test('Durable Object also checks token and connection cap before accepting', async t => {
  const r = room(t, { EASYTIER_RELAY_TOKEN: 'secret', EASYTIER_MAX_CONNECTIONS: '1' });
  await handshake(r, 123);
  const req = url => new Request(url, { headers: { Upgrade: 'websocket' } });
  assert.equal((await r.fetch(req('https://relay/ws'))).status, 401);
  assert.equal((await r.fetch(req('https://relay/ws?token=secret'))).status, 503);
});

test('room config never falls back to another process environment', () => {
  const old = process.env.EASYTIER_DISABLE_RELAY;
  process.env.EASYTIER_DISABLE_RELAY = '1';
  try { assert.equal(new PeerManager().isPureP2PMode(), false); }
  finally { if (old === undefined) delete process.env.EASYTIER_DISABLE_RELAY; else process.env.EASYTIER_DISABLE_RELAY = old; }
});

test('data cannot spend reserved local control capacity', async t => {
  const r = room(t), a = await handshake(r, 123), b = await handshake(r, 456);
  r.peerManager.flushRouteUpdates();
  r.outboundWindowBytes = r.maxRoomOutboundBytesPerSecond - r.roomControlReserveBytes;
  b.outboundWindowBytes = r.maxOutboundBytesPerSecond - r.controlReserveBytes;
  const before = b.sent.length;
  await r.webSocketMessage(a, packet(123, 456, PacketType.Data, Buffer.alloc(10)));
  assert.equal(b.sent.length, before);
  await r.webSocketMessage(b, packet(456, MY_PEER_ID, PacketType.Ping, Buffer.from('ping')));
  assert.equal(parseHeader(b.sent.at(-1)).packetType, PacketType.Pong);
  r.peerManager.pushRouteUpdateTo(456, b, types, { forceFull: true });
  assert.equal(lastRoute(b).myPeerId, MY_PEER_ID);
  assert.equal(r.pendingControls.size, 0);
  assert.equal(r.peerManager.pendingRouteUpdates.size, 0);
});

test('route retries send the latest topology without another network event', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: Date.now() });
  const r = room(t), observer = await handshake(r, 456);
  r.peerManager.flushRouteUpdates();
  const session = r.peerManager._getSession(observer.groupKey, 456);
  session.dstSessionId = '42';
  const oldVersions = new Map(session.peerInfoVerMap);
  const oldBitmapVersions = new Map(session.connBitmapVerMap);
  const oldSignature = session.lastConnBitmapSig;
  const oldForeignVersion = session.foreignNetVer;
  r.outboundWindowBytes = r.maxRoomOutboundBytesPerSecond;
  await handshake(r, 123);
  r.peerManager.flushRouteUpdates();
  assert.ok(r.peerManager.pendingRouteUpdates.has(observer));
  assert.deepEqual(session.peerInfoVerMap, oldVersions);
  assert.deepEqual(session.connBitmapVerMap, oldBitmapVersions);
  assert.equal(session.lastConnBitmapSig, oldSignature);
  assert.equal(session.foreignNetVer, oldForeignVersion);
  // Coalesce an additional change into the dirty intent instead of replaying a frame.
  await handshake(r, 789);
  r.peerManager.flushRouteUpdates();
  t.mock.timers.tick(1001);
  assert.deepEqual(lastRoute(observer).connBitmap.peerIds.map(p => p.peerId).sort((a, b) => a - b),
    [MY_PEER_ID, 123, 456, 789].sort((a, b) => a - b));
  assert.equal(session.peerInfoVerMap.get(123), 1);
  assert.equal(session.peerInfoVerMap.get(789), 1);
  assert.equal(r.peerManager.pendingRouteUpdates.size, 0);
});

test('hibernation resync stays pending through repeated throttling and eventually clears', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: Date.now() });
  const original = room(t), ws = await handshake(original, 123);
  original.peerManager.flushRouteUpdates();
  const fresh = Object.assign(new Socket(), { attachment: structuredClone(ws.attachment) });
  const r = room(t, {}, [fresh]);
  r.outboundWindowBytes = r.maxRoomOutboundBytesPerSecond;
  await r.webSocketMessage(fresh, packet(123, MY_PEER_ID, PacketType.Ping));
  assert.equal(fresh.routeResyncPending, true);
  assert.equal(fresh.sent.length, 0);
  assert.equal(fresh.readyState, 1);
  r.peerManager.retryRouteUpdates();
  assert.equal(fresh.routeResyncPending, true);
  t.mock.timers.tick(1001);
  t.mock.timers.tick(1001);
  assert.equal(fresh.routeResyncPending, false);
  assert.equal(lastRoute(fresh).peerInfos.items.length, 2);
  assert.equal(r.peerManager.pendingRouteUpdates.size, 0);
});

test('legal RPC at the room limit is queued and answered without a protocol close', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: Date.now() });
  const r = room(t), ws = await handshake(r, 123);
  r.peerManager.flushRouteUpdates();
  const before = ws.sent.length;
  r.outboundWindowBytes = r.maxRoomOutboundBytesPerSecond;
  await rpc(r, ws, 'PeerCenterRpc', 1, types.GetGlobalPeerMapRequest, {});
  assert.equal(ws.readyState, 1);
  assert.equal(ws.sent.length, before);
  assert.equal(r.pendingControls.size, 1);
  t.mock.timers.tick(1001);
  assert.equal(lastResponse(ws).envelope.transactionId.toString(), '9007199254740993');
  assert.ok(types.GetGlobalPeerMapResponse.decode(lastResponse(ws).body).globalPeerMap[123]);
  assert.equal(r.pendingControlBytes, 0);
  assert.equal(r.pendingControls.size, 0);
});

test('a throttled handshake stays before its route update', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: Date.now() });
  const r = room(t);
  r.outboundWindowBytes = r.maxRoomOutboundBytesPerSecond;
  const ws = await handshake(r, 123);
  assert.equal(ws.readyState, 1);
  assert.equal(ws.sent.length, 0);
  t.mock.timers.tick(1001);
  assert.equal(parseHeader(ws.sent[0]).packetType, PacketType.HandShake);
  assert.equal(lastRoute(ws).myPeerId, MY_PEER_ID);
});

test('closed sockets lose queued responses and dirty routes before reconnect', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: Date.now() });
  const r = room(t), old = await handshake(r, 123);
  r.outboundWindowBytes = r.maxRoomOutboundBytesPerSecond;
  await rpc(r, old, 'PeerCenterRpc', 1, types.GetGlobalPeerMapRequest, {});
  r.peerManager.pushRouteUpdateTo(123, old, types, { forceFull: true });
  const before = old.sent.length;
  old.readyState = 2;
  await r.webSocketClose(old);
  assert.equal(r.pendingControls.size, 0);
  assert.equal(r.peerManager.pendingRouteUpdates.size, 0);
  const next = await handshake(r, 123);
  await r.webSocketClose(old);
  t.mock.timers.tick(1001);
  assert.equal(old.sent.length, before);
  assert.equal(next.readyState, 1);
  assert.equal(r.peerManager.getPeerWs(123, next.groupKey), next);
  assert.equal(lastRoute(next).peerInfos.items.length, 2);
});

test('control queues are bounded and saturation does not turn RPC into invalid input', async t => {
  const r = room(t), ws = await handshake(r, 123);
  r.outboundWindowBytes = r.maxRoomOutboundBytesPerSecond;
  for (let i = 0; i < 32; i++) r.sendControlToSocket(ws, Buffer.alloc(16));
  assert.throws(() => r.sendControlToSocket(ws, Buffer.alloc(16)), RateLimitError);
  await rpc(r, ws, 'PeerCenterRpc', 1, types.GetGlobalPeerMapRequest, {});
  assert.equal(ws.readyState, 1);
  assert.equal(r.pendingControls.get(ws).frames.length, 32);
  assert.equal(r.pendingControlBytes, 512);
});

test('per-socket control byte cap and permanent frame limits never blame the receiver', async t => {
  const r = room(t), ws = await handshake(r, 123);
  r.outboundWindowBytes = r.maxRoomOutboundBytesPerSecond;
  r.sendControlToSocket(ws, Buffer.alloc(1024 * 1024));
  assert.throws(() => r.sendControlToSocket(ws, Buffer.alloc(1)), RateLimitError);
  assert.throws(() => r.sendToSocket(ws, Buffer.alloc(r.maxOutboundFrameBytes + 1)), OutboundError);
  assert.equal(ws.readyState, 1);
  assert.equal(r.pendingControlBytes, 1024 * 1024);
});

test('a transient full buffer recovers without disconnecting either peer', async t => {
  const r = room(t, { EASYTIER_MAX_OUTBOUND_BUFFERED_BYTES: '1024' });
  const a = await handshake(r, 123), b = await handshake(r, 456);
  b.bufferedAmount = 1024;
  await r.webSocketMessage(a, packet(123, 456, PacketType.Data));
  assert.equal(b.readyState, 1);
  b.bufferedAmount = 0;
  const data = packet(123, 456, PacketType.Data, Buffer.from('recovered'));
  await r.webSocketMessage(a, data);
  assert.deepEqual(b.sent.at(-1), data);
  assert.equal(b.slowConsumerSince, null);
});

test('socket send failure is attributed to the destination, not the forwarding peer', async t => {
  const r = room(t), a = await handshake(r, 123), b = await handshake(r, 456);
  b.send = () => { throw new Error('Transport failed'); };
  await r.webSocketMessage(a, packet(123, 456, PacketType.Data));
  assert.equal(a.readyState, 1);
  assert.equal(b.closeCode, 1011);
  assert.throws(() => r.sendToSocket(b, Buffer.alloc(1)), SocketClosedError);
});

test('queued control traffic rotates between clients across constrained room windows', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: Date.now() });
  const r = room(t), a = await handshake(r, 123), b = await handshake(r, 456);
  r.peerManager.flushRouteUpdates();
  // Only one of these control frames fits per window.
  r.maxRoomOutboundBytesPerSecond = 100;
  r.outboundWindowBytes = 100;
  for (let i = 0; i < 3; i++) r.sendControlToSocket(a, Buffer.alloc(60, 1));
  r.sendControlToSocket(b, Buffer.alloc(60, 2));
  const beforeA = a.sent.length, beforeB = b.sent.length;
  t.mock.timers.tick(1001);
  assert.equal(a.sent.length, beforeA + 1);
  assert.equal(b.sent.length, beforeB);
  t.mock.timers.tick(1001);
  assert.equal(b.sent.length, beforeB + 1);
  assert.equal(a.sent.length, beforeA + 1);
});

for (const type of [PacketType.RpcReq, PacketType.RpcResp]) {
  for (const missing of ['fromPeer', 'toPeer', 'both', 'malformed']) {
    test(`forwarded plaintext RPC ${type} rejects ${missing} identity fields`, async t => {
      const r = room(t), a = await handshake(r, 123), b = await handshake(r, 456);
      const fields = { fromPeer: 123, toPeer: 456, isRequest: type === PacketType.RpcReq, totalPieces: 1 };
      if (missing === 'fromPeer' || missing === 'both') delete fields.fromPeer;
      if (missing === 'toPeer' || missing === 'both') delete fields.toPeer;
      const body = missing === 'malformed' ? Buffer.from([0xff]) : types.RpcPacket.encode(fields).finish();
      const before = b.sent.length;
      await r.webSocketMessage(a, packet(123, 456, type, body));
      assert.equal(a.closeCode, 1002);
      assert.equal(b.sent.length, before);
      assert.equal(b.readyState, 1);
    });
  }
}

test('encrypted/compressed RPC envelopes remain opaque end-to-end signaling', async t => {
  const r = room(t, { EASYTIER_DISABLE_RELAY: '1' }), a = await handshake(r, 123), b = await handshake(r, 456);
  for (const type of [PacketType.RpcReq, PacketType.RpcResp]) {
    for (const flags of [1, 16, 17]) {
      const frame = packet(123, 456, type, Buffer.from('encoded signaling'));
      frame[9] = flags;
      await r.webSocketMessage(a, frame);
      assert.deepEqual(b.sent.at(-1), frame);
      assert.equal(a.readyState, 1);
    }
  }
});

test('mutual PeerCenter policy confirms, withdraws and expires directed link reports', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: Date.now() });
  const r = room(t, { EASYTIER_PEER_CENTER_REQUIRE_MUTUAL: '1', EASYTIER_PEER_CENTER_TTL_MS: '2000' });
  const a = await handshake(r, 123), b = await handshake(r, 456);
  const report = (ws, directPeers) => rpc(r, ws, 'PeerCenterRpc', 0, types.ReportPeersRequest,
    { myPeerId: ws.peerId, peerInfos: { directPeers } });
  const snapshot = async () => {
    a.expensiveRpcWindowCount = 0;
    await rpc(r, a, 'PeerCenterRpc', 1, types.GetGlobalPeerMapRequest, {});
    return types.GetGlobalPeerMapResponse.decode(lastResponse(a).body).globalPeerMap;
  };
  await report(a, { 456: { latencyMs: 10 } });
  assert.equal((await snapshot())[123].directPeers[456], undefined);
  await report(b, { 123: { latencyMs: 20 } });
  const confirmed = await snapshot();
  assert.equal(confirmed[123].directPeers[456].latencyMs, 10);
  assert.equal(confirmed[456].directPeers[123].latencyMs, 20);
  await report(b, {});
  assert.equal((await snapshot())[123].directPeers[456], undefined);
  await report(b, { 123: { latencyMs: 20 } });
  t.mock.timers.tick(2001);
  assert.equal((await snapshot())[123].directPeers[456], undefined);
});

test('PeerCenter reconnect cannot resurrect a link from the old connection generation', async t => {
  const r = room(t, { EASYTIER_PEER_CENTER_REQUIRE_MUTUAL: '1' });
  const a = await handshake(r, 123), b = await handshake(r, 456);
  for (const [ws, target] of [[a, 456], [b, 123]]) {
    await rpc(r, ws, 'PeerCenterRpc', 0, types.ReportPeersRequest,
      { myPeerId: ws.peerId, peerInfos: { directPeers: { [target]: { latencyMs: 10 } } } });
  }
  b.readyState = 2;
  const replacement = await handshake(r, 456);
  await r.webSocketClose(b);
  await rpc(r, replacement, 'PeerCenterRpc', 0, types.ReportPeersRequest,
    { myPeerId: 456, peerInfos: { directPeers: { 123: { latencyMs: 20 } } } });
  await rpc(r, a, 'PeerCenterRpc', 1, types.GetGlobalPeerMapRequest, {});
  const map = types.GetGlobalPeerMapResponse.decode(lastResponse(a).body).globalPeerMap;
  assert.equal(map[123].directPeers[456], undefined);
  assert.equal(map[456].directPeers[123], undefined);
});
