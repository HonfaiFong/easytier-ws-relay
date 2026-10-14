// Use an isolated wrangler instance with 8192 bytes/socket/s and 16384 bytes/room/s.
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import WebSocket from 'ws';
import { loadProtos } from '../src/worker/core/protos.js';
import { createHeader, parseHeader } from '../src/worker/core/packet.js';
import { HEADER_SIZE, MAGIC, MY_PEER_ID, PacketType } from '../src/worker/core/constants.js';

const types = loadProtos();
const base = process.argv[2] || 'ws://127.0.0.1:18784/ws';
const sockets = [];
const roomName = `egress-${Date.now()}`;
function frame(id, to, type, body = Buffer.alloc(0)) {
  return Buffer.concat([createHeader(id, to, type, body.length), Buffer.from(body)]);
}
function routeContains(data, peerId) {
  if (parseHeader(data).packetType !== PacketType.RpcReq || parseHeader(data).fromPeerId !== MY_PEER_ID) return false;
  const rpc = types.RpcPacket.decode(data.subarray(HEADER_SIZE));
  const route = types.SyncRouteInfoRequest.decode(types.RpcRequest.decode(rpc.body).request);
  return route.connBitmap?.peerIds.some(peer => peer.peerId === peerId);
}
async function connect(id) {
  const url = new URL(base);
  url.searchParams.set('room', roomName);
  const ws = new WebSocket(url);
  sockets.push(ws);
  const pending = [], waiters = [], received = [];
  ws.on('message', bytes => {
    received.push(bytes);
    const index = waiters.findIndex(item => item.predicate(bytes));
    if (index < 0) pending.push(bytes);
    else waiters.splice(index, 1)[0].resolve(bytes);
  });
  const read = predicate => {
    const index = pending.findIndex(predicate);
    if (index >= 0) return Promise.resolve(pending.splice(index, 1)[0]);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`Response timed out for ${id}`)), 5000);
      waiters.push({ predicate, resolve: bytes => { clearTimeout(timer); resolve(bytes); } });
    });
  };
  await once(ws, 'open', { signal: AbortSignal.timeout(5000) });
  ws.send(frame(id, MY_PEER_ID, PacketType.HandShake, types.HandshakeRequest.encode({
    magic: MAGIC, myPeerId: id, networkName: 'egress-smoke', networkSecretDigest: Buffer.alloc(32),
  }).finish()));
  await read(bytes => parseHeader(bytes).packetType === PacketType.HandShake);
  await read(bytes => routeContains(bytes, id));
  return { id, ws, read, received };
}

try {
  const a = await connect(123), b = await connect(456), c = await connect(789);
  await a.read(bytes => routeContains(bytes, 789));
  // Start in a recovered window, then generate converging traffic below each ingress cap.
  await delay(1100);
  const first = frame(a.id, c.id, PacketType.Data, Buffer.alloc(6000, 1));
  a.ws.send(first);
  assert.deepEqual(await c.read(bytes => parseHeader(bytes).packetType === PacketType.Data), first);
  b.ws.send(frame(b.id, c.id, PacketType.Data, Buffer.alloc(6000, 2)));
  // A local Pong would exceed C's total socket budget, so it must wait instead of closing C.
  c.ws.send(frame(c.id, MY_PEER_ID, PacketType.Ping, Buffer.alloc(3000, 3)));
  const rpc = types.RpcPacket.encode({
    fromPeer: c.id, toPeer: MY_PEER_ID, transactionId: 77,
    descriptor: { protoName: 'peer_rpc', serviceName: 'PeerCenterRpc', methodIndex: 1 },
    body: types.RpcRequest.encode({ request: types.GetGlobalPeerMapRequest.encode({}).finish() }).finish(),
    isRequest: true, totalPieces: 1, pieceIdx: 0, compressionInfo: { algo: 1, acceptedAlgo: 1 },
  }).finish();
  c.ws.send(frame(c.id, MY_PEER_ID, PacketType.RpcReq, rpc));
  const d = await connect(901);
  const pong = await c.read(bytes => parseHeader(bytes).packetType === PacketType.Pong);
  assert.equal(pong.length, HEADER_SIZE + 3000);
  const response = await c.read(bytes => parseHeader(bytes).packetType === PacketType.RpcResp);
  assert.equal(types.RpcPacket.decode(response.subarray(HEADER_SIZE)).transactionId.toString(), '77');
  await c.read(bytes => routeContains(bytes, d.id));
  assert.equal(c.received.filter(bytes => parseHeader(bytes).packetType === PacketType.Data).length, 1);
  for (const peer of [a, b, c, d]) assert.equal(peer.ws.readyState, WebSocket.OPEN);
  // Strict plaintext validation must reject the sender without touching the recipient.
  const closed = once(b.ws, 'close', { signal: AbortSignal.timeout(5000) });
  b.ws.send(frame(b.id, c.id, PacketType.RpcReq, types.RpcPacket.encode({ toPeer: c.id }).finish()));
  assert.equal((await closed)[0], 1002);
  assert.equal(c.ws.readyState, WebSocket.OPEN);
  console.log('PASS: workerd converging traffic drops, receiver survival, deferred Pong/RPC, eventual topology sync, strict plaintext identity');
} finally {
  for (const ws of sockets) ws.terminate();
}
