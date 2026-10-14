// Run against an already-started `wrangler dev --ip 127.0.0.1 --port 18783`.
import assert from 'node:assert/strict';
import { once } from 'node:events';
import WebSocket from 'ws';
import { loadProtos } from '../src/worker/core/protos.js';
import { createHeader, parseHeader } from '../src/worker/core/packet.js';
import { HEADER_SIZE, MAGIC, MY_PEER_ID, PacketType } from '../src/worker/core/constants.js';

const types = loadProtos();
const base = process.argv[2] || 'ws://127.0.0.1:18783/ws';
const roomName = `smoke-${Date.now()}`;
const sockets = [];
function frame(id, to, type, body = Buffer.alloc(0)) {
  return Buffer.concat([createHeader(id, to, type, body.length), Buffer.from(body)]);
}
async function connect(id, group = roomName, accept = true) {
  const url = new URL(base);
  url.searchParams.set('room', group);
  const ws = new WebSocket(url);
  sockets.push(ws);
  const queue = [], waiters = [];
  ws.on('message', data => {
    const index = waiters.findIndex(w => w.predicate(data));
    if (index < 0) queue.push(data);
    else waiters.splice(index, 1)[0].resolve(data);
  });
  const read = predicate => {
    const index = queue.findIndex(predicate);
    if (index >= 0) return Promise.resolve(queue.splice(index, 1)[0]);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Runtime response timed out')), 5000);
      waiters.push({ predicate, resolve: data => { clearTimeout(timer); resolve(data); } });
    });
  };
  await once(ws, 'open', { signal: AbortSignal.timeout(5000) });
  const closed = accept ? null : once(ws, 'close', { signal: AbortSignal.timeout(5000) });
  ws.send(frame(id, MY_PEER_ID, PacketType.HandShake, types.HandshakeRequest.encode({
    magic: MAGIC, myPeerId: id, networkName: 'smoke', networkSecretDigest: Buffer.alloc(32),
  }).finish()));
  if (!accept) {
    const [code] = await closed;
    assert.equal(code, 1008);
    return { ws, read };
  }
  const handshake = await read(data => parseHeader(data).packetType === PacketType.HandShake);
  assert.equal(types.HandshakeRequest.decode(handshake.subarray(HEADER_SIZE)).myPeerId, MY_PEER_ID);
  await read(data => parseHeader(data).packetType === PacketType.RpcReq);
  return { ws, read };
}

try {
  const health = new URL(base);
  health.protocol = health.protocol === 'wss:' ? 'https:' : 'http:';
  health.pathname = '/healthz';
  health.search = '';
  assert.equal(await (await fetch(health)).text(), 'ok');
  const a = await connect(123), b = await connect(456);
  const data = frame(123, 456, PacketType.Data, Buffer.from('runtime payload'));
  a.ws.send(data);
  assert.deepEqual(await b.read(bytes => parseHeader(bytes).packetType === PacketType.Data), data);
  const signaling = frame(123, 456, PacketType.RpcReq, types.RpcPacket.encode({
    fromPeer: 123, toPeer: 456, transactionId: 42, isRequest: true,
    descriptor: { serviceName: 'UdpHolePunchRpc', protoName: 'UdpHolePunchRpc', methodIndex: 1 },
    body: types.RpcRequest.encode({ request: Buffer.alloc(0) }).finish(),
    totalPieces: 1, compressionInfo: { algo: 1, acceptedAlgo: 1 },
  }).finish());
  a.ws.send(signaling);
  assert.deepEqual(await b.read(bytes => parseHeader(bytes).packetType === PacketType.RpcReq && parseHeader(bytes).fromPeerId === 123), signaling);
  const ping = frame(123, MY_PEER_ID, PacketType.Ping, Buffer.from('ping'));
  a.ws.send(ping);
  assert.equal((await a.read(bytes => parseHeader(bytes).packetType === PacketType.Pong)).subarray(HEADER_SIZE).toString(), 'ping');
  await connect(123, roomName, false);
  await connect(123, `${roomName}-other`);
  const closed = once(a.ws, 'close', { signal: AbortSignal.timeout(5000) });
  a.ws.send(frame(999, MY_PEER_ID, PacketType.Ping));
  assert.equal((await closed)[0], 1008);
  console.log('PASS: workerd health, handshake, data/RPC forwarding, ping, duplicate rejection, room isolation, forged source rejection');
} finally {
  for (const ws of sockets) ws.terminate();
}
