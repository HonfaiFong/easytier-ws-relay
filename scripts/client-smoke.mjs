import fs from 'node:fs';
import path from 'node:path';
import net from 'node:net';
import { once } from 'node:events';
import { spawn, execFileSync } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
if (!process.argv[2]) throw new Error('Usage: node scripts/client-smoke.mjs <EasyTier binary directory> [ws-url]');
const dir = path.resolve(process.argv[2]);
const base = process.argv[3] || 'ws://127.0.0.1:18783/ws';
fs.mkdirSync('tmp', { recursive: true });
const suffix = process.platform === 'win32' ? '.exe' : '';
const core = path.join(dir, `easytier-core${suffix}`), cli = path.join(dir, `easytier-cli${suffix}`);
const version = execFileSync(core, ['--version'], { encoding: 'utf8', windowsHide: true }).trim();
if (!version.startsWith('easytier-core 2.6.4-8428a89d')) throw new Error(`Unexpected client version: ${version}`);
console.log(version);
const children = [], connections = new Set();
const name = `real-data-${Date.now()}`;
const relayUrl = new URL(base);
relayUrl.searchParams.set('room', name);
const echo = net.createServer(socket => { connections.add(socket); socket.on('error', () => {}); socket.on('close', () => connections.delete(socket)); socket.pipe(socket); });
echo.listen(19895, '127.0.0.1');
await once(echo, 'listening');
const start = index => {
  const args = ['--no-tun', 'true', '--no-listener', '--disable-p2p', 'true', '--disable-encryption', 'true', '--secure-mode', 'false',
    '--disable-ipv6', 'true', '--disable-upnp', 'true', '--bind-device', 'false', '--network-name', name, '--network-secret', 'local-upstream-test',
    '--ipv4', `10.254.200.${index + 1}`, '--peers', relayUrl.toString(),
    '--hostname', `real-data-${index}`, '--instance-name', `real-data-${index}`, '--rpc-portal', `127.0.0.1:${19881 + index}`,
    '--console-log-level', 'warn', '--stun-servers', '--stun-servers-v6'];
  if (index === 0) args.push('--socks5', '19891');
  else args.push('--proxy-networks', '127.0.0.1/32->10.255.254.1/32');
  const log = fs.openSync(`tmp/real-data-${index}.log`, 'a');
  const child = spawn(core, args, { cwd: dir, windowsHide: true, stdio: ['ignore', log, log] });
  child.completed = once(child, 'exit');
  children.push(child); fs.closeSync(log); return child;
};
function reader(socket) {
  let buffer = Buffer.alloc(0), pending;
  socket.on('data', bytes => { buffer = Buffer.concat([buffer, bytes]); pending?.(); });
  return size => new Promise((resolve, reject) => {
    const timer = setTimeout(() => { pending = null; reject(new Error('SOCKS response timeout')); }, 8000);
    pending = () => { if (buffer.length >= size) { const result = buffer.subarray(0, size); buffer = buffer.subarray(size); pending = null; clearTimeout(timer); resolve(result); } };
    pending();
  });
}
async function roundTrip() {
  const socket = net.createConnection({ host: '127.0.0.1', port: 19891 });
  connections.add(socket); socket.on('error', () => {});
  try {
    await once(socket, 'connect'); const read = reader(socket);
    socket.write(Buffer.from([5, 1, 0]));
    if (!(await read(2)).equals(Buffer.from([5, 0]))) throw new Error('SOCKS auth failed');
    socket.write(Buffer.from([5, 1, 0, 1, 10, 255, 254, 1, 19895 >> 8, 19895 & 255]));
    const header = await read(4);
    if (header[1] !== 0) throw new Error(`SOCKS connect failed: ${header[1]}`);
    if (header[3] === 1) await read(6);
    else if (header[3] === 4) await read(18);
    else throw new Error('Unexpected SOCKS address');
    const message = Buffer.from('official v2.6.4 data through workerd relay\n'.repeat(100));
    socket.write(message);
    if (!(await read(message.length)).equals(message)) throw new Error('Echo data mismatch');
    console.log(`PASS: real TCP round trip, ${message.length} bytes through relay`);
  } finally { socket.destroy(); connections.delete(socket); }
}
try {
  start(0); let b = start(1);
  await delay(10000);
  for (const port of [19881, 19882]) {
    for (const cmd of ['peer', 'route', 'peer-center']) {
      try { const output = execFileSync(cli, ['-p', `127.0.0.1:${port}`, '-o', 'json', cmd], { encoding: 'utf8', windowsHide: true, timeout: 10000 }); fs.writeFileSync(`tmp/real-data-${port}-${cmd}.json`, output); console.log(`${port} ${cmd}: ${output.slice(0, 300)}`); }
      catch (e) { console.log(`${port} ${cmd}: ${e.message}`); }
    }
  }
  await roundTrip();
  b.kill(); await b.completed; b = start(1);
  await delay(7000);
  await roundTrip();
  console.log('PASS: official client reconnect and resumed TCP relay');
} finally {
  for (const child of children) if (child.exitCode === null && child.signalCode === null) child.kill();
  await Promise.all(children.map(child => child.completed));
  for (const socket of connections) socket.destroy();
  await new Promise(resolve => echo.close(resolve));
  for (const i of [0, 1]) console.log(`client ${i}: ${fs.readFileSync(`tmp/real-data-${i}.log`, 'utf8').slice(-2000)}`);
}
