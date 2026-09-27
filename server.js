#!/usr/bin/env node
/**
 * feiq-glm —— 局域网文件/文件夹互传（零依赖 Node.js）
 *
 * 运行：node server.js   （macOS / Windows 通用）
 * 环境变量：
 *   PORT        HTTP 端口（默认 3210，被占用自动顺延）
 *   UDP_PORT    发现协议起始 UDP 端口（默认 32101，共尝试 5 个）
 *   NAME        本机设备名
 *   SAVE_DIR    接收文件保存目录（默认 ~/Downloads/feiq-glm）
 *   CONFIG_PATH 配置文件路径（默认 ./config.json，同机多实例时需各自指定）
 *   NO_OPEN     设为 1 时不自动打开浏览器
 */
'use strict';

const http = require('http');
const net = require('net');
const os = require('os');
const fs = require('fs');
const path = require('path');
const dgram = require('dgram');
const crypto = require('crypto');
const { exec } = require('child_process');

// ---------------- 常量 ----------------
const VERSION = '1.2.0';
const HTTP_PORT_BASE = parseInt(process.env.PORT || '3210', 10);
const UDP_PORT_BASE = parseInt(process.env.UDP_PORT || '32101', 10);
const UDP_PORT_RANGE = 5;              // UDP 同时向 32101..32105 发送/监听
const ANNOUNCE_INTERVAL = 3000;        // 心跳周期
const PEER_TIMEOUT = 10000;            // 超过该时长无心跳视为离线
const OFFER_TIMEOUT = 60 * 1000;       // 等待接收方确认的超时
const UPLOAD_TIMEOUT = 10 * 60 * 1000; // 已接受但长时间无数据的超时
const FINISHED_KEEP = 100;             // 最多保留的已结束传输记录数
const SAVE_DIR_DEFAULT = path.join(os.homedir(), 'Downloads', 'feiq-glm');

const PLATFORM_NAME =
  process.platform === 'darwin' ? 'macOS' :
  process.platform === 'win32' ? 'Windows' :
  process.platform === 'linux' ? 'Linux' : process.platform;

// ---------------- 配置 ----------------
const CONFIG_PATH = process.env.CONFIG_PATH || path.join(__dirname, 'config.json');
const config = { id: null, name: null, saveDir: null };
try {
  Object.assign(config, JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8')));
} catch (_) { /* 首次运行无配置 */ }
if (!config.id) config.id = crypto.randomUUID();
if (!config.saveDir) config.saveDir = SAVE_DIR_DEFAULT;
if (process.env.SAVE_DIR) config.saveDir = process.env.SAVE_DIR;
if (!config.name) config.name = `${os.hostname().replace(/\.local\.?$/, '')} (${PLATFORM_NAME})`;
if (process.env.NAME) config.name = process.env.NAME;

/**
 * id 内嵌本机 IP（格式：<ip>-<uuid>）。
 * 项目文件夹拷贝到其他机器后，启动时检测到 id 中的 IP 与本机 IP 不一致，
 * 自动改写为本机 IP（保留 uuid 部分），从源头避免局域网 id 冲突。
 */
function buildNodeId() {
  return `${lanIps()[0] || '127.0.0.1'}-${crypto.randomUUID()}`;
}
function ensureNodeId() {
  const localIp = lanIps()[0] || '127.0.0.1';
  if (!config.id) {
    config.id = buildNodeId();
    return;
  }
  const m = /^(\d+\.\d+\.\d+\.\d+)-(.+)$/.exec(config.id);
  if (!m) {
    // 旧格式（纯 uuid）：升级为含 IP 的新格式，保留原 uuid
    config.id = `${localIp}-${config.id}`;
    console.log(`[配置] id 已升级为含本机 IP 的格式：${config.id.slice(0, 13)}…`);
  } else if (m[1] !== localIp) {
    console.log(`[配置] config.json 中 id 内嵌的 IP（${m[1]}）与本机 IP（${localIp}）不一致，已改写，避免拷贝导致的 id 冲突`);
    config.id = `${localIp}-${m[2]}`;
  }
}
ensureNodeId();

function saveConfig() {
  try {
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(
      { id: config.id, name: config.name, saveDir: config.saveDir }, null, 2));
  } catch (_) { /* 配置写失败不致命 */ }
}
saveConfig();

// ---------------- 状态 ----------------
const peers = new Map();    // id -> {id,name,os,host,httpPort,lastSeen,manual}
const offers = new Map();   // id -> 接收侧传输任务
const sseClients = new Set();

let httpPort = HTTP_PORT_BASE;
let udp = null;
let udpPort = null;

// ---------------- 小工具 ----------------
function lanIps() {
  const out = [];
  for (const list of Object.values(os.networkInterfaces())) {
    for (const ni of list) {
      if (ni.family === 'IPv4' && !ni.internal) out.push(ni.address);
    }
  }
  return out;
}

function localInfo() {
  return {
    id: config.id,
    name: config.name,
    os: PLATFORM_NAME,
    version: VERSION,
    saveDir: config.saveDir,
    httpPort,
    ips: lanIps(),
  };
}

function sendJson(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(body);
}

function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) {
        reject(new Error('请求体过大'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

async function readJson(req, limit = 8 * 1024 * 1024) {
  try {
    const buf = await readBody(req, limit);
    return JSON.parse(buf.toString('utf8') || '{}');
  } catch (_) {
    return null;
  }
}

/** 校验上传相对路径，防路径穿越；同时把对 Windows 非法的字符替换掉 */
function safeRelPath(input) {
  if (typeof input !== 'string' || !input.length || input.includes('\0')) return null;
  if (/^[a-zA-Z]:/.test(input)) return null;                 // 盘符
  if (input.startsWith('/') || input.startsWith('\\')) return null; // 绝对路径
  const parts = input.split(/[\\/]+/).filter((s) => s && s !== '.');
  if (!parts.length) return null;
  if (parts.some((s) => s === '..')) return null;            // 路径穿越，直接拒绝
  const clean = parts
    .map((s) => s.replace(/[\\/:*?"<>|\x00-\x1f]/g, '_').replace(/[. ]+$/, ''))
    .filter((s) => s.length);
  if (!clean.length) return null;
  return path.join(...clean);
}

/** 生成可用于文件名的设备名 */
function safeFsName(name) {
  let s = String(name || '').replace(/[\\/:*?"<>|\x00-\x1f]/g, '_').trim();
  s = s.replace(/[. ]+$/, '').slice(0, 60);
  return s || '未知设备';
}

function tsString(d = new Date()) {
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}-${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}`;
}

function uniqueDest(p) {
  if (!fs.existsSync(p)) return p;
  let i = 2;
  while (fs.existsSync(`${p} (${i})`)) i++;
  return `${p} (${i})`;
}

// ---------------- SSE ----------------
function sseWrite(res, event, data) {
  try {
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  } catch (_) { /* 客户端已断开 */ }
}

function publicPeer(p) {
  return {
    id: p.id,
    name: p.name,
    os: p.os,
    host: p.host,
    httpPort: p.httpPort,
    manual: !!p.manual,
    online: Date.now() - p.lastSeen < PEER_TIMEOUT,
  };
}

function publicOffer(o) {
  return {
    id: o.id,
    from: o.from,
    files: o.files,
    totalSize: o.totalSize,
    status: o.status,
    receivedBytes: o.receivedBytes,
    createdAt: o.createdAt,
    savedDir: o.savedDir,
  };
}

function broadcast(event, data) {
  for (const res of sseClients) sseWrite(res, event, data);
}

function broadcastUpdate(offer) {
  broadcast('offer-update', {
    id: offer.id,
    status: offer.status,
    receivedBytes: offer.receivedBytes,
    savedDir: offer.savedDir,
  });
}

// 节点列表变化先标脏，统一节流推送
let peersDirty = false;
function touchPeers() { peersDirty = true; }
setInterval(() => {
  if (!peersDirty) return;
  peersDirty = false;
  broadcast('peers', { peers: [...peers.values()].map(publicPeer) });
}, 400);

function handleSse(req, res) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.write('retry: 2000\n\n');
  sseClients.add(res);
  sseWrite(res, 'init', {
    me: localInfo(),
    peers: [...peers.values()].map(publicPeer),
    offers: [...offers.values()].map(publicOffer),
  });
  req.on('close', () => sseClients.delete(res));
}

// SSE 保活
setInterval(() => {
  for (const res of sseClients) {
    try { res.write(': ping\n\n'); } catch (_) {}
  }
}, 15000);

// ---------------- 节点发现（UDP 广播） ----------------
function udpPortList() {
  const arr = [];
  for (let i = 0; i < UDP_PORT_RANGE; i++) arr.push(UDP_PORT_BASE + i);
  return arr;
}

function ipToInt(ip) {
  return ip.split('.').reduce((acc, o) => ((acc << 8) + (parseInt(o, 10) || 0)) >>> 0, 0);
}
function intToIp(n) {
  return [(n >>> 24) & 255, (n >>> 16) & 255, (n >>> 8) & 255, n & 255].join('.');
}
function maskOf(prefix) {
  return prefix === 0 ? 0 : (0xFFFFFFFF << (32 - prefix)) >>> 0;
}
/** 按 CIDR 计算真实的子网定向广播地址（兼容 /28、/23 等非 /24 网段） */
function broadcastOf(ip, prefix) {
  const ipInt = ipToInt(ip);
  const mask = maskOf(prefix);
  const bc = (ipInt & mask) | (~mask >>> 0);
  return intToIp(bc);
}

function broadcastAddrs() {
  const out = new Set(['255.255.255.255']);
  for (const list of Object.values(os.networkInterfaces())) {
    for (const ni of list) {
      if (ni.family !== 'IPv4' || ni.internal) continue;
      if (ni.cidr) {
        const [ip, prefixStr] = ni.cidr.split('/');
        const prefix = parseInt(prefixStr, 10);
        if (prefix >= 0 && prefix <= 32) out.add(broadcastOf(ip, prefix));
      } else {
        const seg = ni.address.split('.');
        if (seg.length === 4) out.add(`${seg[0]}.${seg[1]}.${seg[2]}.255`);
      }
    }
  }
  return [...out];
}

/** targets 为 [host, port] 数组；为空则向所有广播地址 × 所有发现端口发送 */
function sendDiscovery(msg, targets) {
  if (!udp) return;
  const buf = Buffer.from(JSON.stringify(msg));
  const list = targets || [];
  if (!targets) {
    for (const addr of broadcastAddrs()) {
      for (const port of udpPortList()) list.push([addr, port]);
    }
  }
  for (const [host, port] of list) {
    udp.send(buf, 0, buf.length, port, host, () => {});
  }
}

function announceAll() {
  sendDiscovery({ v: 1, type: 'announce', id: config.id, name: config.name, os: PLATFORM_NAME, httpPort });
}

function onDiscoveryMessage(buf, rinfo) {
  let msg;
  try { msg = JSON.parse(buf.toString('utf8')); } catch (_) { return; }
  if (!msg || msg.v !== 1) return;
  if (msg.type === 'bye') {
    if (msg.id && peers.delete(msg.id)) touchPeers();
    return;
  }
  if (msg.type !== 'announce' || !msg.id) return;
  if (msg.id === config.id) {
    // 同 id 但来自其他 IP：配置文件被整份拷贝过，自愈
    if (!isMyIp(rinfo.address)) {
      regenerateId(`收到来自 ${rinfo.address} 的同 id announce`);
    }
    return;
  }

  const now = Date.now();
  const known = peers.get(msg.id);
  const p = known || { id: msg.id, _repliedAt: 0 };
  p.name = String(msg.name || '未知设备').slice(0, 60);
  p.os = String(msg.os || '');
  p.host = rinfo.address;
  p.httpPort = parseInt(msg.httpPort, 10) || 3210;
  p.lastSeen = now;
  p.manual = false;
  peers.set(msg.id, p);
  touchPeers();
  if (!known) console.log(`[发现] 新节点：${p.name} (${p.host}:${p.httpPort})`);

  // 新节点上线时单播回送一次自己的 announce，加速相互发现
  if (now - (p._repliedAt || 0) > 5000) {
    p._repliedAt = now;
    sendDiscovery({ v: 1, type: 'announce', id: config.id, name: config.name, os: PLATFORM_NAME, httpPort },
      [[rinfo.address, rinfo.port]]);
  }
}

function startUdp() {
  return new Promise((resolve) => {
    const tryPort = (port) => new Promise((ok) => {
      const sock = dgram.createSocket({ type: 'udp4', reuseAddr: true });
      let settled = false;
      const fail = () => {
        if (settled) return;
        settled = true;
        try { sock.close(); } catch (_) {}
        ok(false);
      };
      sock.once('error', fail);
      sock.once('listening', () => {
        if (settled) return;
        settled = true;
        try { sock.setBroadcast(true); } catch (_) {}
        sock.on('error', () => {}); // 运行期错误（如 ICMP 不可达）忽略
        sock.on('message', onDiscoveryMessage);
        udp = sock;
        udpPort = port;
        ok(true);
      });
      sock.bind(port);
    });

    (async () => {
      for (let i = 0; i < UDP_PORT_RANGE; i++) {
        if (await tryPort(UDP_PORT_BASE + i)) {
          console.log(`[发现] UDP 监听端口 ${udpPort}`);
          announceAll();
          setInterval(announceAll, ANNOUNCE_INTERVAL);
          resolve();
          return;
        }
      }
      console.error('[发现] UDP 端口绑定全部失败，自动发现不可用（仍可手动添加节点）');
      resolve();
    })();
  });
}

function sendBye() {
  if (!udp) return;
  try {
    sendDiscovery({ v: 1, type: 'bye', id: config.id });
  } catch (_) {}
}

// ---------------- HTTP 探测（手动添加节点 / 保活） ----------------
function fetchInfo(host, port, timeoutMs = 2500) {
  return new Promise((resolve) => {
    let done = false;
    const finish = (v) => { if (!done) { done = true; resolve(v); } };
    const req = http.get({ host, port, path: '/api/info', timeout: timeoutMs }, (res) => {
      let data = '';
      res.on('data', (c) => {
        data += c;
        if (data.length > 65536) req.destroy();
      });
      res.on('end', () => {
        try {
          const j = JSON.parse(data);
          finish(j && j.id ? j : null);
        } catch (_) { finish(null); }
      });
      res.on('error', () => finish(null));
    });
    req.on('timeout', () => { req.destroy(); finish(null); });
    req.on('error', () => finish(null));
  });
}

// 手动添加的节点没有 UDP 心跳，用 HTTP 探测保活
setInterval(() => {
  for (const p of [...peers.values()]) {
    if (!p.manual) continue;
    fetchInfo(p.host, p.httpPort).then((info) => {
      if (!info) return;
      if (info.id !== p.id) {
        // 对端 id 变化（重装等），迁移条目
        peers.delete(p.id);
        const np = peers.get(info.id) || { id: info.id };
        Object.assign(np, {
          id: info.id, name: info.name, os: info.os,
          host: p.host, httpPort: p.httpPort, manual: true, lastSeen: Date.now(),
        });
        peers.set(info.id, np);
        touchPeers();
        return;
      }
      p.name = info.name;
      p.os = info.os;
      p.lastSeen = Date.now();
      touchPeers();
    }).catch(() => {});
  }
}, 5000);

// ---------------- 网段扫描（UDP 广播被拦截时的兜底发现） ----------------
function upsertPeer(info, host, port, manual) {
  const isNew = !peers.has(info.id);
  const p = peers.get(info.id) || { id: info.id };
  Object.assign(p, {
    id: info.id, name: info.name, os: info.os,
    host, httpPort: port, manual: !!manual, lastSeen: Date.now(),
  });
  peers.set(info.id, p);
  touchPeers();
  return { p, isNew };
}

function isMyIp(host) {
  return host === '127.0.0.1' || host === 'localhost' || lanIps().includes(host);
}

/**
 * 发现同 id 但不同 IP 的节点 = 配置文件被整份拷贝过。
 * 自动重新生成本机 id，否则双方会互相当成自己而忽略对方的所有广播。
 */
function regenerateId(reason) {
  const old = config.id;
  config.id = buildNodeId();
  saveConfig();
  console.warn(`[配置] ${reason}，已自动重新生成本机 id（${old.slice(0, 8)}… → ${config.id.slice(0, 8)}…）`);
  announceAll();
}

/** 统一的节点接纳入口：处理撞 id 自愈、去重、登记与新节点的反向注册 */
function acceptDiscoveredNode(info, host, port, manual) {
  if (!info || !info.id) return null;
  if (info.id === config.id) {
    if (isMyIp(host)) return null;               // 探测到的是自己，忽略
    regenerateId(`发现同 id 节点 ${host}:${port}（config.json 可能被整份拷贝过）`);
  }
  const { p, isNew } = upsertPeer(info, host, port, manual);
  if (isNew) {
    console.log(`[发现] 新节点：${p.name} (${p.host}:${p.httpPort})`);
    registerSelfTo(host, port);
  }
  return p;
}

/** 挑选与对端同网段的本机 IP，作为对端回连自己的地址 */
function myIpFor(peerHost) {
  const peerInt = ipToInt(peerHost);
  for (const list of Object.values(os.networkInterfaces())) {
    for (const ni of list) {
      if (ni.family !== 'IPv4' || ni.internal || !ni.cidr) continue;
      const [ip, prefixStr] = ni.cidr.split('/');
      const mask = maskOf(parseInt(prefixStr, 10));
      if ((ipToInt(ip) & mask) === (peerInt & mask)) return ip;
    }
  }
  return lanIps()[0] || null;
}

/** 把本机注册到对方（对方用现有 /api/discover 反向添加我们），实现单向发现、双向可见 */
function registerSelfTo(peerHost, peerPort) {
  const myIp = myIpFor(peerHost);
  if (!myIp) return;
  const body = JSON.stringify({ host: myIp, port: httpPort });
  const req = http.request({
    host: peerHost, port: peerPort, path: '/api/discover', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    timeout: 3000,
  }, () => req.destroy());
  req.on('error', () => {});
  req.on('timeout', () => req.destroy());
  req.end(body);
}

function discoverPeer(host, port) {
  return fetchInfo(host, port).then((info) => {
    if (!info) return null;
    return acceptDiscoveredNode(info, host, port, true);
  });
}

const SCAN_PORTS = [...new Set([HTTP_PORT_BASE, 3210, 3211, 3212])];

function scanSubnets() {
  return new Promise((resolve) => {
    const hosts = new Set();
    const seenNets = new Set();
    for (const list of Object.values(os.networkInterfaces())) {
      for (const ni of list) {
        if (ni.family !== 'IPv4' || ni.internal || !ni.cidr) continue;
        const [ip, prefixStr] = ni.cidr.split('/');
        const prefix = parseInt(prefixStr, 10);
        if (prefix < 22 || prefix > 30) continue;        // 网段过大不扫，避免海量探测
        if (ip.startsWith('169.254.')) continue;         // 链路本地
        const netInt = ipToInt(ip) & maskOf(prefix);
        const key = `${netInt}/${prefix}`;
        if (seenNets.has(key)) continue;
        seenNets.add(key);
        const size = 2 ** (32 - prefix);
        const max = Math.min(size - 1, 1024);            // 单网段最多探测 1024 个地址
        for (let i = 1; i < max; i++) hosts.add(intToIp(netInt + i));
      }
    }
    if (!hosts.size) return resolve(0);

    const pairs = [];
    for (const h of hosts) for (const p of SCAN_PORTS) pairs.push([h, p]);

    const found = new Set();
    let idx = 0;
    const total = pairs.length;
    const startedAt = Date.now();
    const CONCURRENCY = 64;
    const CONNECT_TIMEOUT = 900;

    const probe = ([host, port]) => new Promise((resolvePair) => {
      const sock = net.createConnection({ host, port });
      let settled = false;
      const finish = (ok) => {
        if (settled) return;
        settled = true;
        sock.destroy();
        resolvePair(ok);
      };
      sock.setTimeout(CONNECT_TIMEOUT, () => finish(false));
      sock.on('connect', () => finish(true));
      sock.on('error', () => finish(false));
    });

    const worker = async () => {
      while (idx < total && Date.now() - startedAt < 20000) {
        const pair = pairs[idx++];
        if (await probe(pair)) {
          const info = await fetchInfo(pair[0], pair[1], 1200);
          const p = acceptDiscoveredNode(info, pair[0], pair[1], true);
          if (p) found.add(p.id);
        }
      }
    };
    const workers = Array.from({ length: Math.min(CONCURRENCY, total) }, worker);
    (async () => {
      await Promise.all(workers);
      console.log(`[扫描] 探测 ${total} 个地址，发现 ${found.size} 个节点，耗时 ${Date.now() - startedAt}ms`);
      resolve(found.size);
    })();
  });
}

// 离线清理
setInterval(() => {
  const now = Date.now();
  for (const [id, p] of peers) {
    if (now - p.lastSeen > PEER_TIMEOUT) {
      peers.delete(id);
      touchPeers();
    }
  }
}, 2000);

// ---------------- 接收侧传输任务 ----------------
function cleanupTmp(offer) {
  if (offer.tmpDir) {
    try { fs.rmSync(offer.tmpDir, { recursive: true, force: true }); } catch (_) {}
    offer.tmpDir = null;
  }
}

function emitProgress(offer, force = false) {
  const now = Date.now();
  if (!force && now - (offer._lastEmit || 0) < 400) return;
  offer._lastEmit = now;
  broadcastUpdate(offer);
}

// 超时清理 + 保留最近 100 条已结束记录
setInterval(() => {
  const now = Date.now();
  for (const o of offers.values()) {
    if (o.status === 'pending' && now - o.createdAt > OFFER_TIMEOUT) {
      o.status = 'expired';
      cleanupTmp(o);
      broadcastUpdate(o);
    } else if ((o.status === 'accepted' || o.status === 'transferring') &&
               now - (o.lastActivity || o.createdAt) > UPLOAD_TIMEOUT) {
      o.status = 'failed';
      cleanupTmp(o);
      broadcastUpdate(o);
    }
  }
  const finished = [...offers.values()]
    .filter((o) => ['done', 'failed', 'rejected', 'expired'].includes(o.status))
    .sort((a, b) => a.createdAt - b.createdAt);
  while (finished.length > FINISHED_KEEP) {
    const o = finished.shift();
    offers.delete(o.id);
  }
}, 3000);

function finalizeOffer(offer) {
  fs.mkdirSync(config.saveDir, { recursive: true });
  const finalDir = uniqueDest(path.join(config.saveDir, `${safeFsName(offer.from.name)}-${tsString()}`));
  try {
    fs.renameSync(offer.tmpDir, finalDir);
  } catch (err) {
    if (err.code === 'EXDEV') {
      fs.cpSync(offer.tmpDir, finalDir, { recursive: true });
      fs.rmSync(offer.tmpDir, { recursive: true, force: true });
    } else {
      throw err;
    }
  }
  return finalDir;
}

// ---------------- HTTP 路由 ----------------
function setCors(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers',
    req.headers['access-control-request-headers'] || '*');
  res.setHeader('Access-Control-Max-Age', '600');
}

function serveStatic(res, pathname) {
  const rel = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
  const filePath = path.normalize(path.join(__dirname, 'public', rel));
  if (filePath !== path.join(__dirname, 'public') &&
      !filePath.startsWith(path.join(__dirname, 'public') + path.sep)) {
    sendJson(res, 403, { error: 'Forbidden' });
    return;
  }
  fs.stat(filePath, (err, st) => {
    if (err || !st.isFile()) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('404 Not Found');
      return;
    }
    const ext = path.extname(filePath).toLowerCase();
    const mime = {
      '.html': 'text/html; charset=utf-8',
      '.js': 'text/javascript; charset=utf-8',
      '.css': 'text/css; charset=utf-8',
      '.svg': 'image/svg+xml',
      '.png': 'image/png',
      '.ico': 'image/x-icon',
      '.json': 'application/json; charset=utf-8',
    }[ext] || 'application/octet-stream';
    res.writeHead(200, { 'Content-Type': mime, 'Content-Length': st.size });
    fs.createReadStream(filePath).pipe(res);
  });
}

function handleUpload(req, res, offer, q) {
  if (offer.token !== q.get('token')) return sendJson(res, 403, { error: '令牌无效' });
  if (offer.status !== 'accepted' && offer.status !== 'transferring') {
    return sendJson(res, 409, { error: '传输未处于可接收状态' });
  }
  const rel = safeRelPath(q.get('path') || '');
  if (!rel) return sendJson(res, 400, { error: '非法文件路径' });
  if (!offer.tmpDir) return sendJson(res, 409, { error: '临时目录不存在' });

  const dest = path.join(offer.tmpDir, rel);
  const dir = path.dirname(dest);
  if (!offer.dirCache.has(dir)) {
    fs.mkdirSync(dir, { recursive: true });
    offer.dirCache.add(dir);
  }
  offer.status = 'transferring';
  offer.lastActivity = Date.now();

  const ws = fs.createWriteStream(dest);
  let responded = false;
  const finish = (code, obj) => {
    if (responded) return;
    responded = true;
    sendJson(res, code, obj);
  };

  req.on('data', (c) => {
    offer.receivedBytes += c.length;
    offer.lastActivity = Date.now();
    emitProgress(offer);
  });
  const abort = () => {
    ws.destroy();
    try { fs.unlinkSync(dest); } catch (_) {}
  };
  req.on('aborted', abort);
  req.on('error', abort);
  ws.on('error', () => finish(500, { error: '磁盘写入失败' }));
  ws.on('finish', () => {
    emitProgress(offer, true);
    finish(200, { ok: true, bytes: ws.bytesWritten });
  });
  req.pipe(ws);
}

async function handleApi(req, res, pathname, q) {
  const method = req.method;

  if (pathname === '/api/events' && method === 'GET') {
    handleSse(req, res);
    return undefined;
  }
  if (pathname === '/api/info' && method === 'GET') {
    return sendJson(res, 200, localInfo());
  }
  if (pathname === '/api/peers' && method === 'GET') {
    return sendJson(res, 200, { peers: [...peers.values()].map(publicPeer) });
  }
  if (pathname === '/api/offers' && method === 'GET') {
    return sendJson(res, 200, { offers: [...offers.values()].map(publicOffer) });
  }
  if (pathname === '/api/name' && method === 'POST') {
    const body = await readJson(req, 64 * 1024);
    if (!body) return sendJson(res, 400, { error: '请求体无效' });
    const name = String(body.name || '').trim().slice(0, 40);
    if (!name) return sendJson(res, 400, { error: '名称不能为空' });
    config.name = name;
    saveConfig();
    announceAll();
    return sendJson(res, 200, { ok: true, name });
  }
  if (pathname === '/api/discover' && method === 'POST') {
    const body = await readJson(req, 64 * 1024);
    if (!body) return sendJson(res, 400, { error: '请求体无效' });
    const host = String(body.host || '').trim();
    const port = parseInt(body.port, 10) || 3210;
    if (!host || port < 1 || port > 65535) return sendJson(res, 400, { error: '地址无效' });
    const p = await discoverPeer(host, port);
    if (!p) return sendJson(res, 502, { error: '无法连接该节点' });
    return sendJson(res, 200, { ok: true, peer: publicPeer(p) });
  }
  if (pathname === '/api/scan' && method === 'POST') {
    const found = await scanSubnets();
    return sendJson(res, 200, { ok: true, found });
  }
  if (pathname === '/api/message' && method === 'POST') {
    const body = await readJson(req, 256 * 1024);
    if (!body) return sendJson(res, 400, { error: '请求体无效' });
    const text = String(body.text || '').slice(0, 20000).trim();
    if (!text) return sendJson(res, 400, { error: '消息不能为空' });
    broadcast('message', {
      fromId: String(body.fromId || ''),
      fromName: String(body.fromName || '未知设备').slice(0, 60),
      text,
      ts: Date.now(),
    });
    return sendJson(res, 200, { ok: true });
  }

  if (pathname === '/api/transfer/offer' && method === 'POST') {
    const body = await readJson(req);
    if (!body || !Array.isArray(body.files) || !body.files.length) {
      return sendJson(res, 400, { error: '文件列表为空' });
    }
    const files = body.files
      .map((f) => ({ path: String(f.path || ''), size: Math.max(0, Number(f.size) || 0) }))
      .filter((f) => f.path);
    if (!files.length) return sendJson(res, 400, { error: '文件列表无效' });
    const offer = {
      id: crypto.randomUUID(),
      token: crypto.randomBytes(16).toString('hex'),
      from: { id: String(body.fromId || ''), name: String(body.fromName || '未知设备').slice(0, 60) },
      files,
      totalSize: files.reduce((s, f) => s + f.size, 0),
      status: 'pending',
      receivedBytes: 0,
      createdAt: Date.now(),
      tmpDir: null,
      savedDir: null,
      dirCache: new Set(),
    };
    offers.set(offer.id, offer);
    broadcast('offer', { offer: publicOffer(offer) });
    return sendJson(res, 200, { id: offer.id });
  }

  let m = pathname.match(/^\/api\/transfer\/status\/([\w-]+)$/);
  if (m && method === 'GET') {
    const offer = offers.get(m[1]);
    if (!offer) return sendJson(res, 404, { error: '任务不存在' });
    const active = offer.status === 'accepted' || offer.status === 'transferring';
    return sendJson(res, 200, {
      status: offer.status,
      token: active ? offer.token : undefined,
      receivedBytes: offer.receivedBytes,
    });
  }

  m = pathname.match(/^\/api\/transfer\/accept\/([\w-]+)$/);
  if (m && method === 'POST') {
    const offer = offers.get(m[1]);
    if (!offer) return sendJson(res, 404, { error: '任务不存在' });
    if (offer.status !== 'pending') return sendJson(res, 409, { error: '当前状态无法接受' });
    offer.status = 'accepted';
    offer.lastActivity = Date.now();
    offer.tmpDir = path.join(os.tmpdir(), `feiq-${offer.id}`);
    fs.mkdirSync(offer.tmpDir, { recursive: true });
    broadcastUpdate(offer);
    return sendJson(res, 200, { ok: true, token: offer.token });
  }

  m = pathname.match(/^\/api\/transfer\/reject\/([\w-]+)$/);
  if (m && method === 'POST') {
    const offer = offers.get(m[1]);
    if (!offer) return sendJson(res, 404, { error: '任务不存在' });
    if (offer.status !== 'pending') return sendJson(res, 409, { error: '当前状态无法拒绝' });
    offer.status = 'rejected';
    cleanupTmp(offer);
    broadcastUpdate(offer);
    return sendJson(res, 200, { ok: true });
  }

  m = pathname.match(/^\/api\/transfer\/abort\/([\w-]+)$/);
  if (m && method === 'POST') {
    const offer = offers.get(m[1]);
    if (!offer) return sendJson(res, 404, { error: '任务不存在' });
    if (offer.status !== 'accepted' && offer.status !== 'transferring') {
      return sendJson(res, 409, { error: '当前状态无法中止' });
    }
    offer.status = 'failed';
    cleanupTmp(offer);
    broadcastUpdate(offer);
    return sendJson(res, 200, { ok: true });
  }

  m = pathname.match(/^\/api\/transfer\/upload\/([\w-]+)$/);
  if (m && method === 'POST') {
    const offer = offers.get(m[1]);
    if (!offer) return sendJson(res, 404, { error: '任务不存在' });
    return handleUpload(req, res, offer, q);
  }

  m = pathname.match(/^\/api\/transfer\/complete\/([\w-]+)$/);
  if (m && method === 'POST') {
    const offer = offers.get(m[1]);
    if (!offer) return sendJson(res, 404, { error: '任务不存在' });
    if (offer.token !== q.get('token')) return sendJson(res, 403, { error: '令牌无效' });
    if (offer.status !== 'accepted' && offer.status !== 'transferring') {
      return sendJson(res, 409, { error: '传输未处于可接收状态' });
    }
    let finalDir;
    try {
      finalDir = finalizeOffer(offer);
    } catch (err) {
      offer.status = 'failed';
      cleanupTmp(offer);
      broadcastUpdate(offer);
      return sendJson(res, 500, { error: `保存失败：${err.message}` });
    }
    offer.tmpDir = null;
    offer.status = 'done';
    offer.savedDir = finalDir;
    broadcastUpdate(offer);
    return sendJson(res, 200, { ok: true, savedDir: finalDir });
  }

  return null; // 未匹配 API
}

const server = http.createServer((req, res) => {
  setCors(req, res);
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }
  let u;
  try {
    u = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  } catch (_) {
    sendJson(res, 400, { error: 'Bad Request' });
    return;
  }
  handleApi(req, res, u.pathname, u.searchParams)
    .then((matched) => {
      if (matched !== null) return;
      if (req.method === 'GET') return serveStatic(res, u.pathname);
      sendJson(res, 404, { error: 'Not Found' });
    })
    .catch((err) => {
      if (!res.headersSent) sendJson(res, 500, { error: err.message || '服务器错误' });
      else { try { res.end(); } catch (_) {} }
    });
});

function startHttp() {
  return new Promise((resolve) => {
    let attempt = 0;
    const tryListen = () => {
      const port = HTTP_PORT_BASE + attempt;
      server.once('error', (err) => {
        if (err.code === 'EADDRINUSE' && attempt < 10) {
          attempt++;
          console.warn(`[HTTP] 端口 ${port} 被占用，尝试 ${port + 1}`);
          tryListen();
        } else {
          console.error(`[HTTP] 监听失败：${err.message}`);
          process.exit(1);
        }
      });
      server.listen(port, '0.0.0.0', () => {
        server.removeAllListeners('error');
        httpPort = port;
        resolve();
      });
    };
    tryListen();
  });
}

function openBrowser() {
  if (process.env.NO_OPEN === '1') return;
  const url = `http://localhost:${httpPort}`;
  const cmd =
    process.platform === 'darwin' ? `open "${url}"` :
    process.platform === 'win32' ? `start "" "${url}"` :
    `xdg-open "${url}"`;
  exec(cmd, () => {});
}

async function main() {
  await startHttp();
  await startUdp();

  console.log('');
  console.log('  feiq-glm 局域网互传已启动');
  console.log(`  本机 id：${config.id}`);
  console.log(`  本机名称：${config.name}`);
  console.log(`  本机访问：http://localhost:${httpPort}`);
  for (const ip of lanIps()) {
    console.log(`  局域网访问：http://${ip}:${httpPort}`);
  }
  console.log(`  发现广播目标：${broadcastAddrs().join(', ')} (UDP ${udpPort || '未绑定'})`);
  console.log(`  接收目录：${config.saveDir}`);
  console.log('');

  // 启动后一段时间仍无节点时自动扫描一次网段（广播被防火墙/VPN 拦截的兜底）
  setTimeout(() => {
    if (peers.size === 0) {
      console.log('[发现] 尚未发现任何节点，自动扫描网段…');
      scanSubnets();
    }
  }, 12000);

  openBrowser();
}

function shutdown() {
  sendBye();
  setTimeout(() => process.exit(0), 150);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
process.on('uncaughtException', (err) => {
  console.error('[异常]', err.message);
});

main();
