/* feiq-glm 前端逻辑（原生 JS） */
'use strict';

// ---------------- 状态 ----------------
const state = {
  me: null,
  peers: new Map(),     // id -> peer
  selectedId: null,
  offers: new Map(),    // id -> 接收任务
  sends: new Map(),     // taskId -> 发送任务
  chats: new Map(),     // peerId -> [{dir, text, ts}]
  unread: new Map(),    // peerId -> count
};

const $ = (sel) => document.querySelector(sel);
const modalRoot = $('#modal-root');
const toastRoot = $('#toast-root');

// ---------------- 工具 ----------------
const OS_EMOJI = { macOS: '🍎', Windows: '🪟', Linux: '🐧' };
const osEmoji = (os) => OS_EMOJI[os] || '💻';

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

function fmtSize(n) {
  if (!isFinite(n) || n < 0) return '?';
  if (n < 1024) return `${n} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let v = n / 1024, i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v >= 100 ? v.toFixed(0) : v.toFixed(1)} ${units[i]}`;
}

function fmtTime(ts) {
  const d = new Date(ts);
  const p = (n) => String(n).padStart(2, '0');
  const now = new Date();
  const sameDay = d.toDateString() === now.toDateString();
  return sameDay
    ? `${p(d.getHours())}:${p(d.getMinutes())}`
    : `${d.getMonth() + 1}-${d.getDate()} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

const uid = () => Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function peerBase(peer) {
  return `http://${peer.host}:${peer.httpPort}`;
}

async function fetchJSON(url, method = 'GET', body = null, timeoutMs = 30000) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method,
      headers: body !== null ? { 'Content-Type': 'application/json' } : undefined,
      body: body !== null ? JSON.stringify(body) : undefined,
      signal: ctrl.signal,
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    return data;
  } finally {
    clearTimeout(timer);
  }
}

function toast(text, type = '') {
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.textContent = text;
  toastRoot.appendChild(el);
  setTimeout(() => el.remove(), 4500);
}

// ---------------- SSE ----------------
const es = new EventSource('/api/events');
es.addEventListener('init', (e) => {
  const d = JSON.parse(e.data);
  state.me = d.me;
  replacePeers(d.peers);
  for (const o of d.offers) state.offers.set(o.id, o);
  if (!state.selectedId) autoSelect();
  renderAll();
  renderModals(); // 页面打开/刷新时若有待确认的传输，补弹确认框
});
es.addEventListener('peers', (e) => {
  replacePeers(JSON.parse(e.data).peers);
  if (state.selectedId && !state.peers.has(state.selectedId)) state.selectedId = null;
  if (!state.selectedId) autoSelect();
  renderAll();
});
es.addEventListener('offer', (e) => {
  const { offer } = JSON.parse(e.data);
  state.offers.set(offer.id, offer);
  toast(`收到来自「${offer.from.name}」的文件请求（${offer.files.length} 个文件）`);
  renderTransfers();
  renderModals();
});
es.addEventListener('offer-update', (e) => {
  const d = JSON.parse(e.data);
  const o = state.offers.get(d.id);
  if (!o) return;
  const prev = o.status;
  Object.assign(o, d);
  if (prev !== o.status) {
    if (o.status === 'done') toast(`已保存至：${o.savedDir}`, 'ok');
    else if (o.status === 'rejected') toast('已拒绝对方的传输请求');
    else if (o.status === 'expired') toast('一个传输请求已超时', 'bad');
    else if (o.status === 'failed') toast('一个接收任务失败', 'bad');
  }
  renderTransfers();
  renderModals();
});
es.addEventListener('message', (e) => {
  const m = JSON.parse(e.data);
  addChat(m.fromId, { dir: 'in', text: m.text, ts: m.ts, name: m.fromName });
  if (state.selectedId === m.fromId) {
    renderChat();
  } else {
    state.unread.set(m.fromId, (state.unread.get(m.fromId) || 0) + 1);
    renderPeers();
    toast(`来自「${m.fromName}」的消息：${m.text.slice(0, 40)}`);
  }
});

function replacePeers(list) {
  const fresh = new Map();
  for (const p of list) fresh.set(p.id, p);
  state.peers = fresh;
}

function autoSelect() {
  const online = [...state.peers.values()].filter((p) => p.online);
  const first = online[0] || [...state.peers.values()][0];
  if (first) {
    state.selectedId = first.id;
    state.unread.set(first.id, 0);
  }
}

// ---------------- 渲染 ----------------
function renderAll() {
  renderMe();
  renderPeers();
  renderSession();
}

function renderMe() {
  if (!state.me) return;
  const nameEl = $('#me-name');
  // 输入框正在编辑时不覆盖用户输入（心跳每几秒触发一次全量渲染）
  if (document.activeElement !== nameEl) nameEl.value = state.me.name;
  const ip = (state.me.ips || []).join('，') || '未接入局域网';
  $('#me-meta').innerHTML = `IP：${esc(ip)}<br>接收目录：${esc(state.me.saveDir)}${state.me.saveDirFixed ? '（固定）' : ''}`;
}

function currentPeer() {
  return state.selectedId ? state.peers.get(state.selectedId) : null;
}

function renderPeers() {
  const list = $('#peer-list');
  const peers = [...state.peers.values()].sort((a, b) => {
    if (a.online !== b.online) return a.online ? -1 : 1;
    return a.name.localeCompare(b.name, 'zh');
  });
  const onlineCount = peers.filter((p) => p.online).length;
  $('#peer-count').textContent = `${onlineCount} 在线 / 共 ${peers.length}`;

  if (!peers.length) {
    list.innerHTML = '<li class="peer-empty">正在搜索局域网节点…<br>找不到？点上方「扫描」搜索网段，<br>或检查防火墙后手动添加</li>';
    return;
  }
  list.innerHTML = peers.map((p) => {
    const unread = state.unread.get(p.id) || 0;
    return `
    <li class="peer-item ${p.id === state.selectedId ? 'selected' : ''} ${p.online ? '' : 'offline'}"
        data-id="${esc(p.id)}">
      <div class="avatar">${osEmoji(p.os)}</div>
      <div class="p-info">
        <div class="p-name"><span>${esc(p.name)}</span>
          ${unread ? `<span class="badge">${unread}</span>` : ''}
        </div>
        <div class="p-meta">${esc(p.host)}:${p.httpPort}${p.manual ? ' · 手动添加' : ''}</div>
      </div>
      <span class="dot ${p.online ? 'on' : ''}"></span>
    </li>`;
  }).join('');
}

function renderSession() {
  const peer = currentPeer();
  $('#welcome').classList.toggle('hidden', !!peer);
  $('#session').classList.toggle('hidden', !peer);
  if (!peer) return;

  $('#sel-avatar').textContent = osEmoji(peer.os);
  $('#sel-name').textContent = peer.name;
  $('#sel-meta').textContent = `${peer.os} · ${peer.host}:${peer.httpPort}`;
  const badge = $('#sel-online');
  badge.textContent = peer.online ? '在线' : '离线';
  badge.classList.toggle('off', !peer.online);
  $('#dropzone').classList.toggle('disabled', !peer.online);

  renderTransfers();
  renderChat();
}

const SEND_STATUS = {
  waiting: ['等待对方确认', ''],
  uploading: ['传输中', ''],
  done: ['已完成', 'ok'],
  rejected: ['对方已拒绝', 'bad'],
  expired: ['已超时', 'bad'],
  failed: ['传输失败', 'bad'],
};
const RECV_STATUS = {
  pending: ['等待你确认', 'warn'],
  accepted: ['已接受，等待传输', ''],
  transferring: ['接收中', ''],
  done: ['已完成', 'ok'],
  rejected: ['已拒绝', 'bad'],
  expired: ['已超时', 'bad'],
  failed: ['传输失败', 'bad'],
};

function taskTitle(paths) {
  const tops = [...new Set(paths.map((p) => p.split('/')[0]))];
  return tops.length > 1 ? `${tops[0]} 等 ${tops.length} 项` : (tops[0] || '未知');
}

function renderTransfers() {
  const peer = currentPeer();
  const sendBox = $('#send-items');
  const recvBox = $('#recv-items');
  if (!peer) return;

  const sends = [...state.sends.values()].filter((t) => t.peerId === peer.id).reverse();
  sendBox.innerHTML = sends.length ? sends.map((t) => {
    const [label, cls] = SEND_STATUS[t.status] || [t.status, ''];
    const pct = t.totalSize ? Math.min(100, Math.round((t.sentBytes / t.totalSize) * 100)) : 0;
    const statusText = t.status === 'uploading' ? `传输中 ${pct}%` : label;
    const barCls = t.status === 'done' ? 'done' : (['failed', 'rejected', 'expired'].includes(t.status) ? 'bad' : '');
    return `
    <div class="titem">
      <div class="titem-top">
        <span class="titem-icon">⬆️</span>
        <span class="titem-name">${esc(t.title)}</span>
        <span class="titem-status ${cls}">${esc(statusText)}</span>
      </div>
      <div class="titem-sub">
        <span>${t.files.length} 个文件 · ${fmtSize(t.totalSize)} · ${fmtTime(t.ts)}</span>
        <span class="dir">${t.status === 'failed' && t.error ? esc(t.error) : (t.savedDir ? `已保存至 ${esc(t.savedDir)}` : '')}</span>
      </div>
      <div class="bar"><div class="bar-fill ${barCls}" style="width:${t.status === 'done' ? 100 : pct}%"></div></div>
    </div>`;
  }).join('') : '<p class="empty">暂无发送任务</p>';

  const recvs = [...state.offers.values()].filter((o) => o.from.id === peer.id).reverse();
  recvBox.innerHTML = recvs.length ? recvs.map((o) => {
    const [label, cls] = RECV_STATUS[o.status] || [o.status, ''];
    const pct = o.totalSize ? Math.min(100, Math.round((o.receivedBytes / o.totalSize) * 100)) : 0;
    const statusText = o.status === 'transferring' ? `接收中 ${pct}%` : label;
    const barCls = o.status === 'done' ? 'done' : (['failed', 'rejected', 'expired'].includes(o.status) ? 'bad' : '');
    return `
    <div class="titem">
      <div class="titem-top">
        <span class="titem-icon">⬇️</span>
        <span class="titem-name">${esc(taskTitle(o.files.map((f) => f.path)))}</span>
        <span class="titem-status ${cls}">${esc(statusText)}</span>
      </div>
      <div class="titem-sub">
        <span>${o.files.length} 个文件 · ${fmtSize(o.totalSize)} · ${fmtTime(o.createdAt)}</span>
        <span class="dir">${o.savedDir ? `已保存至 ${esc(o.savedDir)}` : ''}</span>
      </div>
      <div class="bar"><div class="bar-fill ${barCls}" style="width:${o.status === 'done' ? 100 : pct}%"></div></div>
    </div>`;
  }).join('') : '<p class="empty">暂无接收任务</p>';
}

function renderChat() {
  const peer = currentPeer();
  const box = $('#chat-list');
  if (!peer) return;
  const msgs = state.chats.get(peer.id) || [];
  if (!msgs.length) {
    box.innerHTML = '<div class="chat-empty">暂无消息，发一条打个招呼吧</div>';
    return;
  }
  box.innerHTML = msgs.map((m) => `
    <div class="msg ${m.dir}">
      <div class="bubble">${esc(m.text)}</div>
      <div class="meta">${m.dir === 'in' ? esc(m.name || '') + ' · ' : ''}${fmtTime(m.ts)}</div>
    </div>`).join('');
  box.scrollTop = box.scrollHeight;
}

function addChat(peerId, msg) {
  if (!state.chats.has(peerId)) state.chats.set(peerId, []);
  state.chats.get(peerId).push(msg);
}

// ---------------- 收件确认弹窗 ----------------
function renderModals() {
  const pending = [...state.offers.values()].filter((o) => o.status === 'pending');
  const fixed = !!(state.me && state.me.saveDirFixed);
  const prefill = state.me ? (state.me.lastSaveDir || state.me.saveDir || '') : '';
  modalRoot.innerHTML = pending.map((o) => {
    const rows = o.files.slice(0, 50).map((f) =>
      `<div><span>${esc(f.path)}</span><span class="fsize">${fmtSize(f.size)}</span></div>`).join('');
    const more = o.files.length > 50 ? `<div><span>… 共 ${o.files.length} 个文件</span><span></span></div>` : '';
    const pathBlock = fixed
      ? `<p class="m-fixed">保存到：${esc(state.me.saveDir)}（已在配置中固定，不再询问）</p>`
      : `<div class="m-path">
          <label class="m-label">保存到（可修改）</label>
          <input class="m-save-dir" value="${esc(prefill)}" spellcheck="false">
          <label class="m-remember"><input type="checkbox" class="m-remember-box"> 记住此路径，以后不再询问</label>
        </div>`;
    return `
    <div class="modal-overlay">
      <div class="modal">
        <h3>📥 收到文件请求</h3>
        <p class="m-sub">来自 <strong>${esc(o.from.name)}</strong> · 共 ${o.files.length} 个文件 · ${fmtSize(o.totalSize)}</p>
        <div class="m-files">${rows}${more}</div>
        ${pathBlock}
        <div class="m-actions">
          <button class="btn danger" data-act="reject" data-id="${esc(o.id)}">拒绝</button>
          <button class="btn primary" data-act="accept" data-id="${esc(o.id)}">接受</button>
        </div>
      </div>
    </div>`;
  }).join('');
}

modalRoot.addEventListener('click', async (e) => {
  const btn = e.target.closest('[data-act]');
  if (!btn) return;
  const id = btn.dataset.id;
  const act = btn.dataset.act;
  const offer = state.offers.get(id);
  if (!offer) return;
  const modal = btn.closest('.modal');

  let body = {};
  if (act === 'accept' && !(state.me && state.me.saveDirFixed)) {
    const dir = ((modal.querySelector('.m-save-dir') || {}).value || '').trim();
    if (!dir) { toast('请填写保存路径', 'bad'); return; }
    body.saveDir = dir;
    body.always = !!(modal.querySelector('.m-remember-box') || {}).checked;
  }

  btn.disabled = true;
  try {
    if (act === 'accept') {
      const r = await fetchJSON(`/api/transfer/accept/${id}`, 'POST', body);
      offer.status = 'accepted';
      if (state.me && r.saveDir) {
        state.me.saveDir = r.saveDir;
        state.me.saveDirFixed = !!r.saveDirFixed;
        renderMe();
      }
      toast(`已接受「${offer.from.name}」的传输`, 'ok');
    } else {
      await fetchJSON(`/api/transfer/reject/${id}`, 'POST', {});
      offer.status = 'rejected';
    }
  } catch (err) {
    toast(`操作失败：${err.message}`, 'bad');
  }
  renderModals();
  renderTransfers();
});

// ---------------- 文件收集 ----------------
function walkEntry(entry, prefix, out) {
  return new Promise((resolve) => {
    if (entry.isFile) {
      entry.file((f) => {
        out.push({ file: f, path: prefix + entry.name });
        resolve();
      }, () => resolve());
    } else if (entry.isDirectory) {
      const reader = entry.createReader();
      const all = [];
      const readBatch = () => {
        reader.readEntries(async (batch) => {
          if (!batch.length) {
            for (const e of all) await walkEntry(e, prefix + entry.name + '/', out);
            resolve();
          } else {
            all.push(...batch);
            readBatch();
          }
        }, () => resolve());
      };
      readBatch();
    } else {
      resolve();
    }
  });
}

/** 必须在 drop 事件内同步读取 dataTransfer.items，之后再异步展开 */
function collectFromDataTransfer(dt) {
  const out = [];
  const entries = [];
  if (dt.items && dt.items.length) {
    for (const item of Array.from(dt.items)) {
      if (item.kind !== 'file') continue;
      const entry = item.webkitGetAsEntry && item.webkitGetAsEntry();
      if (entry) entries.push(entry);
      else {
        const f = item.getAsFile();
        if (f) out.push({ file: f, path: f.name });
      }
    }
  }
  if (!entries.length && dt.files && dt.files.length) {
    for (const f of Array.from(dt.files)) out.push({ file: f, path: f.name });
  }
  return { out, entries };
}

async function expandEntries(entries, out) {
  for (const e of entries) await walkEntry(e, '', out);
  return out;
}

// ---------------- 发送流程 ----------------
function uploadFile(url, file, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open('POST', url, true);
    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable) onProgress(e.loaded);
    };
    xhr.onload = () => (xhr.status >= 200 && xhr.status < 300
      ? resolve()
      : reject(new Error(`HTTP ${xhr.status}`)));
    xhr.onerror = () => reject(new Error('网络错误'));
    xhr.send(file);
  });
}

async function waitForAccept(base, task) {
  for (let i = 0; i < 65; i++) {
    await sleep(1000);
    let st;
    try {
      st = await fetchJSON(`${base}/api/transfer/status/${task.offerId}`, 'GET', null, 8000);
    } catch (err) {
      throw new Error(`无法查询传输状态：${err.message}`);
    }
    if (st.status === 'accepted') return { token: st.token };
    if (st.status === 'rejected') { task.status = 'rejected'; throw new Error('对方已拒绝'); }
    if (st.status === 'expired') { task.status = 'expired'; throw new Error('等待确认超时'); }
  }
  task.status = 'expired';
  throw new Error('等待确认超时');
}

let renderQueued = false;
function renderTransfersSoon() {
  if (renderQueued) return;
  renderQueued = true;
  requestAnimationFrame(() => {
    renderQueued = false;
    renderTransfers();
  });
}

async function startSend(peer, items) {
  if (!state.me || !items.length) return;
  const totalSize = items.reduce((s, i) => s + i.file.size, 0);
  const tops = [...new Set(items.map((i) => i.path.split('/')[0]))];
  const task = {
    id: uid(),
    peerId: peer.id,
    title: tops.length > 1 ? `${tops[0]} 等 ${tops.length} 项` : (tops[0] || '未知'),
    files: items,
    totalSize,
    sentBytes: 0,
    status: 'waiting',
    savedDir: null,
    error: null,
    offerId: null,
    token: null,
    ts: Date.now(),
  };
  state.sends.set(task.id, task);
  renderTransfers();

  const base = peerBase(peer);
  try {
    const r = await fetchJSON(`${base}/api/transfer/offer`, 'POST', {
      fromId: state.me.id,
      fromName: state.me.name,
      totalSize,
      files: items.map((i) => ({ path: i.path, size: i.file.size })),
    });
    task.offerId = r.id;

    const acc = await waitForAccept(base, task);
    task.token = acc.token;
    task.status = 'uploading';
    renderTransfersSoon();

    let baseSent = 0;
    let lastPaint = 0;
    for (const item of task.files) {
      await uploadFile(
        `${base}/api/transfer/upload/${task.offerId}?token=${encodeURIComponent(task.token)}&path=${encodeURIComponent(item.path)}`,
        item.file,
        (loaded) => {
          task.sentBytes = baseSent + loaded;
          const now = Date.now();
          if (now - lastPaint > 120) { lastPaint = now; renderTransfersSoon(); }
        },
      );
      baseSent += item.file.size;
      task.sentBytes = baseSent;
      renderTransfersSoon();
    }

    const done = await fetchJSON(
      `${base}/api/transfer/complete/${task.offerId}?token=${encodeURIComponent(task.token)}`,
      'POST', {});
    task.status = 'done';
    task.savedDir = done.savedDir;
  } catch (err) {
    if (!['rejected', 'expired', 'done'].includes(task.status)) {
      task.status = 'failed';
      task.error = err.message;
      if (task.offerId && task.token) {
        fetchJSON(`${base}/api/transfer/abort/${task.offerId}?token=${encodeURIComponent(task.token)}`,
          'POST', {}, 5000).catch(() => {});
      }
    }
  }
  renderTransfers();
}

// ---------------- 交互绑定 ----------------
$('#peer-list').addEventListener('click', (e) => {
  const item = e.target.closest('.peer-item');
  if (!item) return;
  state.selectedId = item.dataset.id;
  state.unread.set(state.selectedId, 0);
  renderAll();
});

const dropzone = $('#dropzone');
dropzone.addEventListener('dragover', (e) => {
  e.preventDefault();
  dropzone.classList.add('dragover');
});
dropzone.addEventListener('dragleave', (e) => {
  if (e.target === dropzone) dropzone.classList.remove('dragover');
});
dropzone.addEventListener('drop', async (e) => {
  e.preventDefault();
  dropzone.classList.remove('dragover');
  const peer = currentPeer();
  if (!peer) return;
  if (!peer.online) { toast('对方节点当前离线', 'bad'); return; }
  const { out, entries } = collectFromDataTransfer(e.dataTransfer);
  await expandEntries(entries, out);
  if (!out.length) { toast('未读取到任何文件', 'bad'); return; }
  startSend(peer, out);
});

$('#pick-files-btn').addEventListener('click', () => $('#pick-files').click());
$('#pick-folder-btn').addEventListener('click', () => $('#pick-folder').click());
$('#pick-files').addEventListener('change', (e) => {
  const peer = currentPeer();
  const items = [...e.target.files].map((f) => ({ file: f, path: f.name }));
  e.target.value = '';
  if (!peer) return;
  if (!peer.online) { toast('对方节点当前离线', 'bad'); return; }
  if (items.length) startSend(peer, items);
});
$('#pick-folder').addEventListener('change', (e) => {
  const peer = currentPeer();
  const items = [...e.target.files]
    .filter((f) => f.webkitRelativePath)
    .map((f) => ({ file: f, path: f.webkitRelativePath }));
  e.target.value = '';
  if (!peer) return;
  if (!peer.online) { toast('对方节点当前离线', 'bad'); return; }
  if (items.length) startSend(peer, items);
});

$('#chat-send').addEventListener('click', sendChat);
$('#chat-text').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') sendChat();
});

function sendChat() {
  const peer = currentPeer();
  const input = $('#chat-text');
  const text = input.value.trim();
  if (!peer || !text || !state.me) return;
  input.value = '';
  addChat(peer.id, { dir: 'out', text, ts: Date.now() });
  renderChat();
  fetchJSON(`${peerBase(peer)}/api/message`, 'POST', {
    fromId: state.me.id,
    fromName: state.me.name,
    text,
  }, 10000).catch(() => toast('消息发送失败', 'bad'));
}

function parseAddr(str) {
  const s = str.trim();
  if (!s) return null;
  let host = s;
  let port = 3210;
  const m = s.match(/^(.+):(\d+)$/);
  if (m) {
    host = m[1].trim();
    port = parseInt(m[2], 10);
  }
  if (!host || !(port >= 1 && port <= 65535)) return null;
  return { host, port };
}

$('#add-btn').addEventListener('click', async () => {
  const input = $('#add-input');
  const addr = parseAddr(input.value);
  if (!addr) { toast('请输入有效地址，如 192.168.1.6:3210', 'bad'); return; }
  try {
    await fetchJSON('/api/discover', 'POST', addr, 8000);
    input.value = '';
    toast(`已添加节点 ${addr.host}:${addr.port}`, 'ok');
  } catch (err) {
    toast(`添加失败：${err.message}`, 'bad');
  }
});
$('#add-input').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') $('#add-btn').click();
});

$('#scan-btn').addEventListener('click', async () => {
  const btn = $('#scan-btn');
  btn.disabled = true;
  btn.textContent = '扫描中';
  toast('正在扫描本机网段，约需几秒…');
  try {
    const r = await fetchJSON('/api/scan', 'POST', {}, 20000);
    toast(r.found ? `扫描完成，发现 ${r.found} 个节点` : '扫描完成，未发现新节点', r.found ? 'ok' : '');
  } catch (err) {
    toast(`扫描失败：${err.message}`, 'bad');
  } finally {
    btn.disabled = false;
    btn.textContent = '扫描';
  }
});

$('#me-name').addEventListener('change', async (e) => {
  const name = e.target.value.trim();
  if (!name || !state.me) { renderMe(); return; }
  try {
    const r = await fetchJSON('/api/name', 'POST', { name }, 8000);
    state.me.name = r.name;
    toast('设备名已更新', 'ok');
  } catch (err) {
    toast(`修改失败：${err.message}`, 'bad');
  } finally {
    renderMe();
  }
});
