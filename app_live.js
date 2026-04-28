'use strict';

/* ═══════════════════════════════════════════════════════════
   app_live.js — Production build
   Render URL: https://screen-connect-rtca.onrender.com
   - Socket.io connects to Render, not localhost
   - No window.location.href = 'login.html' redirects
     (works with both separate-file and combined single-file builds)
═══════════════════════════════════════════════════════════ */

let screens = [];
const files = [];
const activityLogs = [];

const defaultFiles = [
  { name: 'No files yet', size: '0 KB', type: 'doc', modified: 'No sync events yet', locked: false },
];

/* ── Production Server URL ──────────────────────────────────
   Always reads from config.js first, falls back to Render URL.
   Never use localhost or a raw IP in production.
─────────────────────────────────────────────────────────── */
const PROD_SERVER_URL = 'https://screen-connect-rtca.onrender.com';

function getServerUrl() {
  return window.MVIEW_SERVER_URL || PROD_SERVER_URL;
}

/* ── Socket.io connection ───────────────────────────────────
   Connects to Render. The server must have:
     socketio = SocketIO(app, cors_allowed_origins="*")
   Do NOT add :10000 — Render exposes only HTTPS (443).
─────────────────────────────────────────────────────────── */
let socket = null;

function initSocket() {
  const url = getServerUrl();
  if (typeof io === 'undefined') {
    // Socket.io library not loaded — skip silently (Supabase-only mode)
    console.info('[app_live] socket.io not found — running in Supabase-only mode.');
    return;
  }
  try {
    socket = io(url, {
      transports: ['websocket', 'polling'],
      reconnectionAttempts: 5,
      timeout: 10000,
    });

    socket.on('connect', () => {
      console.info('[app_live] Socket connected to', url);
      updateFlaskStatus(true);
    });

    socket.on('disconnect', () => {
      console.warn('[app_live] Socket disconnected');
      updateFlaskStatus(false);
    });

    socket.on('connect_error', (err) => {
      console.warn('[app_live] Socket connection error:', err.message);
      updateFlaskStatus(false);
    });

    // Real-time device updates pushed from Flask
    socket.on('device_update', (data) => {
      if (data && Array.isArray(data.rows)) {
        hydrateScreens(data.rows);
        renderScreensTable(screens);
        renderRemoteGrid();
      }
    });

  } catch (err) {
    console.error('[app_live] Socket init failed:', err);
    updateFlaskStatus(false);
  }
}

/* ── Flask / server status indicator ───────────────────── */
function updateFlaskStatus(isOnline) {
  // Update any status badge that shows Flask/server health
  const badge = document.getElementById('flask-status-badge');
  if (!badge) return;
  badge.textContent = isOnline ? 'Server Online' : 'Server Offline';
  badge.className = isOnline
    ? 'status-chip on'
    : 'status-chip';
  badge.style.background = isOnline
    ? 'var(--green-soft, rgba(48,209,88,0.12))'
    : 'var(--red-soft, rgba(255,59,48,0.10))';
  badge.style.color = isOnline
    ? 'var(--green-text, #1a7a35)'
    : 'var(--red-text, #8b1a14)';
}

/* ── Supabase data layer ─────────────────────────────────── */
const DashboardData = (() => {
  const config = () => (window.SessionManager?.CONFIG || {});
  const supabaseBaseUrl = () =>
    String(config().SUPABASE_URL || '').trim().replace(/\/+$/, '').replace(/\/rest\/v1$/i, '');

  function tableCandidates() {
    const configured = config().TABLE_CANDIDATES || [];
    return configured.length ? configured : ['devices', 'agent_invites'];
  }

  async function fetchRows(table, query = '') {
    const url = `${supabaseBaseUrl()}/rest/v1/${table}${query}`;
    const res = await fetch(url, {
      headers: {
        apikey: config().SUPABASE_ANON_KEY,
        Authorization: `Bearer ${config().SUPABASE_ANON_KEY}`,
      },
    });
    if (!res.ok) throw new Error(`${table} request failed (${res.status})`);
    return res.json();
  }

  async function loadRows() {
    for (const table of tableCandidates()) {
      try {
        const rows = await fetchRows(table, '?select=*');
        return { table, rows: Array.isArray(rows) ? rows : [] };
      } catch (error) {
        if (!String(error.message).includes('(404)')) console.warn(error.message);
      }
    }
    return { table: null, rows: [] };
  }

  return { loadRows };
})();

/* ── Helpers ─────────────────────────────────────────────── */
function formatRelativeTime(value) {
  if (!value) return 'Just now';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return 'Just now';
  const mins = Math.max(1, Math.floor((Date.now() - date.getTime()) / 60000));
  if (mins < 60) return `${mins} min ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs} hr ago`;
  return `${Math.floor(hrs / 24)} day ago`;
}

function toGB(value) {
  if (typeof value !== 'number') return '0.0 GB';
  return `${value.toFixed(1)} GB`;
}

function statusFromRow(row) {
  const raw = String(row.status || 'pending').toLowerCase();
  if (raw === 'connected' || raw === 'online') return 'online';
  if (raw === 'warning' || raw === 'warn') return 'warning';
  return 'offline';
}

function capitalize(str) { return str ? str.charAt(0).toUpperCase() + str.slice(1) : ''; }

/* ── Hydrate screens from Supabase rows ─────────────────── */
function hydrateScreens(rows) {
  screens = (rows || []).map((row, idx) => ({
    id: idx + 1,
    name: row.label || row.device_name || row.device_id || row.token || `Screen-${idx + 1}`,
    status: statusFromRow(row),
    location: row.location || 'Unknown location',
    lastActive: formatRelativeTime(row.connected_at || row.created_at),
    storage: toGB(typeof row.storage_gb === 'number' ? row.storage_gb : 0),
    type: row.device_type || 'Standard Display',
    rawStatus: String(row.status || 'pending').toLowerCase(),
    ipAddress: row.ip_address || null,
  }));
}

/* ── KPI cards ──────────────────────────────────────────── */
function renderKpis(rows) {
  const total   = rows.length;
  const online  = rows.filter((r) => ['connected', 'online'].includes(String(r.status || '').toLowerCase())).length;
  const alerts  = rows.filter((r) => ['warning', 'warn', 'offline', 'error'].includes(String(r.status || '').toLowerCase())).length;
  const storage = rows.reduce((sum, r) => sum + (Number(r.storage_gb) || 0), 0);

  const k  = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  const ks = (id, v) => { const el = document.getElementById(id); if (el) el.innerHTML = v; };

  k('kpi-total-screens',   String(total));
  ks('kpi-total-screens-sub', `<span class="material-symbols-outlined">monitor</span> live from server`);
  k('kpi-online-now',      String(online));
  ks('kpi-online-now-sub', `<span class="material-symbols-outlined">wifi</span> ${Math.round((online / Math.max(total, 1)) * 100)}% online`);
  k('kpi-alerts',          String(alerts));
  ks('kpi-alerts-sub',     `<span class="material-symbols-outlined">warning</span> live issue count`);
  k('kpi-storage-used',    toGB(storage));
  ks('kpi-storage-used-sub', `<span class="material-symbols-outlined">storage</span> across all screens`);

  k('donut-total',   String(total));
  k('legend-online', String(online));
  k('legend-syncing', String(Math.max(0, total - online - alerts)));
  k('legend-warn',   String(alerts));

  const sub = document.getElementById('dashboard-sub');
  if (sub) {
    sub.textContent = total === 0
      ? 'No devices connected — Add a device to begin'
      : `${online}/${total} screens online`;
  }
}

/* ── App usage tiles ────────────────────────────────────── */
function renderUsageTiles(rows) {
  const from = (field) => rows.reduce((sum, r) => sum + (Number(r[field]) || 0), 0);
  const totals = {
    yt: from('youtube_minutes'),
    ch: from('chrome_minutes'),
    tt: from('tiktok_minutes'),
    ig: from('instagram_minutes'),
  };
  const sum = Math.max(1, totals.yt + totals.ch + totals.tt + totals.ig);
  [['youtube', totals.yt], ['chrome', totals.ch], ['tiktok', totals.tt], ['instagram', totals.ig]].forEach(([key, mins]) => {
    const pct = Math.round((mins / sum) * 100);
    const t = document.getElementById(`usage-${key}-time`);
    const p = document.getElementById(`usage-${key}-pct`);
    const b = document.getElementById(`usage-${key}-bar`);
    if (t) t.textContent = `${Math.round(mins)} min`;
    if (p) p.textContent = `${pct}%`;
    if (b) b.style.width = `${pct}%`;
  });
  const highest = Math.max(totals.yt, totals.ch, totals.tt, totals.ig, 1);
  [['ua-yt', totals.yt], ['ua-ch', totals.ch], ['ua-tt', totals.tt], ['ua-ig', totals.ig]].forEach(([id, v]) => {
    const el = document.getElementById(id);
    if (el) el.style.height = `${Math.max(6, Math.round((v / highest) * 100))}%`;
  });
}

/* ── Connection overview text ───────────────────────────── */
function renderConnectionOverviews(rows) {
  const pending   = rows.filter((r) => ['pending', 'downloading'].includes(String(r.status || '').toLowerCase()));
  const connected = rows.filter((r) => ['connected', 'online'].includes(String(r.status || '').toLowerCase()));
  const text = pending.length
    ? `${pending.length} link(s) establishing. Connected: ${connected.length}.`
    : connected.length
      ? `${connected.length} screen(s) connected and active.`
      : 'No active links. Generate a link to connect a screen.';
  const conn = document.getElementById('connection-overview');
  if (conn) conn.textContent = text;
  const explorerSub = document.getElementById('explorer-status-sub');
  if (explorerSub) explorerSub.textContent = `Connected screens: ${connected.length} · Files from server`;
}

/* ── Storage summary ────────────────────────────────────── */
function renderStorageSummary(rows) {
  const totalGb   = rows.reduce((sum, r) => sum + (Number(r.storage_gb) || 0), 0);
  const fileCount = rows.reduce((sum, r) => sum + (Number(r.files_count) || 0), 0);
  const maxGb = 20;
  const pct = Math.min(100, Math.round((totalGb / maxGb) * 100));
  const usageMain = document.getElementById('storage-usage-main');
  if (usageMain) usageMain.textContent = `${toGB(totalGb)} / ${maxGb} GB`;
  const filesCount = document.getElementById('storage-files-count');
  if (filesCount) filesCount.textContent = `${fileCount} items`;
  const fill = document.querySelector('.storage-fill');
  if (fill) fill.style.width = rows.length ? `${pct}%` : '0%';
}

/* ── User info hydration ────────────────────────────────── */
function hydrateUserInfo() {
  const raw = localStorage.getItem('mview-auth-user');
  if (!raw) return false;
  try {
    const user = JSON.parse(raw);
    if (!user || !user.email) return false;
    const initials = (user.name || user.email)
      .split(/\s+/)
      .map((w) => w[0])
      .join('')
      .toUpperCase()
      .slice(0, 2);
    const setTxt = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
    setTxt('sidebar-user-name', user.name || user.email);
    setTxt('sidebar-user-avatar', initials);
    setTxt('topbar-user-avatar', initials);
    const nameInput = document.getElementById('settings-full-name');
    if (nameInput) nameInput.value = user.name || '';
    const welcomeEl = document.getElementById('cw-welcome-text');
    if (welcomeEl) welcomeEl.textContent = `Welcome back, ${(user.name || user.email).split(' ')[0]}!`;
    const instanceEl = document.getElementById('cw-instance-id');
    if (instanceEl) instanceEl.textContent = `Logged in as ${user.email}`;
    return true;
  } catch {
    return false;
  }
}

/* ── Screens table ──────────────────────────────────────── */
function renderScreensTable(data) {
  const tbody = document.getElementById('screens-tbody');
  if (!tbody) return;
  tbody.innerHTML = data.length
    ? data.map((s, idx) => `
      <tr>
        <td>
          <div style="font-weight:600">${s.name}</div>
          <div style="font-size:10px;color:var(--text-2)">${s.type}</div>
        </td>
        <td><span class="status-pill ${s.status}"><i></i>${capitalize(s.status)}</span></td>
        <td style="color:var(--text-2)">${s.location}</td>
        <td style="color:var(--text-2)">${s.lastActive}</td>
        <td>${s.storage}</td>
        <td style="display:flex;gap:5px">
          <button class="action-btn" title="View Details" onclick="openScreenDetail(${idx})">
            <span class="material-symbols-outlined" style="font-size:16px">info</span>
          </button>
          <button class="action-btn" title="Remote View" onclick="showSection('remote-view')">
            <span class="material-symbols-outlined" style="font-size:16px">visibility</span>
          </button>
        </td>
      </tr>
    `).join('')
    : `<tr><td colspan="6" style="padding:24px;text-align:center;color:var(--text-3)">
        <span class="material-symbols-outlined" style="font-size:2rem;display:block;margin-bottom:8px">devices</span>
        No connected screens yet. Click <strong>Add Device</strong> to start.
      </td></tr>`;
}

/* ── Remote view grid ───────────────────────────────────── */
function renderRemoteGrid() {
  const grid = document.getElementById('remote-grid');
  if (!grid) return;
  if (!screens.length) { grid.innerHTML = ''; return; }
  grid.innerHTML = screens.map((s, idx) => {
    const statusColor = s.status === 'online' ? '#4ade80' : s.status === 'warning' ? '#fbbf24' : '#f87171';
    return `
    <div class="remote-card">
      <div class="remote-screen">
        <span class="material-symbols-outlined">monitor</span>
        <div class="remote-screen-overlay">
          <span style="width:5px;height:5px;border-radius:50%;background:${statusColor}"></span>
          ${capitalize(s.status)}
        </div>
      </div>
      <div class="remote-card-body">
        <div class="remote-card-name">${s.name}</div>
        <div class="remote-card-sub">${s.location}</div>
      </div>
      <div class="remote-card-footer">
        <span class="status-pill ${s.status}"><i></i>${capitalize(s.status)}</span>
        <button class="btn-ghost btn-sm" onclick="openScreenDetail(${idx})">
          <span class="material-symbols-outlined" style="font-size:14px">open_in_new</span>
          ${s.rawStatus === 'pending' ? 'Pending' : 'Connect'}
        </button>
      </div>
    </div>`;
  }).join('');
}

/* ── Files table ────────────────────────────────────────── */
function renderFilesTable(data) {
  const tbody = document.getElementById('file-tbody');
  if (!tbody) return;
  const iconMap = { vid: 'videocam', doc: 'description', img: 'image' };
  tbody.innerHTML = data.map((f) => `
    <tr>
      <td>
        <div class="file-icon-wrap">
          <div class="file-icon ${f.type}">
            <span class="material-symbols-outlined" style="font-size:14px">${iconMap[f.type] || 'description'}</span>
          </div>
          <span style="font-weight:500">${f.name}</span>
        </div>
      </td>
      <td>${f.size}</td>
      <td style="text-transform:uppercase;font-size:10px;letter-spacing:.06em;color:var(--text-2)">${f.type}</td>
      <td style="color:var(--text-2)">${f.modified}</td>
      <td>
        <button class="action-btn" title="Download">
          <span class="material-symbols-outlined" style="font-size:16px">download</span>
        </button>
      </td>
    </tr>
  `).join('');
}

/* ── Logs table ─────────────────────────────────────────── */
function renderLogsTable(data) {
  const tbody = document.getElementById('logs-tbody');
  if (!tbody) return;
  tbody.innerHTML = data.length
    ? data.map((l) => `
      <tr>
        <td style="color:var(--text-2);font-size:12px;white-space:nowrap">${l.time}</td>
        <td style="font-weight:500">${l.event}</td>
        <td>${l.screen}</td>
        <td>${l.user}</td>
        <td><span class="log-severity ${l.sev}">${capitalize(l.sev)}</span></td>
      </tr>
    `).join('')
    : `<tr><td colspan="5" style="padding:16px;color:var(--text-2)">
        No activity yet. Logs appear after first connection event.
      </td></tr>`;
}

/* ── Misc UI helpers ────────────────────────────────────── */
function closeModal(id)        { document.getElementById(id)?.classList.remove('open'); }
function showNotifications()   { document.getElementById('notif-panel')?.classList.toggle('open'); }
function handleConnectDevice() { if (typeof openAddDeviceModal === 'function') openAddDeviceModal(); }
function toggleDir(el)         { el.classList.toggle('expanded'); }
function addScreen()           { showToast('Screen creation comes from the Supabase invite flow.', 'info'); closeModal('modal-add-screen'); }
function openAddFileModal()    { showToast('File uploads activate when storage integration is connected.', 'info'); }
function openAddScreenModal()  { document.getElementById('modal-add-screen')?.classList.add('open'); }

function selectDir(el, dir) {
  document.querySelectorAll('.dir-item').forEach((i) => i.classList.remove('active'));
  el.classList.add('active');
  const dirEl = document.getElementById('current-dir');
  if (dirEl) dirEl.textContent = dir;
}

function filterScreens(query) {
  renderScreensTable(
    screens.filter((s) => [s.name, s.location, s.status].some((v) => v.toLowerCase().includes(query.toLowerCase())))
  );
}

/* ── Toast ──────────────────────────────────────────────── */
function showToast(msg, type = 'info') {
  let container = document.querySelector('.toast-container');
  if (!container) {
    container = document.createElement('div');
    container.className = 'toast-container';
    document.body.appendChild(container);
  }
  const icons = { success: 'check_circle', error: 'error', info: 'info' };
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `<span class="material-symbols-outlined">${icons[type] || 'info'}</span>${msg}`;
  container.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateX(20px)';
    toast.style.transition = '.3s';
    setTimeout(() => toast.remove(), 300);
  }, 2600);
}

/* ── FAQ chat bot ───────────────────────────────────────── */
function faqReply(question) {
  const q = question.toLowerCase();
  if (q.includes('link') || q.includes('invite')) return 'Click "Add Device" → "Generate Invite Link". Install the .exe on the target computer.';
  if (q.includes('404') || q.includes('supabase'))  return 'A 404 means wrong table endpoint or project URL. The app retries "devices" and "agent_invites" tables.';
  if (q.includes('remote') || q.includes('view'))   return 'Remote View activates once a device status changes from pending to connected (online).';
  if (q.includes('offline') || q.includes('flask')) return `Flask server is at ${getServerUrl()}. Check Render dashboard → Logs for errors. Make sure cors_allowed_origins="*" is set.`;
  if (q.includes('storage'))                        return 'Storage totals load from Supabase fields storage_gb and files_count.';
  return 'I can answer about: setup, invite links, Supabase errors, remote view, storage, and server status.';
}

function toggleFaqChat()     { document.getElementById('faq-chat-panel')?.classList.toggle('open'); }

function appendFaqMessage(text, who) {
  const body = document.getElementById('faq-chat-body');
  if (!body) return;
  const line = document.createElement('div');
  line.className = `faq-chat-msg ${who}`;
  line.textContent = text;
  body.appendChild(line);
  body.scrollTop = body.scrollHeight;
}

function sendFaqMessage() {
  const input = document.getElementById('faq-chat-input');
  if (!input || !input.value.trim()) return;
  const msg = input.value.trim();
  appendFaqMessage(msg, 'user');
  input.value = '';
  setTimeout(() => appendFaqMessage(faqReply(msg), 'bot'), 250);
}

function handleFaqEnter(event) { if (event.key === 'Enter') sendFaqMessage(); }

/* ── Activity log helper ────────────────────────────────── */
function addActivityLog(entry) {
  activityLogs.unshift(entry);
  renderLogsTable(activityLogs);
}

/* ── Main Supabase refresh ──────────────────────────────── */
async function refreshDashboardFromSupabase() {
  if (!window.SessionManager?.CONFIG?.SUPABASE_URL || !window.SessionManager?.CONFIG?.SUPABASE_ANON_KEY) {
    showToast('Supabase config missing. Check config.js and refresh.', 'error');
    return;
  }

  try {
    const { rows } = await DashboardData.loadRows();

    hydrateScreens(rows);
    renderKpis(rows);
    renderUsageTiles(rows);
    renderConnectionOverviews(rows);
    renderStorageSummary(rows);

    if (typeof updateConnectionState === 'function') updateConnectionState(rows);

    if (rows.length) {
      const onlineRows = rows.filter((r) => ['connected', 'online'].includes(String(r.status || '').toLowerCase()));
      if (onlineRows.length) {
        files.splice(0, files.length, {
          name: 'connected_screen_snapshot.png',
          size: '2 MB',
          type: 'img',
          modified: new Date().toLocaleString(),
          locked: false,
        });
        addActivityLog({
          time: new Date().toLocaleTimeString(),
          event: `${onlineRows.length} screen(s) online`,
          screen: 'All Screens',
          user: 'System',
          sev: 'ok',
        });
      }
    }

    renderScreensTable(screens);
    renderRemoteGrid();
    renderFilesTable(files.length ? files : defaultFiles);
    renderLogsTable(activityLogs);

  } catch (error) {
    showToast(`Load error: ${error.message}`, 'error');
    console.error('[app_live] refreshDashboardFromSupabase error:', error);
  }
}

/* ── DOMContentLoaded init ──────────────────────────────── */
document.addEventListener('DOMContentLoaded', async () => {
  // Apply saved theme
  if (typeof applyTheme === 'function') {
    applyTheme(localStorage.getItem('mview-theme') || 'light');
  }

  // Init socket connection to Render
  initSocket();

  // Check auth state
  const authRaw = localStorage.getItem('mview-auth-user');
  let isLoggedIn = false;
  if (authRaw) {
    try { JSON.parse(authRaw); isLoggedIn = true; } catch (e) { /* invalid JSON */ }
  }

  if (isLoggedIn) {
    const userOk = typeof hydrateUserInfo === 'function' ? hydrateUserInfo() : true;
    if (!userOk) {
      // In combined single-file build: show login page, not a redirect
      if (typeof showPage === 'function') showPage('login');
      return;
    }
    if (typeof showPage === 'function') showPage('dashboard');
    if (typeof showSection === 'function') showSection('dashboard-home');
    if (typeof updateConnectionState === 'function') updateConnectionState([]);

    try {
      await refreshDashboardFromSupabase();
    } catch (error) {
      showToast(`Supabase load error: ${error.message}`, 'error');
    }
    setInterval(refreshDashboardFromSupabase, 15000);
  } else {
    // Show landing page (not login — user hasn't tried to sign in yet)
    if (typeof showPage === 'function') showPage('landing');
  }
});
