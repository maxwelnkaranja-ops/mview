'use strict';

let screens = [];
const files = [];
const activityLogs = [];

const defaultFiles = [
  { name: 'No files yet', size: '0 KB', type: 'doc', modified: 'No sync events yet', locked: false },
];

const DashboardData = (() => {
  const config = () => (window.SessionManager?.CONFIG || {});
  const supabaseBaseUrl = () => String(config().SUPABASE_URL || '').trim().replace(/\/+$/, '').replace(/\/rest\/v1$/i, '');

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
  if (raw === 'pending' || raw === 'downloading') return 'offline';
  return 'offline';
}

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

function renderKpis(rows) {
  const total = rows.length;
  const online = rows.filter((r) => ['connected', 'online'].includes(String(r.status || '').toLowerCase())).length;
  const alerts = rows.filter((r) => ['warning', 'warn', 'offline', 'error'].includes(String(r.status || '').toLowerCase())).length;
  const storage = rows.reduce((sum, r) => sum + (Number(r.storage_gb) || 0), 0);
  const k = (id, value) => { const el = document.getElementById(id); if (el) el.textContent = value; };
  const ks = (id, value) => { const el = document.getElementById(id); if (el) el.innerHTML = value; };

  k('kpi-total-screens', String(total));
  ks('kpi-total-screens-sub', `<span class="material-symbols-outlined">monitor</span> live from Supabase`);
  k('kpi-online-now', String(online));
  ks('kpi-online-now-sub', `<span class="material-symbols-outlined">wifi</span> ${Math.round((online / Math.max(total, 1)) * 100)}% online`);
  k('kpi-alerts', String(alerts));
  ks('kpi-alerts-sub', `<span class="material-symbols-outlined">warning</span> live issue count`);
  k('kpi-storage-used', toGB(storage));
  ks('kpi-storage-used-sub', `<span class="material-symbols-outlined">storage</span> usage from Supabase`);
}

function renderUsageTiles(rows) {
  const from = (field) => rows.reduce((sum, r) => sum + (Number(r[field]) || 0), 0);
  const totals = {
    yt: from('youtube_minutes'),
    ch: from('chrome_minutes'),
    tt: from('tiktok_minutes'),
    ig: from('instagram_minutes'),
  };
  const sum = Math.max(1, totals.yt + totals.ch + totals.tt + totals.ig);
  const pairs = [
    ['youtube', totals.yt],
    ['chrome', totals.ch],
    ['tiktok', totals.tt],
    ['instagram', totals.ig],
  ];
  pairs.forEach(([key, mins]) => {
    const pct = Math.round((mins / sum) * 100);
    const timeEl = document.getElementById(`usage-${key}-time`);
    const pctEl = document.getElementById(`usage-${key}-pct`);
    const barEl = document.getElementById(`usage-${key}-bar`);
    if (timeEl) timeEl.textContent = `${Math.round(mins)} min`;
    if (pctEl) pctEl.textContent = `${pct}%`;
    if (barEl) barEl.style.width = `${pct}%`;
  });
  const highest = Math.max(totals.yt, totals.ch, totals.tt, totals.ig, 1);
  const toHeight = (v) => `${Math.max(6, Math.round((v / highest) * 100))}%`;
  const map = [
    ['ua-yt', totals.yt],
    ['ua-ch', totals.ch],
    ['ua-tt', totals.tt],
    ['ua-ig', totals.ig],
  ];
  map.forEach(([id, v]) => {
    const el = document.getElementById(id);
    if (el) el.style.height = toHeight(v);
  });
}

function renderConnectionOverviews(rows) {
  const pending = rows.filter((r) => ['pending', 'downloading'].includes(String(r.status || '').toLowerCase()));
  const connected = rows.filter((r) => ['connected', 'online'].includes(String(r.status || '').toLowerCase()));
  const text = pending.length
    ? `${pending.length} link(s) are establishing connection. Connected: ${connected.length}.`
    : connected.length
      ? `Connected screens: ${connected.length}. No links currently pending.`
      : 'No active links yet. Generate a link to start establishing a screen connection.';
  const conn = document.getElementById('connection-overview');
  if (conn) conn.textContent = text;

  const explorerSub = document.getElementById('explorer-status-sub');
  if (explorerSub) explorerSub.textContent = connected.length
    ? `Connected screens: ${connected.length} · Files loading from Supabase`
    : 'Connected screens: 0 · Files loading from Supabase';
}

function renderStorageSummary(rows) {
  const totalGb = rows.reduce((sum, r) => sum + (Number(r.storage_gb) || 0), 0);
  const fileCount = rows.reduce((sum, r) => sum + (Number(r.files_count) || 0), 0);
  const maxGb = 20;
  const pct = Math.min(100, Math.round((totalGb / maxGb) * 100));

  const usageMain = document.getElementById('storage-usage-main');
  if (usageMain) usageMain.textContent = `${toGB(totalGb)} / ${maxGb} GB`;
  const filesCount = document.getElementById('storage-files-count');
  if (filesCount) filesCount.textContent = `${fileCount} items`;
  const totalSize = document.getElementById('storage-total-size');
  if (totalSize) totalSize.textContent = toGB(totalGb);
  const fill = document.querySelector('.storage-fill');
  if (fill) fill.style.width = rows.length ? `${pct}%` : '0%';
}

function hydrateUserInfo() {
  const raw = localStorage.getItem('mview-auth-user');
  if (!raw) return false;
  try {
    const user = JSON.parse(raw);
    const name = user.name || user.email || 'm view User';
    const initials = name.split(' ').map((part) => part[0]).filter(Boolean).slice(0, 2).join('').toUpperCase() || 'MV';
    const sidebarName = document.getElementById('sidebar-user-name');
    if (sidebarName) sidebarName.textContent = name;
    const sideAvatar = document.getElementById('sidebar-user-avatar');
    if (sideAvatar) sideAvatar.textContent = initials;
    const topAvatar = document.getElementById('topbar-user-avatar');
    if (topAvatar) topAvatar.textContent = initials;
    const settingsName = document.getElementById('settings-full-name');
    if (settingsName) settingsName.value = name;
    return true;
  } catch (error) {
    return false;
  }
}

function goToLogin() {
  window.location.href = 'login.html';
}

function showPage(page) {
  document.querySelectorAll('.page').forEach((p) => p.classList.remove('active'));
  const target = document.getElementById(`page-${page}`);
  if (target) target.classList.add('active');
  if (page === 'dashboard') {
    renderScreensTable(screens);
    renderRemoteGrid();
    renderFilesTable(files.length ? files : defaultFiles);
    renderLogsTable(activityLogs);
  }
  window.scrollTo(0, 0);
}

function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  localStorage.setItem('mview-theme', theme);
  const icon = document.getElementById('theme-icon');
  const label = document.getElementById('theme-label');
  if (icon) icon.textContent = theme === 'dark' ? 'light_mode' : 'dark_mode';
  if (label) label.textContent = theme === 'dark' ? 'Light' : 'Dark';
}

function toggleTheme() {
  const current = document.documentElement.getAttribute('data-theme') || 'light';
  applyTheme(current === 'light' ? 'dark' : 'light');
}

function showSection(sectionId) {
  document.querySelectorAll('.nav-item').forEach((i) => i.classList.remove('active'));
  document.querySelectorAll('.dash-section').forEach((s) => s.classList.remove('active'));
  const section = document.getElementById(`section-${sectionId}`);
  if (section) section.classList.add('active');
  document.querySelectorAll('.nav-item').forEach((item) => {
    if ((item.getAttribute('onclick') || '').includes(sectionId)) item.classList.add('active');
  });
  const bc = document.getElementById('breadcrumb-text');
  if (bc) bc.textContent = ({
    'dashboard-home': 'Dashboard',
    'remote-view': 'Remote View',
    'file-explorer': 'File Explorer',
    'activity-logs': 'Activity Logs',
    settings: 'Settings',
    support: 'Support',
  })[sectionId] || sectionId;
}

function toggleSidebar() {
  const sidebar = document.getElementById('sidebar');
  const main = document.querySelector('.dash-main');
  if (window.innerWidth <= 768) sidebar.classList.toggle('mobile-open');
  else {
    sidebar.classList.toggle('collapsed');
    main.classList.toggle('full-width');
  }
}

function renderScreensTable(data) {
  const tbody = document.getElementById('screens-tbody');
  if (!tbody) return;
  tbody.innerHTML = data.length ? data.map((s) => `
    <tr>
      <td><div style="font-weight:600">${s.name}</div><div style="font-size:11px;color:var(--on-surface-variant)">${s.type}</div></td>
      <td><span class="status-pill ${s.status}"><i></i>${capitalize(s.status)}</span></td>
      <td>${s.location}</td><td>${s.lastActive}</td><td>${s.storage}</td>
      <td><button class="action-btn" onclick="showSection('remote-view')"><span class="material-symbols-outlined" style="font-size:18px">visibility</span></button></td>
    </tr>
  `).join('') : `<tr><td colspan="6" style="padding:18px;color:var(--on-surface-variant)">No connected screens yet. Generate a link and wait for device check-in.</td></tr>`;
}

function renderRemoteGrid() {
  const grid = document.getElementById('remote-grid');
  if (!grid) return;
  grid.innerHTML = screens.length ? screens.map((s) => `
    <div class="remote-card">
      <div class="remote-screen">
        <span class="material-symbols-outlined">monitor</span>
        <div class="remote-screen-overlay"><span style="width:5px;height:5px;border-radius:50%;background:${s.status === 'online' ? '#4ade80' : s.status === 'warning' ? '#fbbf24' : '#f87171'}"></span>${capitalize(s.status)}</div>
      </div>
      <div class="remote-card-body">
        <div class="remote-card-name">${s.name}</div>
        <div class="remote-card-sub">${s.location} · ${s.rawStatus === 'pending' ? 'Establishing connection' : 'Connected screen'}</div>
      </div>
      <div class="remote-card-footer">
        <span class="status-pill ${s.status}"><i></i>${capitalize(s.status)}</span>
        <button class="btn-ghost btn-sm" onclick="showToast('${s.rawStatus === 'pending' ? 'Connection still establishing' : `Connecting to ${s.name}...`}', 'info')">
          <span class="material-symbols-outlined" style="font-size:14px">open_in_new</span>${s.rawStatus === 'pending' ? 'Pending' : 'Connect'}
        </button>
      </div>
    </div>
  `).join('') : '<div class="connection-overview pulse-loading">No connected screens yet. Start by generating a device link.</div>';
}

function renderFilesTable(data) {
  const tbody = document.getElementById('file-tbody');
  if (!tbody) return;
  const iconMap = { vid: 'videocam', doc: 'description', img: 'image' };
  tbody.innerHTML = data.map((f) => `
    <tr><td><div class="file-icon-wrap"><div class="file-icon ${f.type}"><span class="material-symbols-outlined" style="font-size:14px">${iconMap[f.type] || 'description'}</span></div><span style="font-weight:500">${f.name}</span></div></td>
    <td>${f.size}</td><td style="text-transform:uppercase;font-size:10px;letter-spacing:.06em;color:var(--on-surface-variant)">${f.type}</td><td style="color:var(--on-surface-variant)">${f.modified}</td>
    <td><button class="action-btn"><span class="material-symbols-outlined" style="font-size:16px">download</span></button></td></tr>
  `).join('');
}

function renderLogsTable(data) {
  const tbody = document.getElementById('logs-tbody');
  if (!tbody) return;
  tbody.innerHTML = data.length ? data.map((l) => `
    <tr><td style="color:var(--on-surface-variant);font-size:12px;white-space:nowrap">${l.time}</td><td style="font-weight:500">${l.event}</td><td>${l.screen}</td><td>${l.user}</td><td><span class="log-severity ${l.sev}">${capitalize(l.sev)}</span></td></tr>
  `).join('') : '<tr><td colspan="5" style="padding:16px;color:var(--on-surface-variant)">No activity yet. Logs will appear after first connection event.</td></tr>';
}

function openAddScreenModal() { document.getElementById('modal-add-screen').classList.add('open'); }
function closeModal(id) { document.getElementById(id).classList.remove('open'); }
function addScreen() { showToast('Screen creation now comes from Supabase invite flow.', 'info'); closeModal('modal-add-screen'); }
function showNotifications() { document.getElementById('notif-panel').classList.toggle('open'); }
function handleConnectDevice() { openAddDeviceModal(); }
function toggleDir(el) { el.classList.toggle('expanded'); }
function selectDir(el, dir) { document.querySelectorAll('.dir-item').forEach((i) => i.classList.remove('active')); el.classList.add('active'); const dirEl = document.getElementById('current-dir'); if (dirEl) dirEl.textContent = dir; }
function openAddFileModal() { showToast('File uploads will activate when storage integration is connected.', 'info'); }
function filterScreens(query) { renderScreensTable(screens.filter((s) => [s.name, s.location, s.status].some((v) => v.toLowerCase().includes(query.toLowerCase())))); }

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

function capitalize(str) { return str ? str.charAt(0).toUpperCase() + str.slice(1) : ''; }
function addActivityLog(entry) { activityLogs.unshift(entry); renderLogsTable(activityLogs); }

function faqReply(question) {
  const q = question.toLowerCase();
  if (q.includes('link') || q.includes('invite')) return 'Use Add Device, then Generate Invite Link. If insert fails, verify Supabase URL and table endpoint.';
  if (q.includes('404') || q.includes('supabase')) return 'A 404 usually means wrong table endpoint or project URL. This app now retries both devices and agent_invites tables.';
  if (q.includes('remote')) return 'Remote View updates after the device session status changes from pending to connected.';
  if (q.includes('storage')) return 'Storage totals load from Supabase fields like storage_gb and files_count. If fields are missing, values stay at 0.';
  return 'I can answer setup, invite links, Supabase errors, remote view, and dashboard metrics.';
}

function toggleFaqChat() {
  document.getElementById('faq-chat-panel')?.classList.toggle('open');
}

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

function handleFaqEnter(event) {
  if (event.key === 'Enter') sendFaqMessage();
}

async function refreshDashboardFromSupabase() {
  if (!window.SessionManager?.CONFIG?.SUPABASE_URL || !window.SessionManager?.CONFIG?.SUPABASE_ANON_KEY) {
    showToast('Supabase config missing. Add values to config.js and refresh.', 'error');
    return;
  }

  const { rows } = await DashboardData.loadRows();
  hydrateScreens(rows);
  renderKpis(rows);
  renderUsageTiles(rows);
  renderConnectionOverviews(rows);
  renderStorageSummary(rows);

  if (rows.length) {
    files.splice(0, files.length, {
      name: 'connected_screen_snapshot.png',
      size: '2 MB',
      type: 'img',
      modified: new Date().toLocaleString(),
      locked: false,
    });
    addActivityLog({
      time: new Date().toLocaleString(),
      event: 'Dashboard synced from Supabase',
      screen: 'All Screens',
      user: 'System',
      sev: 'ok',
    });
  }

  renderScreensTable(screens);
  renderRemoteGrid();
  renderFilesTable(files.length ? files : defaultFiles);
  renderLogsTable(activityLogs);
}

document.addEventListener('DOMContentLoaded', async () => {
  applyTheme(localStorage.getItem('mview-theme') || 'light');
  if (window.location.hash === '#dashboard') {
    if (!hydrateUserInfo()) {
      window.location.href = 'login.html';
      return;
    }
    showPage('dashboard');
    showSection('dashboard-home');
    try {
      await refreshDashboardFromSupabase();
    } catch (error) {
      showToast(`Supabase load error: ${error.message}`, 'error');
    }
    setInterval(refreshDashboardFromSupabase, 15000);
  } else {
    showPage('landing');
  }
});
