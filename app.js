/* ═══════════════════════════════════════════
   m view — Application Logic
═══════════════════════════════════════════ */

'use strict';

// ── Data Store ──────────────────────────────
let screens = [
  { id: 1, name: 'Alpha-Station-2024', status: 'online',  location: 'Nairobi HQ · Floor 3',  lastActive: 'Just now',   storage: '4.2 GB', type: 'Standard Display' },
  { id: 2, name: 'Beta-Hub-03',        status: 'online',  location: 'Nairobi HQ · Floor 1',  lastActive: '2 min ago',  storage: '2.8 GB', type: 'Video Wall' },
  { id: 3, name: 'Gamma-Node-07',      status: 'warning', location: 'Mombasa Branch · L2',   lastActive: '15 min ago', storage: '1.1 GB', type: 'Digital Signage' },
  { id: 4, name: 'Delta-Kiosk-01',    status: 'online',  location: 'Kisumu Office · Lobby',  lastActive: '1 hr ago',   storage: '0.9 GB', type: 'Interactive Kiosk' },
  { id: 5, name: 'Epsilon-Wall-02',   status: 'offline', location: 'Eldoret Branch · Main',  lastActive: '3 hr ago',   storage: '3.4 GB', type: 'Video Wall' },
  { id: 6, name: 'Zeta-Panel-09',     status: 'online',  location: 'Nakuru Hub · Floor 2',   lastActive: '5 min ago',  storage: '0.6 GB', type: 'Standard Display' },
];

const files = [
  { name: 'footage_raw_q1.mp4',   size: '10 GB',  type: 'vid', modified: 'Mar 12, 2024 · 14:20', locked: true },
  { name: 'incident_report.docx', size: '450 KB', type: 'doc', modified: 'Mar 11, 2024 · 09:12', locked: true },
  { name: 'entry_thumbnail.png',  size: '2 MB',   type: 'img', modified: 'Mar 10, 2024 · 18:45', locked: false },
  { name: 'config_backup.json',   size: '120 KB', type: 'doc', modified: 'Mar 8,  2024 · 11:30', locked: false },
  { name: 'livestream_feb.mp4',   size: '8.2 GB', type: 'vid', modified: 'Feb 28, 2024 · 22:00', locked: true },
];

const activityLogs = [
  { time: 'Apr 18 · 14:32', event: 'Screen connected',         screen: 'Alpha-Station-2024', user: 'J. Doe',    sev: 'ok' },
  { time: 'Apr 18 · 13:55', event: 'Backup completed',         screen: 'All Systems',        user: 'System',    sev: 'ok' },
  { time: 'Apr 18 · 13:10', event: 'Screen offline detected',  screen: 'Gamma-Node-07',      user: 'System',    sev: 'warn' },
  { time: 'Apr 18 · 12:44', event: 'File uploaded',            screen: 'Beta-Hub-03',        user: 'A. Kamau',  sev: 'info' },
  { time: 'Apr 18 · 11:02', event: 'Unauthorized access attempt', screen: 'Epsilon-Wall-02', user: 'Unknown',   sev: 'critical' },
  { time: 'Apr 18 · 10:17', event: 'Config updated',           screen: 'Delta-Kiosk-01',     user: 'J. Doe',    sev: 'info' },
  { time: 'Apr 18 · 09:45', event: 'Security scan passed',     screen: 'All Systems',        user: 'System',    sev: 'ok' },
  { time: 'Apr 17 · 23:00', event: 'Scheduled restart',        screen: 'Zeta-Panel-09',      user: 'Scheduler', sev: 'info' },
];

// ── Page Navigation ──────────────────────────
function showPage(page) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  const target = document.getElementById('page-' + page);
  if (target) target.classList.add('active');

  if (page === 'dashboard') {
    renderScreensTable(screens);
    renderRemoteGrid();
    renderFilesTable(files);
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

// ── Sidebar Navigation ───────────────────────
function showSection(sectionId) {
  // Deactivate all nav items and sections
  document.querySelectorAll('.nav-item').forEach(i => i.classList.remove('active'));
  document.querySelectorAll('.dash-section').forEach(s => s.classList.remove('active'));

  // Activate matching section
  const section = document.getElementById('section-' + sectionId);
  if (section) section.classList.add('active');

  // Activate matching nav item (match by onclick content)
  document.querySelectorAll('.nav-item').forEach(item => {
    const onclick = item.getAttribute('onclick') || '';
    if (onclick.includes(sectionId)) item.classList.add('active');
  });

  // Update breadcrumb
  const labels = {
    'dashboard-home': 'Dashboard',
    'remote-view': 'Remote View',
    'file-explorer': 'File Explorer',
    'activity-logs': 'Activity Logs',
    'settings': 'Settings',
    'support': 'Support',
  };
  const bc = document.getElementById('breadcrumb-text');
  if (bc) bc.textContent = labels[sectionId] || sectionId;

  // Close notification panel if open
  document.getElementById('notif-panel')?.classList.remove('open');
}

// ── Sidebar Toggle ───────────────────────────
function toggleSidebar() {
  const sidebar = document.getElementById('sidebar');
  const main = document.querySelector('.dash-main');
  if (window.innerWidth <= 768) {
    sidebar.classList.toggle('mobile-open');
  } else {
    sidebar.classList.toggle('collapsed');
    main.classList.toggle('full-width');
  }
}

// ── Screens Table ────────────────────────────
function renderScreensTable(data) {
  const tbody = document.getElementById('screens-tbody');
  if (!tbody) return;
  tbody.innerHTML = data.map(s => `
    <tr>
      <td>
        <div style="font-weight:600">${s.name}</div>
        <div style="font-size:11px;color:var(--on-surface-variant)">${s.type}</div>
      </td>
      <td>
        <span class="status-pill ${s.status}">
          <i></i>${capitalize(s.status)}
        </span>
      </td>
      <td>${s.location}</td>
      <td>${s.lastActive}</td>
      <td>${s.storage}</td>
      <td>
        <button class="action-btn" title="View" onclick="showSection('remote-view')">
          <span class="material-symbols-outlined" style="font-size:18px">visibility</span>
        </button>
        <button class="action-btn" title="Edit" onclick="editScreen(${s.id})">
          <span class="material-symbols-outlined" style="font-size:18px">edit</span>
        </button>
        <button class="action-btn" title="Delete" onclick="deleteScreen(${s.id})" style="color:var(--error)">
          <span class="material-symbols-outlined" style="font-size:18px">delete</span>
        </button>
      </td>
    </tr>
  `).join('');
}

function filterScreens(query) {
  const filtered = screens.filter(s =>
    s.name.toLowerCase().includes(query.toLowerCase()) ||
    s.location.toLowerCase().includes(query.toLowerCase()) ||
    s.status.toLowerCase().includes(query.toLowerCase())
  );
  renderScreensTable(filtered);
}

function deleteScreen(id) {
  screens = screens.filter(s => s.id !== id);
  renderScreensTable(screens);
  renderRemoteGrid();
  showToast('Screen removed', 'success');
}

function editScreen(id) {
  showToast('Edit screen — coming soon', 'info');
}

// ── Remote View Grid ─────────────────────────
function renderRemoteGrid() {
  const grid = document.getElementById('remote-grid');
  if (!grid) return;
  grid.innerHTML = screens.map(s => `
    <div class="remote-card">
      <div class="remote-screen">
        <span class="material-symbols-outlined">monitor</span>
        <div class="remote-screen-overlay">
          <span style="width:5px;height:5px;border-radius:50%;background:${s.status==='online'?'#4ade80':s.status==='warning'?'#fbbf24':'#f87171'}"></span>
          ${capitalize(s.status)}
        </div>
      </div>
      <div class="remote-card-body">
        <div class="remote-card-name">${s.name}</div>
        <div class="remote-card-sub">${s.location}</div>
      </div>
      <div class="remote-card-footer">
        <span class="status-pill ${s.status}"><i></i>${capitalize(s.status)}</span>
        <button class="btn-ghost btn-sm" onclick="showToast('Connecting to ${s.name}…','info')">
          <span class="material-symbols-outlined" style="font-size:14px">open_in_new</span>
          Connect
        </button>
      </div>
    </div>
  `).join('');
}

// ── Files Table ──────────────────────────────
function renderFilesTable(data) {
  const tbody = document.getElementById('file-tbody');
  if (!tbody) return;
  const iconMap = { vid: 'videocam', doc: 'description', img: 'image' };
  tbody.innerHTML = data.map(f => `
    <tr>
      <td>
        <div class="file-icon-wrap">
          ${f.locked ? `<span class="material-symbols-outlined file-lock">lock</span>` : ''}
          <div class="file-icon ${f.type}">
            <span class="material-symbols-outlined" style="font-size:14px">${iconMap[f.type]}</span>
          </div>
          <span style="font-weight:500">${f.name}</span>
        </div>
      </td>
      <td>${f.size}</td>
      <td style="text-transform:uppercase;font-size:10px;letter-spacing:.06em;color:var(--on-surface-variant)">${f.type}</td>
      <td style="color:var(--on-surface-variant)">${f.modified}</td>
      <td>
        <button class="action-btn" title="Download" onclick="showToast('Downloading ${f.name}','success')">
          <span class="material-symbols-outlined" style="font-size:16px">download</span>
        </button>
        <button class="action-btn" title="Delete" onclick="showToast('File removed','success')">
          <span class="material-symbols-outlined" style="font-size:16px">delete</span>
        </button>
      </td>
    </tr>
  `).join('');
}

function selectDir(el, dir) {
  document.querySelectorAll('.dir-item').forEach(i => i.classList.remove('active'));
  el.classList.add('active');
  const dirEl = document.getElementById('current-dir');
  if (dirEl) dirEl.textContent = dir;
}

function toggleDir(el) {
  el.classList.toggle('expanded');
}

function openAddFileModal() {
  showToast('Upload feature — coming soon', 'info');
}

function handleConnectDevice() {
  openAddDeviceModal();
}

// ── Logs Table ───────────────────────────────
function renderLogsTable(data) {
  const tbody = document.getElementById('logs-tbody');
  if (!tbody) return;
  tbody.innerHTML = data.map(l => `
    <tr>
      <td style="color:var(--on-surface-variant);font-size:12px;white-space:nowrap">${l.time}</td>
      <td style="font-weight:500">${l.event}</td>
      <td>${l.screen}</td>
      <td>${l.user}</td>
      <td><span class="log-severity ${l.sev}">${capitalize(l.sev)}</span></td>
    </tr>
  `).join('');
}

// ── Add Screen Modal ─────────────────────────
function openAddScreenModal() {
  document.getElementById('modal-add-screen').classList.add('open');
}

function closeModal(id) {
  document.getElementById(id).classList.remove('open');
}

function addScreen() {
  const name    = document.getElementById('new-screen-name').value.trim();
  const loc     = document.getElementById('new-screen-location').value.trim();
  const type    = document.getElementById('new-screen-type').value;
  const status  = document.getElementById('new-screen-status').value;

  if (!name) { showToast('Please enter a screen name', 'error'); return; }

  screens.push({
    id: Date.now(),
    name, location: loc || 'Unknown Location',
    type, status, lastActive: 'Just now', storage: '0 GB',
  });

  closeModal('modal-add-screen');
  renderScreensTable(screens);
  renderRemoteGrid();

  // Clear form
  document.getElementById('new-screen-name').value = '';
  document.getElementById('new-screen-location').value = '';

  showToast(`${name} added successfully`, 'success');
}

// ── Notifications ────────────────────────────
function showNotifications() {
  document.getElementById('notif-panel').classList.toggle('open');
}

// ── Toast ────────────────────────────────────
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
  setTimeout(() => { toast.style.opacity = '0'; toast.style.transform = 'translateX(20px)'; toast.style.transition = '.3s'; setTimeout(() => toast.remove(), 300); }, 3000);
}

// ── Nav Scroll Effect ────────────────────────
window.addEventListener('scroll', () => {
  const nav = document.getElementById('main-nav');
  if (nav) nav.classList.toggle('scrolled', window.scrollY > 20);
});

// ── Keyboard Shortcuts ───────────────────────
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    document.querySelectorAll('.modal-overlay.open').forEach(m => m.classList.remove('open'));
    document.getElementById('notif-panel')?.classList.remove('open');
  }
});

// ── Click outside to close notif ────────────
document.addEventListener('click', e => {
  const panel = document.getElementById('notif-panel');
  if (panel?.classList.contains('open') && !panel.contains(e.target) && !e.target.closest('.icon-btn')) {
    panel.classList.remove('open');
  }
});

// ── Utility ──────────────────────────────────
function capitalize(str) { return str.charAt(0).toUpperCase() + str.slice(1); }

// ── Init ─────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  const savedTheme = localStorage.getItem('mview-theme') || 'light';
  applyTheme(savedTheme);
  if (window.location.hash === '#dashboard') {
    showPage('dashboard');
    showSection('dashboard-home');
  } else {
    showPage('landing');
  }
});

// ── Activity Log Helper (called by sessionManager) ──────────
function addActivityLog(entry) {
  activityLogs.unshift(entry);
  renderLogsTable(activityLogs);
}
