/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║  m view — Session Manager  v4.0                              ║
 * ║  Device onboarding · invite link generation · live polling   ║
 * ╚══════════════════════════════════════════════════════════════╝
 *
 *  ROOT CAUSE OF THE 400 ERROR (now fixed):
 *   1. Payload sent columns that don't exist in Supabase yet
 *      (agent_version, ip_address, os_info — filled LATER by the agent)
 *   2. SERVER_BASE_URL was still 'https://your-server.com' placeholder
 *   3. Token regex on server was [A-F] only but JS crypto returns lowercase
 *
 *  YOUR SUPABASE CREDENTIALS ARE ALREADY SET BELOW.
 *  Your server URL needs to match your machine's IP.
 */

const MVIEW_CONFIG = {
  SERVER_BASE_URL:   window.MVIEW_SERVER_URL || 'http://192.168.0.101:5000',
  SUPABASE_URL:      'https://iacdzpcoftxxcoigopun.supabase.co',
  SUPABASE_ANON_KEY: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlhY2R6cGNvZnR4eGNvaWdvcHVuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY0MjA1NTUsImV4cCI6MjA5MTk5NjU1NX0.5Eo21XrLTWL3RyKmuvJPdaS-NssraDMyAxVMFy-F054',
  POLL_INTERVAL_MS:  3500,
  POLL_TIMEOUT_MS:   600000,
  TABLE_NAME:        'devices',
};

const SupabaseDB = (() => {
  const { SUPABASE_URL, SUPABASE_ANON_KEY } = MVIEW_CONFIG;
  const h = {
    'Content-Type':  'application/json',
    'apikey':        SUPABASE_ANON_KEY,
    'Authorization': `Bearer ${SUPABASE_ANON_KEY}`,
  };
  return {
    async insert(table, payload) {
      const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
        method: 'POST', headers: { ...h, 'Prefer': 'return=representation' },
        body: JSON.stringify(payload),
      });
      const text = await res.text();
      if (!res.ok) {
        let msg = text;
        try { msg = JSON.parse(text)?.message || msg; } catch (_) {}
        throw new Error(`Supabase INSERT ${res.status}: ${msg}`);
      }
      return text ? JSON.parse(text) : [];
    },
    async select(table, filters = {}) {
      const params = new URLSearchParams(filters);
      const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}?${params}`, { headers: h });
      if (!res.ok) throw new Error(`Supabase SELECT ${res.status}`);
      return res.json();
    },
    async patch(table, filterKey, filterVal, payload) {
      const params = new URLSearchParams({ [filterKey]: `eq.${filterVal}` });
      const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}?${params}`, {
        method: 'PATCH', headers: { ...h, 'Prefer': 'return=minimal' },
        body: JSON.stringify(payload),
      });
      if (!res.ok) throw new Error(`Supabase PATCH ${res.status}`);
      return true;
    },
  };
})();

const SessionManager = (() => {
  let _s = { token: null, label: '', location: '', type: '', expirySecs: 86400, serverUrl: '', pollTimer: null, pollStart: null, supabaseOk: false };

  function generateToken() {
    const bytes = new Uint8Array(9);
    try { crypto.getRandomValues(bytes); }
    catch (_) { for (let i = 0; i < 9; i++) bytes[i] = Math.floor(Math.random() * 256); }
    const hex = Array.from(bytes).map(b => b.toString(16).padStart(2,'0').toUpperCase()).join('');
    return `MV-${hex.slice(0,6)}-${hex.slice(6,12)}-${hex.slice(12,18)}`;
  }

  function expiryLabel(s) { return {3600:'1 hour',86400:'24 hours',604800:'7 days',0:'Never'}[s]||`${s}s`; }
  function expiresAt(s)   { return s===0 ? null : new Date(Date.now()+s*1000).toISOString(); }
  function wait(ms)       { return new Promise(r=>setTimeout(r,ms)); }
  function now()          { return new Date().toLocaleString(); }

  function step(n) {
    document.querySelectorAll('.device-step').forEach((el,i)=>el.classList.toggle('active',i+1===n));
  }
  function genStatus(t) { const el=document.getElementById('gen-status-text'); if(el) el.textContent=t; }
  function pollStatus(t, state) {
    const dot=document.getElementById('poll-dot'), span=document.getElementById('poll-status-text');
    if(span) span.textContent=t;
    if(dot) { dot.className='poll-dot'; if(state==='waiting') dot.classList.add('pulsing'); if(state==='connected') dot.classList.add('connected'); }
  }

  function populateUI(token, link, expSecs, type) {
    const set=(id,v)=>{const e=document.getElementById(id);if(e)e.textContent=v;};
    const val=(id,v)=>{const e=document.getElementById(id);if(e)e.value=v;};
    set('display-token', token);
    set('display-expiry', expSecs===0?'No expiry':`Expires in ${expiryLabel(expSecs)}`);
    set('meta-expiry', expiryLabel(expSecs));
    set('meta-type', type);
    val('copy-link-input', link);
    const dl=document.getElementById('direct-download-btn');
    if(dl){dl.href=link;dl.download=`agent_${token}.pdf`;}
  }

  async function generateInviteLink() {
    const label     = (document.getElementById('dev-label')?.value||'').trim();
    const location  = (document.getElementById('dev-location')?.value||'').trim();
    const type      = document.getElementById('dev-type')?.value||'Standard Display';
    const expSecs   = parseInt(document.getElementById('dev-expiry')?.value||'86400',10);
    const serverUrl = (document.getElementById('dev-server-url')?.value||'').trim() || MVIEW_CONFIG.SERVER_BASE_URL;

    if(!label){ toast('Please enter a device label','error'); document.getElementById('dev-label')?.focus(); return; }

    Object.assign(_s,{label,location,type,expirySecs:expSecs,serverUrl});
    step(2);

    try {
      genStatus('Generating cryptographic token…'); await wait(350);
      const token = generateToken();
      _s.token = token;

      genStatus('Registering session in Supabase…'); await wait(250);

      /* ── FIXED: only send columns that exist in the schema ──
         Do NOT send: agent_version, ip_address, os_info
         Those are filled later by the agent on check-in.        */
      const payload = {
        device_id:   token,
        label:       label,
        location:    location || null,
        device_type: type,
        status:      'pending',
        expires_at:  expiresAt(expSecs),
        created_at:  new Date().toISOString(),
      };

      try {
        await SupabaseDB.insert(MVIEW_CONFIG.TABLE_NAME, payload);
        _s.supabaseOk = true;
        genStatus('Session registered in Supabase ✓');
      } catch(dbErr) {
        console.warn('[SessionManager] Supabase insert failed:', dbErr.message);
        genStatus(`Offline mode active — ${dbErr.message}`);
        _s.supabaseOk = false;
      }

      await wait(400);
      genStatus('Building secure invite link…'); await wait(250);

      const link = `${serverUrl}/invite/${token}`;
      populateUI(token, link, expSecs, type);
      step(3);

      if(_s.supabaseOk) startPolling(token);
      else pollStatus('Supabase not reached — check table exists (see SQL below)', 'waiting');

      if(typeof addActivityLog==='function')
        addActivityLog({time:now(),event:'Invite link generated',screen:label,user:'Admin',sev:'info'});

    } catch(err) {
      console.error('[SessionManager]', err);
      step(1);
      toast(`Link generation failed: ${err.message}`,'error');
    }
  }

  function startPolling(token) {
    stopPolling();
    _s.pollStart = Date.now();
    pollStatus('Waiting for device to connect…', 'waiting');

    _s.pollTimer = setInterval(async () => {
      if(Date.now()-_s.pollStart > MVIEW_CONFIG.POLL_TIMEOUT_MS) {
        stopPolling(); pollStatus('Timed out — no connection in 10 minutes.',null); return;
      }
      try {
        const rows = await SupabaseDB.select(MVIEW_CONFIG.TABLE_NAME, {
          device_id: `eq.${token}`,
          select: 'status,ip_address,os_info,agent_version,hostname',
        });
        if(!rows?.length) return;
        const row = rows[0];
        if(row.status==='connected')       { stopPolling(); onConnected(token,row); }
        else if(row.status==='downloading'){ pollStatus('Agent downloading on target machine…','waiting'); }
        else if(row.status==='rejected'||row.status==='revoked') {
          stopPolling(); pollStatus(`Session ${row.status}.`,null); toast(`Session was ${row.status}.`,'error');
        }
      } catch(err) { console.warn('[SessionManager] Poll error:',err.message); }
    }, MVIEW_CONFIG.POLL_INTERVAL_MS);
  }

  function stopPolling() { clearInterval(_s.pollTimer); _s.pollTimer=null; }

  function onConnected(token, row) {
    const ip=row.ip_address||'unknown', os=row.os_info||'unknown OS', ver=row.agent_version||'?';
    pollStatus(`✓ Connected  ·  IP: ${ip}  ·  ${os}  ·  Agent v${ver}`,'connected');
    toast(`${_s.label} is now online!`,'success');

    if(typeof screens!=='undefined'&&typeof renderScreensTable==='function') {
      screens.push({id:token,name:_s.label,location:_s.location||'Unknown',type:_s.type,status:'online',lastActive:'Just now',storage:'0 GB',ip,os});
      renderScreensTable(screens);
      if(typeof renderRemoteGrid==='function') renderRemoteGrid();
    }
    if(typeof updateKPIs==='function') updateKPIs();
    if(typeof addActivityLog==='function')
      addActivityLog({time:now(),event:'Device connected via invite',screen:_s.label,user:'System',sev:'ok'});
  }

  async function copyLink() {
    const input=document.getElementById('copy-link-input');
    const icon=document.getElementById('copy-icon'), btn=document.getElementById('copy-btn');
    if(!input?.value) return;
    try { await navigator.clipboard.writeText(input.value); }
    catch(_) { input.select(); document.execCommand('copy'); }
    if(icon) icon.textContent='check';
    if(btn)  btn.style.background='#16a34a';
    toast('Link copied to clipboard','success');
    setTimeout(()=>{ if(icon) icon.textContent='content_copy'; if(btn) btn.style.background=''; },2000);
  }

  function reset() {
    stopPolling(); _s.token=null;
    ['dev-label','dev-location','dev-server-url'].forEach(id=>{ const e=document.getElementById(id); if(e) e.value=''; });
    step(1);
  }

  function toast(msg,type='info') {
    if(typeof window.showToast==='function'){window.showToast(msg,type);return;}
    let c=document.querySelector('.toast-container');
    if(!c){c=document.createElement('div');c.className='toast-container';document.body.appendChild(c);}
    const icons={success:'check_circle',error:'error',info:'info'};
    const t=document.createElement('div');
    t.className=`toast ${type}`;
    t.innerHTML=`<span class="material-symbols-outlined">${icons[type]||'info'}</span>${msg}`;
    c.appendChild(t);
    setTimeout(()=>{t.style.cssText='opacity:0;transform:translateX(20px);transition:.3s';setTimeout(()=>t.remove(),300);},3500);
  }

  return { generateInviteLink, copyLink, reset, stopPolling };
})();

function openAddDeviceModal() {
  if(typeof SessionManager!=='undefined') SessionManager.reset();
  const m=document.getElementById('modal-add-device');
  if(m) m.classList.add('open');
}
