/**
 * m view — Global Config
 * Production: pointing to Render deployment.
 *
 * NOTE: On Render, the server is exposed as HTTPS on port 443 (standard).
 * Do NOT add :10000 — Render handles port mapping internally.
 * Just use the plain https:// URL as shown below.
 */
window.MVIEW_SERVER_URL = 'https://screen-connect-rtca.onrender.com';

// ── Supabase credentials ───────────────────────────────────
window.MVIEW_SUPABASE_URL      = 'https://iacdzpcoftxxcoigopun.supabase.co';
window.MVIEW_SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlhY2R6cGNvZnR4eGNvaWdvcHVuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY0MjA1NTUsImV4cCI6MjA5MTk5NjU1NX0.5Eo21XrLTWL3RyKmuvJPdaS-NssraDMyAxVMFy-F054';

// ── SessionManager config bridge ───────────────────────────
// This lets session_manager.js and app_live.js pick up the values
// via window.SessionManager.CONFIG without any extra changes.
window.SessionManager = window.SessionManager || {};
window.SessionManager.CONFIG = window.SessionManager.CONFIG || {};
window.SessionManager.CONFIG.SERVER_URL      = window.MVIEW_SERVER_URL;
window.SessionManager.CONFIG.SUPABASE_URL    = window.MVIEW_SUPABASE_URL;
window.SessionManager.CONFIG.SUPABASE_ANON_KEY = window.MVIEW_SUPABASE_ANON_KEY;
