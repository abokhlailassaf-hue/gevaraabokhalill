// ===================== سيرفر الديوان العسكري (بناء جديد) =====================
// يوفر: تقديم الواجهة، تخزين دائم فعلي (SQLite على قرص دائم)، ومزامنة فورية
// بين كل الأجهزة المتصلة عبر WebSocket (بدون أي استطلاع دوري / polling).
//
// المبدأ: أي تعديل من أي جهاز -> يُحفظ على القرص أولاً (قبل أي رد) -> ثم يُبث
// فوراً لكل الأجهزة الأخرى المتصلة حالياً. لا يوجد "لقطة كاملة قديمة" تكتب
// فوق تعديلات حديثة؛ كل مفتاح حالة له رقم مراجعة يزيد باستمرار، وكل تعديل
// يُسجَّل بشكل دائم في history (راجع db.js) فلا يضيع أي تعديل مهما حصل.

require('dotenv').config();

const path = require('path');
const fs = require('fs');
const express = require('express');
const http = require('http');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const basicAuth = require('express-basic-auth');
const { WebSocketServer } = require('ws');
const db = require('./db');

const PORT = process.env.PORT || 3000;

// ----- مصادقة الخادم -----
// كل بيانات مرور صحيحة تمنح الوصول نفسه؛ لا أدوار admin/viewer داخل الخادم.
function getBasicAuthUsers() {
  if (process.env.BASIC_AUTH_USERS_JSON) {
    try {
      const users = JSON.parse(process.env.BASIC_AUTH_USERS_JSON);
      if (users && typeof users === 'object' && Object.keys(users).length) return users;
    } catch (_) {}
  }
  const users = {};
  if (process.env.BASIC_AUTH_USER && process.env.BASIC_AUTH_PASSWORD) users[process.env.BASIC_AUTH_USER] = process.env.BASIC_AUTH_PASSWORD;
  return users;
}
const basicAuthUsers = getBasicAuthUsers();
if (!Object.keys(basicAuthUsers).length) {
  console.error('❌ يجب ضبط BASIC_AUTH_USER/BASIC_AUTH_PASSWORD أو BASIC_AUTH_USERS_JSON قبل التشغيل.');
  process.exit(1);
}
function checkBasicAuthHeader(authorizationHeader) {
  if (!authorizationHeader || !authorizationHeader.startsWith('Basic ')) return null;
  try {
    const decoded = Buffer.from(authorizationHeader.slice(6), 'base64').toString('utf8');
    const idx = decoded.indexOf(':');
    if (idx < 0) return null;
    const user = decoded.slice(0, idx);
    const pass = decoded.slice(idx + 1);
    return basicAuthUsers[user] === pass ? user : null;
  } catch (_) {
    return null;
  }
}

// ----- تحذير مبكر واضح إن لم يكن هناك قرص دائم -----
// السبب الأشهر لـ"رجوع البيانات للنسخة الأصلية بعد كل تحديث" هو أن Railway
// يمسح القرص المحلي عند كل إعادة نشر ما لم يكن مساراً داخل Volume دائم.
if (!process.env.DATA_DIR) {
  console.warn('⚠️  DATA_DIR غير مضبوط — التخزين سيكون داخل مجلد المشروع نفسه.');
  console.warn('    على Railway: أنشئ Volume واربطه بمسار (مثلاً /data) واضبط DATA_DIR=/data');
  console.warn('    وإلا ستُمسح كل البيانات عند أي إعادة نشر (Deploy) قادمة.');
}

// ----- إعداد Express -----
const app = express();
const server = http.createServer(app);

app.set('trust proxy', 1); // Railway وأي منصة خلف reverse proxy

// فحص صحة بدون مصادقة (يُستخدم من Railway Healthcheck) — يجب أن يبقى قبل Basic Auth
// حتى لا يفشل الفحص بسبب طلب بيانات دخول لا يملكها Railway.
app.get('/healthz', (req, res) => res.status(200).json({ ok: true }));

app.use(helmet({ contentSecurityPolicy: false })); // الواجهة تحمّل سكربتات مضمّنة من عدة CDNs

const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: Number.parseInt(process.env.AUTH_RATE_LIMIT_MAX || '300', 10),
  standardHeaders: true,
  legacyHeaders: false,
  message: { ok: false, error: 'محاولات كثيرة جداً، حاول لاحقاً' },
});
app.use(authLimiter);

app.use(basicAuth({ users: basicAuthUsers, challenge: true, realm: 'Five66-IqZ9' }));

const apiLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: Number.parseInt(process.env.API_RATE_LIMIT_MAX || '1200', 10),
  standardHeaders: true,
  legacyHeaders: false,
});
app.use('/api/', apiLimiter);

app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/whoami', (req, res) => res.json({ user: req.auth?.user || null }));

// ----- القراءة الكاملة (نفس صيغة اللقطة القديمة: مفتاح -> نص JSON) -----
// يبقى هذا متوافقاً مع مسار loadState() الاحتياطي القديم في index.html.
app.get('/api/state', (req, res) => res.json(db.readAll()));

// الكتابة الكاملة القديمة أُلغيت عمداً: كل كتابة تمر الآن عبر push دقيق لكل مفتاح
// مع رقم مراجعة، فلا يمكن للقطة قديمة أن تكتب فوق تعديلات وصلت لاحقاً.
app.post('/api/state', (req, res) => {
  res.status(410).json({ ok: false, error: 'الحفظ الكامل القديم غير مفعّل. استخدم /api/sync/push.' });
});

// ----- الدفع الفوري (REST احتياطي: يُستخدم فقط قبل اكتمال اتصال WebSocket) -----
app.post('/api/sync/push', (req, res) => {
  const items = Array.isArray(req.body?.items) ? req.body.items.slice(0, 50) : [];
  if (!items.length) return res.status(400).json({ ok: false, error: 'لا توجد بيانات لإرسالها' });
  const actor = req.auth?.user || 'unknown';
  const { results, accepted } = db.acceptPush(items, actor);
  broadcastAccepted(accepted, null);
  res.json({ ok: true, results });
});

// ----- سجل التعديلات لمفتاح معيّن (نسخ احتياطية قابلة للاسترجاع) -----
app.get('/api/history/:key', (req, res) => {
  const limit = Math.min(Number.parseInt(req.query.limit || '20', 10) || 20, 200);
  res.json(db.history(req.params.key, limit));
});

app.get('/api/sync-status', (req, res) => {
  res.json({ backend: 'sqlite', persistentDiskConfigured: Boolean(process.env.DATA_DIR), connectedClients: wss.clients.size, ...db.stats() });
});

// نسخة احتياطية يدوية كاملة
app.get('/api/backup', (req, res) => {
  res.setHeader('Content-Disposition', 'attachment; filename="diwan-backup.json"');
  res.json(db.readAll());
});

// ===================== تحميل الملفات (حل مشكلة Android WebView) =====================
const _tempFiles = new Map();
const MAX_TEMP_FILES = 200;
app.post('/api/download/upload', (req, res) => {
  try {
    const { data, mime, filename } = req.body || {};
    if (!data || !mime || !filename) return res.status(400).json({ ok: false, error: 'بيانات ناقصة' });
    for (const [k, v] of _tempFiles.entries()) if (v.expiresAt < Date.now()) _tempFiles.delete(k);
    if (_tempFiles.size >= MAX_TEMP_FILES) _tempFiles.delete(_tempFiles.keys().next().value);
    const token = Math.random().toString(36).slice(2) + Date.now().toString(36);
    _tempFiles.set(token, { data, mime, filename, expiresAt: Date.now() + 5 * 60 * 1000 });
    res.json({ ok: true, url: '/api/download/' + token });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});
app.get('/api/download/:token', (req, res) => {
  const entry = _tempFiles.get(req.params.token);
  if (!entry || entry.expiresAt < Date.now()) {
    _tempFiles.delete(req.params.token);
    return res.status(404).send('انتهت صلاحية الرابط');
  }
  const buf = Buffer.from(entry.data, 'base64');
  res.setHeader('Content-Type', entry.mime);
  res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(entry.filename)}`);
  res.setHeader('Content-Length', buf.length);
  res.send(buf);
  _tempFiles.delete(req.params.token);
});

// ===================== WebSocket: مزامنة فورية حقيقية =====================
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const user = checkBasicAuthHeader(req.headers['authorization']);
  if (!user) {
    socket.write('HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm="Five66-IqZ9"\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    ws._user = user;
    wss.emit('connection', ws, req);
  });
});

function broadcastAccepted(accepted, sender) {
  if (!accepted.length) return;
  for (const row of accepted) {
    const msg = JSON.stringify({ type: 'update', key: row.key, revision: row.revision, payload: row.payload, by: row.updated_by, at: row.updated_at });
    for (const client of wss.clients) {
      if (client.readyState === 1) client.send(msg);
    }
  }
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  // عند الاتصال: أرسل فوراً كامل الحالة الحالية مع أرقام المراجعات
  try {
    ws.send(JSON.stringify({ type: 'welcome', state: db.readAllWithRevision() }));
  } catch (_) {}

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (_) { return; }
    if (msg && msg.type === 'push' && Array.isArray(msg.items)) {
      const { results, accepted } = db.acceptPush(msg.items.slice(0, 50), ws._user || 'unknown');
      try { ws.send(JSON.stringify({ type: 'ack', results })); } catch (_) {}
      broadcastAccepted(accepted, ws); // البث يشمل الجميع؛ المرسل يتجاهل مراجعته الخاصة تلقائياً
    }
  });
});

// نبض دوري لإسقاط الاتصالات الميتة (شبكة انقطعت فجأة) حتى لا تبقى "متصلة" وهمياً
const heartbeatInterval = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { try { ws.terminate(); } catch (_) {} continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) {}
  }
}, 30000);

server.on('close', () => clearInterval(heartbeatInterval));

// ----- منع تعطّل السيرفر بالكامل عند انقطاع اتصال العميل فجأة -----
// عندما يغلق العميل (خصوصاً WebView على أندرويد أو شبكة موبايل ضعيفة) الاتصال
// أثناء إرسال جسم طلب كبير (رفع صور base64 مثلاً)، يرمي Node خطأ داخلي من
// IncomingMessage._destroy/abortIncoming لا يصل لأي route handler، فيسقط
// البروسس كله كـ uncaught exception. هذا يمنع ذلك بالتقاط الخطأ وتجاهله
// بأمان بدل تعطّل الخادم بالكامل.
server.on('clientError', (err, socket) => {
  if (!socket.destroyed) {
    socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
  }
});

process.on('uncaughtException', (err) => {
  const msg = String(err && err.message || '');
  const code = err && err.code;
  if (code === 'ECONNRESET' || code === 'ECONNABORTED' || /aborted/i.test(msg) || /destroy/i.test(msg)) {
    console.warn('⚠️  تجاهل خطأ اتصال عميل مقطوع:', code || msg);
    return; // لا تُسقط السيرفر بسبب انقطاع اتصال عميل
  }
  console.error('❌ خطأ غير متوقع (fatal):', err);
  process.exit(1); // أي خطأ آخر غير معروف، الأسلم إعادة تشغيل نظيفة عبر Railway
});

process.on('unhandledRejection', (reason) => {
  console.error('❌ Promise مرفوض بدون معالجة:', reason);
});

server.listen(PORT, () => {
  console.log(`✅ سيرفر الديوان العسكري يعمل على المنفذ ${PORT}`);
  console.log(`   ملف قاعدة البيانات: ${db.DB_FILE}`);
});
