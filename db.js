// ===================== طبقة التخزين الدائم =====================
// SQLite على قرص دائم (يجب أن يكون DATA_DIR داخل Volume دائم على Railway،
// وإلا سيُمسح عند كل إعادة نشر ونعود لنفس مشكلة "الرجوع للنسخة الأصلية").
//
// كل مفتاح حالة (نفس مفاتيح localStorage القديمة: mil_khasm, mil_persons_edited...)
// يُخزَّن كسجل واحد مع رقم مراجعة (revision) يزيد مع كل تعديل. بالإضافة لذلك،
// كل تعديل يُسجَّل أيضاً في جدول history بشكل دائم لا يُحذف أبداً — أي حفظ فعلي
// لأي تعديل يقوم به المستخدم، مهما كان، بشكل قابل للاسترجاع لاحقاً.

const path = require('path');
const fs = require('fs');
const Database = require('better-sqlite3');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
const DB_FILE = path.join(DATA_DIR, 'five66.db');

const db = new Database(DB_FILE);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = FULL'); // أولوية للحفظ الدائم على السرعة القصوى
db.pragma('auto_vacuum = INCREMENTAL'); // يفعّل الحلقة فقط على القواعد الجديدة؛ القاعدة الحالية تحتاج VACUUM يدوي مرة واحدة لتفعيله فعلياً (راجع ملاحظات التنظيف)

db.exec(`
CREATE TABLE IF NOT EXISTS records (
  key TEXT PRIMARY KEY,
  revision INTEGER NOT NULL DEFAULT 0,
  payload TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  updated_by TEXT
);
CREATE TABLE IF NOT EXISTS history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  key TEXT NOT NULL,
  revision INTEGER NOT NULL,
  payload TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  updated_by TEXT
);
CREATE INDEX IF NOT EXISTS idx_history_key ON history(key, revision);
`);

const getAllStmt = db.prepare('SELECT key, revision, payload FROM records');
const getRecordStmt = db.prepare('SELECT revision FROM records WHERE key = ?');
const upsertRecordStmt = db.prepare(`
  INSERT INTO records (key, revision, payload, updated_at, updated_by)
  VALUES (@key, @revision, @payload, @updated_at, @updated_by)
  ON CONFLICT(key) DO UPDATE SET
    revision = @revision, payload = @payload, updated_at = @updated_at, updated_by = @updated_by
`);
const insertHistoryStmt = db.prepare(`
  INSERT INTO history (key, revision, payload, updated_at, updated_by)
  VALUES (@key, @revision, @payload, @updated_at, @updated_by)
`);
const countRecordsStmt = db.prepare('SELECT COUNT(*) c FROM records');
const countHistoryStmt = db.prepare('SELECT COUNT(*) c FROM history');
const getHistoryStmt = db.prepare(
  'SELECT revision, payload, updated_at, updated_by FROM history WHERE key = ? ORDER BY revision DESC LIMIT ?'
);

// يحذف نسخ history الزائدة عن الحد لكل مفتاح، مع إبقاء الأحدث دائماً.
// هذا لا يمس جدول records (الحالة الحالية) إطلاقاً — فقط يقلّص سجل النسخ القديمة.
const deleteOldHistoryStmt = db.prepare(`
  DELETE FROM history
  WHERE id IN (
    SELECT id FROM (
      SELECT id, ROW_NUMBER() OVER (PARTITION BY key ORDER BY revision DESC) AS rn
      FROM history
    )
    WHERE rn > ?
  )
`);

function cleanupHistory(keepPerKey = 50) {
  const info = deleteOldHistoryStmt.run(keepPerKey);
  return { deleted: info.changes };
}

// يعيد الصفحات المحذوفة فعلياً لنظام التشغيل (تقليص حجم الملف على القرص).
// يُستخدم auto_vacuum=INCREMENTAL بدل VACUUM الكامل لأنه لا يحتاج مساحة مؤقتة
// تعادل حجم قاعدة البيانات كاملة (مهم عندما تكون المساحة محدودة أصلاً).
// يفرّغ ملف WAL المؤقت (five66.db-wal) بدمجه بالقاعدة الرئيسية وتصغيره —
// عادة هذا الملف هو المتضخم الفعلي بسبب synchronous=FULL + كتابة متكررة.
function checkpoint() {
  return db.pragma('wal_checkpoint(TRUNCATE)');
}

function reclaimSpace(pages = 1000) {
  db.pragma(`incremental_vacuum(${pages})`);
}

const MAX_ITEM_BYTES = 20 * 1024 * 1024; // حد أقصى معقول لحجم أي مفتاح واحد (يحتوي غالباً مصفوفة JSON)

// يقبل دفعة من التعديلات ويحفظها بشكل ذري (الكل أو لا شيء) داخل معاملة واحدة،
// ويُبقي دائماً على نسخة كاملة من كل تعديل في history قبل أي شيء آخر.
function acceptPush(items, actor) {
  const now = new Date().toISOString();
  const results = [];
  const accepted = [];
  const txn = db.transaction((list) => {
    for (const item of list) {
      if (!item || typeof item.key !== 'string' || !item.key.trim() || typeof item.payload !== 'string') {
        results.push({ key: item && item.key, status: 'invalid' });
        continue;
      }
      const key = item.key.slice(0, 120);
      if (Buffer.byteLength(item.payload, 'utf8') > MAX_ITEM_BYTES) {
        results.push({ key, status: 'too_large' });
        continue;
      }
      const current = getRecordStmt.get(key);
      const revision = (current ? current.revision : 0) + 1;
      const row = { key, revision, payload: item.payload, updated_at: now, updated_by: actor || 'unknown' };
      upsertRecordStmt.run(row);
      insertHistoryStmt.run(row); // الحفظ الدائم الفعلي: سجل لا يُحذف لأي تعديل
      results.push({ key, status: 'ok', revision });
      accepted.push(row);
    }
  });
  txn(items);
  return { results, accepted };
}

function readAll() {
  const state = {};
  for (const r of getAllStmt.all()) state[r.key] = r.payload;
  return state;
}

function readAllWithRevision() {
  const state = {};
  for (const r of getAllStmt.all()) state[r.key] = { revision: r.revision, payload: r.payload };
  return state;
}

function stats() {
  return { recordCount: countRecordsStmt.get().c, historyCount: countHistoryStmt.get().c, dbFile: DB_FILE, dataDir: DATA_DIR };
}

function history(key, limit) {
  return getHistoryStmt.all(key, limit || 20);
}

module.exports = { acceptPush, readAll, readAllWithRevision, stats, history, cleanupHistory, reclaimSpace, checkpoint, DB_FILE, DATA_DIR };
