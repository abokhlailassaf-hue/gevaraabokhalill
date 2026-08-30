// ===================== محرك المزامنة الفورية (بناء جديد) =====================
// يتصل بهذا الملف تلقائياً index.html عبر <script src="/sync-v2.js"></script>
// ويوفّر window.Five66Sync التي يستخدمها الكود الموجود فعلاً في الصفحة
// (refreshAppData, saveState -> _forceFlushNow, ...) بدون أي تعديل على index.html.
//
// الفكرة: لا استطلاع دوري إطلاقاً. الاتصال عبر WebSocket يبقى مفتوحاً؛ أي
// تعديل من هذا الجهاز يُرسَل خلال أجزاء من الثانية بعد الكتابة، وأي تعديل من
// جهاز آخر يصل فوراً عبر نفس القناة ويُطبَّق على الواجهة مباشرة.

(function () {
  'use strict';

  if (typeof window._setPendingSyncState !== 'function' || typeof window._getPendingSyncState !== 'function') {
    console.error('Five66Sync: الخطافات المطلوبة في index.html غير موجودة (_setPendingSyncState/_getPendingSyncState).');
    return;
  }

  var WS_URL = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/';
  var socket = null;
  var connected = false;
  var reconnectDelay = 1000;
  var knownRevision = {}; // آخر رقم مراجعة معروف لكل مفتاح (لتجاهل الرسائل القديمة/المكررة)
  var flushTimer = null;
  var FLUSH_DEBOUNCE_MS = 350; // تجميع تعديلات متتالية سريعة (كتابة حرف بحرف) دون فقد "الفورية" الفعلية

  var _origSetPendingSyncState = window._setPendingSyncState;

  // يُستدعى من التطبيق (عبر saveState) في كل مرة تتغيّر فيها البيانات محلياً.
  // نُبقي السلوك الأصلي كما هو تماماً، ونضيف فقط جدولة رفع فوري للسيرفر.
  window._setPendingSyncState = function (stateObj) {
    _origSetPendingSyncState(stateObj);
    scheduleFlush();
  };

  // يُستخدم داخلياً فقط لكتابة بيانات واردة من السيرفر دون إعادة جدولة رفعها من جديد
  function writeIncomingLocally(stateObj) {
    _origSetPendingSyncState(stateObj);
  }

  function scheduleFlush() {
    clearTimeout(flushTimer);
    flushTimer = setTimeout(flush, FLUSH_DEBOUNCE_MS);
  }

  // يبني قائمة العناصر التي تغيّرت فعلياً عن آخر ما نعرف أنه وصل للسيرفر
  function collectDirtyItems() {
    var pending = window._getPendingSyncState();
    if (!pending) return [];
    var items = [];
    for (var key in pending) {
      if (pending[key] == null) continue;
      items.push({ key: key, baseRevision: knownRevision[key] || 0, payload: pending[key] });
    }
    return items;
  }

  function flush() {
    clearTimeout(flushTimer);
    var items = collectDirtyItems();
    if (!items.length) return Promise.resolve(false);
    if (connected && socket && socket.readyState === 1) {
      try {
        socket.send(JSON.stringify({ type: 'push', items: items }));
        return Promise.resolve(true);
      } catch (e) {
        // يسقط إلى REST تلقائياً إن فشل الإرسال عبر المقبس
      }
    }
    // احتياطي عبر REST قبل اكتمال اتصال WebSocket أو أثناء انقطاعه، حتى لا يضيع أي تعديل
    return fetch('/api/sync/push', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items: items }),
    })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        (data.results || []).forEach(function (r) {
          if (r.status === 'ok') knownRevision[r.key] = r.revision;
        });
        return true;
      })
      .catch(function () { return false; });
  }

  // يطبّق تحديثاً وارداً على الحالة المحلية إن كان أحدث مما نعرفه فعلاً
  function applyIncoming(key, revision, payload) {
    if (knownRevision[key] != null && revision <= knownRevision[key]) return false;
    knownRevision[key] = revision;
    var pending = window._getPendingSyncState() || {};
    pending[key] = payload;
    writeIncomingLocally(pending);
    return true;
  }

  // يعيد بناء متغيرات التطبيق الحيّة من الحالة المحلية ثم يعيد رسم كل الأقسام —
  // بنفس تسلسل التحديث الحي المستخدم أصلاً في refreshAppData().
  var _rehydrating = false;
  function rehydrateAndRender() {
    if (_rehydrating) return Promise.resolve();
    _rehydrating = true;
    return Promise.resolve()
      .then(function () { return typeof loadState === 'function' ? loadState() : null; })
      .then(function () {
        try { if (typeof mergeSpecialRegistryRecordsIntoArchive === 'function' && mergeSpecialRegistryRecordsIntoArchive()) saveState(); } catch (e) {}
        try { if (typeof sortPersonsInPlace === 'function') sortPersonsInPlace(); } catch (e) {}
        try { if (typeof getOrderedPersons === 'function') filteredPersons = getOrderedPersons(persons); } catch (e) {}
        [
          'renderCards', 'renderTable', 'buildTafaqudTable', 'renderInjured', 'renderMartyrs',
          'renderHararin', 'renderKhasmTable', 'updateStats', 'updateSectionHeaders', 'refreshNotifications',
        ].forEach(function (fnName) {
          try { if (typeof window[fnName] === 'function') window[fnName](); } catch (e) { console.error(fnName, e); }
        });
      })
      .catch(function (e) { console.warn('Five66Sync rehydrate:', e); })
      .finally(function () { _rehydrating = false; });
  }

  function connect() {
    try {
      socket = new WebSocket(WS_URL);
    } catch (e) {
      scheduleReconnect();
      return;
    }
    socket.onopen = function () {
      connected = true;
      reconnectDelay = 1000;
    };
    socket.onmessage = function (ev) {
      var msg;
      try { msg = JSON.parse(ev.data); } catch (e) { return; }
      if (!msg || !msg.type) return;
      if (msg.type === 'welcome') {
        var changed = false;
        for (var key in msg.state) {
          if (applyIncoming(key, msg.state[key].revision, msg.state[key].payload)) changed = true;
        }
        if (changed) rehydrateAndRender();
      } else if (msg.type === 'update') {
        if (applyIncoming(msg.key, msg.revision, msg.payload)) rehydrateAndRender();
      } else if (msg.type === 'ack') {
        (msg.results || []).forEach(function (r) {
          if (r.status === 'ok') knownRevision[r.key] = r.revision;
        });
      }
    };
    socket.onclose = function () {
      connected = false;
      scheduleReconnect();
    };
    socket.onerror = function () {
      try { socket.close(); } catch (e) {}
    };
  }

  function scheduleReconnect() {
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 1.6, 15000);
  }

  // واجهة التوافق التي يستخدمها index.html فعلياً (refreshAppData, _forceFlushNow, syncStateToServer...)
  window.Five66Sync = {
    pull: function () { return rehydrateAndRender(); },
    flush: flush,
    status: function () { return { connected: connected, reconnectDelay: reconnectDelay }; },
  };

  connect();

  // ضمان محاولة رفع أخيرة عند إغلاق/مغادرة الصفحة (يتوافق مع خطافات index.html الموجودة أصلاً)
  window.addEventListener('beforeunload', function () {
    var items = collectDirtyItems();
    if (items.length && navigator.sendBeacon) {
      try {
        navigator.sendBeacon('/api/sync/push', new Blob([JSON.stringify({ items: items })], { type: 'application/json' }));
      } catch (e) {}
    }
  });
})();
