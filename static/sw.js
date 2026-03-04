/* ── Myle Community Service Worker ──────────────────────── */
const CACHE   = 'myle-v4';
const STATIC  = [
  '/static/css/style.css',
  '/static/icon-192.png',
  '/static/icon-512.png',
  '/static/manifest.json',
];

/* Install: cache static assets */
self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(STATIC))
  );
  self.skipWaiting();
});

/* Activate: clear old caches */
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

/* Fetch: network-first for pages, cache-first for static */
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  /* Only handle same-origin requests */
  if (url.origin !== location.origin) return;

  /* Static assets → cache first */
  if (url.pathname.startsWith('/static/')) {
    e.respondWith(
      caches.match(e.request).then(cached =>
        cached || fetch(e.request).then(res => {
          const clone = res.clone();
          caches.open(CACHE).then(c => c.put(e.request, clone));
          return res;
        })
      )
    );
    return;
  }

  /* Pages/API → network first, fallback to cache */
  e.respondWith(
    fetch(e.request).catch(() => caches.match(e.request))
  );
});

/* ── Push Notifications ──────────────────────────────────── */
self.addEventListener('push', e => {
  let data = { title: 'Myle', body: '', url: '/' };
  try { data = { ...data, ...e.data.json() }; } catch (_) {}
  e.waitUntil(
    self.registration.showNotification(data.title, {
      body:  data.body,
      icon:  '/static/icon-192.png',
      badge: '/static/icon-192.png',
      data:  { url: data.url }
    })
  );
});

self.addEventListener('notificationclick', e => {
  e.notification.close();
  const target = (e.notification.data && e.notification.data.url) || '/';
  e.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(list => {
      for (const c of list) {
        if (c.url.endsWith(target) && 'focus' in c) return c.focus();
      }
      return clients.openWindow(target);
    })
  );
});
