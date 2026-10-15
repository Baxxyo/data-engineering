/* Modern Data Engineering — SDAIA Academy slide engine
   Keys: → ↓ PgDn Space = next · ← ↑ PgUp = previous · Home/End · f = fullscreen · Esc = exit fullscreen / close
         n = speaker notes · o = overview · ? = help · t = start/pause break timer on a break slide */
(function () {
  const deck = document.querySelector('.deck');
  const slides = Array.from(deck.querySelectorAll(':scope > section.slide'));
  const cfg = Object.assign({ course: 'Modern Data Engineering', day: '', track: 'Data Engineering Track' }, deck.dataset);
  let idx = 0;

  /* ---------- chrome ---------- */
  let session = '';
  slides.forEach((s, i) => {
    if (s.dataset.session !== undefined) session = s.dataset.session;
    const label = s.dataset.label !== undefined ? s.dataset.label : session;
    if (!s.querySelector(':scope > .body')) {
      const body = document.createElement('div');
      body.className = 'body';
      while (s.firstChild) body.appendChild(s.firstChild);
      s.appendChild(body);
    }
    s.querySelectorAll('aside.notes').forEach((n) => s.appendChild(n));
    const top = document.createElement('div');
    top.className = 'chrome-top';
    top.innerHTML = '<div class="logo">' + (window.ACADEMY_LOGO || 'SDAIA Academy').replace(/acad-/g, 'acad' + i + '-') + '</div>' +
      '<div class="crumb">' + (cfg.day ? '<b>' + cfg.day + '</b>' : '') + (label ? '<span>·</span><span>' + label + '</span>' : '') + '</div>';
    const bot = document.createElement('div');
    bot.className = 'chrome-bottom';
    bot.innerHTML = '<div class="track"><i></i>' + cfg.track + ' · ' + cfg.course + '</div><div class="no">' + (i + 1) + ' / ' + slides.length + '</div>';
    s.prepend(top);
    s.appendChild(bot);
    if (s.classList.contains('title')) bot.querySelector('.no').textContent = '';
  });
  const progress = document.createElement('div');
  progress.className = 'progress';
  deck.appendChild(progress);

  /* ---------- scaling ---------- */
  function fit() {
    if (document.body.classList.contains('overview')) return;
    const s = Math.min(window.innerWidth / 1600, window.innerHeight / 900);
    deck.style.transform = 'translate(-50%, -50%) scale(' + s + ')';
  }
  window.addEventListener('resize', fit);
  fit();

  /* ---------- navigation ---------- */
  const notesPanel = document.createElement('div');
  notesPanel.className = 'notes-panel';
  document.body.appendChild(notesPanel);

  function show(i) {
    idx = Math.max(0, Math.min(slides.length - 1, i));
    slides.forEach((s, j) => s.classList.toggle('active', j === idx));
    progress.style.width = (slides.length > 1 ? (idx / (slides.length - 1)) * 100 : 100) + '%';
    history.replaceState(null, '', '#' + (idx + 1));
    const n = slides[idx].querySelector('aside.notes');
    notesPanel.innerHTML = '<h5>Speaker notes · slide ' + (idx + 1) + '</h5>' + (n ? n.innerHTML : '<i>No notes for this slide.</i>');
    const timer = slides[idx].querySelector('.timer');
    if (timer && !timer.dataset.started) resetTimer(timer);
  }
  const next = () => show(idx + 1);
  const prev = () => show(idx - 1);

  function toggleFullscreen() {
    if (document.fullscreenElement) document.exitFullscreen();
    else document.documentElement.requestFullscreen().catch(() => {});
  }

  function toggleOverview(force) {
    const on = force !== undefined ? force : !document.body.classList.contains('overview');
    document.body.classList.toggle('overview', on);
    if (on) { deck.style.transform = ''; slides[idx].scrollIntoView({ block: 'center' }); }
    else fit();
  }
  slides.forEach((s, i) => s.addEventListener('click', (e) => {
    if (document.body.classList.contains('overview')) { e.preventDefault(); toggleOverview(false); show(i); }
  }));

  const help = document.createElement('div');
  help.className = 'help';
  help.innerHTML = '<div class="box"><h5>Keyboard</h5><table>' +
    '<tr><td><kbd>→</kbd><kbd>↓</kbd><kbd>Space</kbd></td><td>Next slide</td></tr>' +
    '<tr><td><kbd>←</kbd><kbd>↑</kbd></td><td>Previous slide</td></tr>' +
    '<tr><td><kbd>Home</kbd><kbd>End</kbd></td><td>First / last slide</td></tr>' +
    '<tr><td><kbd>F</kbd></td><td>Enter / leave fullscreen</td></tr>' +
    '<tr><td><kbd>Esc</kbd></td><td>Leave fullscreen · close panels</td></tr>' +
    '<tr><td><kbd>N</kbd></td><td>Speaker notes</td></tr>' +
    '<tr><td><kbd>O</kbd></td><td>Slide overview</td></tr>' +
    '<tr><td><kbd>T</kbd></td><td>Start / pause the break timer</td></tr>' +
    '<tr><td><kbd>?</kbd></td><td>This help</td></tr></table></div>';
  help.addEventListener('click', () => help.classList.remove('on'));
  document.body.appendChild(help);

  const hint = document.createElement('div');
  hint.className = 'hint';
  hint.textContent = 'F fullscreen · ← → navigate · ? help';
  document.body.appendChild(hint);
  setTimeout(() => (hint.style.opacity = 0), 5000);

  document.addEventListener('keydown', (e) => {
    if (e.ctrlKey || e.metaKey || e.altKey) return;
    const tag = (e.target.tagName || '').toLowerCase();
    if (tag === 'input' || tag === 'textarea') return;
    switch (e.key) {
      case 'ArrowRight': case 'ArrowDown': case 'PageDown': case ' ': case 'Enter':
        e.preventDefault(); next(); break;
      case 'ArrowLeft': case 'ArrowUp': case 'PageUp': case 'Backspace':
        e.preventDefault(); prev(); break;
      case 'Home': e.preventDefault(); show(0); break;
      case 'End': e.preventDefault(); show(slides.length - 1); break;
      case 'f': case 'F': e.preventDefault(); toggleFullscreen(); break;
      case 'Escape':
        if (document.fullscreenElement) document.exitFullscreen();
        help.classList.remove('on'); notesPanel.classList.remove('on'); toggleOverview(false); break;
      case 'n': case 'N': notesPanel.classList.toggle('on'); break;
      case 'o': case 'O': toggleOverview(); break;
      case '?': help.classList.toggle('on'); break;
      case 't': case 'T': {
        const timer = slides[idx].querySelector('.timer');
        if (timer) toggleTimer(timer);
        break;
      }
    }
  });

  /* touch swipe */
  let tx = null;
  document.addEventListener('touchstart', (e) => { tx = e.touches[0].clientX; }, { passive: true });
  document.addEventListener('touchend', (e) => {
    if (tx === null) return;
    const dx = e.changedTouches[0].clientX - tx;
    if (Math.abs(dx) > 50) (dx < 0 ? next : prev)();
    tx = null;
  });

  /* ---------- break timers ---------- */
  /* Default length comes from data-minutes in the HTML (10 for breaks, 20 for the mid-day break).
     On break slides the instructor can change it live: type a length in minutes, or a "back at" clock
     time. The chosen length is remembered in this browser for that break. */
  const timers = new WeakMap();
  const pad = (n) => String(n).padStart(2, '0');
  const clock = (d) => pad(d.getHours()) + ':' + pad(d.getMinutes());
  const store = {
    get(k) { try { return localStorage.getItem(k); } catch (e) { return null; } },
    set(k, v) { try { localStorage.setItem(k, v); } catch (e) { /* storage unavailable */ } },
  };
  function render(el, secs) {
    el.textContent = pad(Math.floor(secs / 60)) + ':' + pad(secs % 60);
    el.classList.toggle('done', secs === 0);
    const st = timers.get(el);
    if (st && st.back) st.back.textContent = st.end ? 'Back at ' + clock(new Date(st.end)) : '';
  }
  function stop(st) { if (st.h) clearInterval(st.h); st.h = null; }
  function tick(el) {
    const st = timers.get(el);
    st.left = Math.max(0, Math.round((st.end - Date.now()) / 1000));
    render(el, st.left);
    if (st.left === 0) stop(st);
  }
  function run(el) {
    const st = timers.get(el);
    stop(st);
    st.end = Date.now() + st.left * 1000;
    el.dataset.started = '1';
    st.h = setInterval(() => tick(el), 250);
    tick(el);
  }
  function resetTimer(el) {
    const st = timers.get(el);
    stop(st);
    st.left = st.minutes * 60;
    st.end = null;
    delete el.dataset.started;
    render(el, st.left);
  }
  function toggleTimer(el) {
    const st = timers.get(el);
    if (st.h) { stop(st); st.end = null; render(el, st.left); } else run(el);
  }
  function setMinutes(el, minutes) {
    const st = timers.get(el);
    st.minutes = Math.max(1, Math.min(180, Math.round(minutes)));
    if (st.key) store.set(st.key, String(st.minutes));
    if (st.input) st.input.value = st.minutes;
    const title = el.closest('.slide').querySelector('h1');
    if (title && /^\d+-minute break$/.test(title.textContent)) title.textContent = st.minutes + '-minute break';
    resetTimer(el);
  }
  document.querySelectorAll('.timer').forEach((el) => {
    const slideNo = slides.indexOf(el.closest('.slide'));
    const isBreak = el.closest('.slide').classList.contains('break');
    const st = { minutes: Number(el.dataset.minutes || 10), left: 0, h: null, end: null };
    timers.set(el, st);
    const btns = document.createElement('div');
    btns.className = 'timer-btns';
    btns.innerHTML = '<button data-a="go">Start / pause</button><button data-a="reset">Reset</button>';
    btns.addEventListener('click', (e) => {
      const a = e.target.dataset.a;
      if (a === 'go') toggleTimer(el);
      if (a === 'reset') resetTimer(el);
    });
    el.after(btns);
    if (isBreak) {
      st.key = 'modern-de:break:' + location.pathname + ':' + slideNo;
      const saved = Number(store.get(st.key));
      if (saved > 0) st.minutes = saved;
      const edit = document.createElement('div');
      edit.className = 'timer-edit';
      edit.innerHTML = '<label>Length <input class="mins" type="text" inputmode="numeric" maxlength="3" lang="en"> min</label>' +
        '<span class="or">or</span><label>back at <input type="time"></label><span class="back"></span>';
      st.input = edit.querySelector('input.mins');
      st.back = edit.querySelector('.back');
      st.input.addEventListener('change', () => setMinutes(el, parseInt(st.input.value.replace(/[٠-٩]/g, (d) => '٠١٢٣٤٥٦٧٨٩'.indexOf(d)), 10) || st.minutes));
      edit.querySelector('input[type=time]').addEventListener('change', (e) => {
        const [h, m] = e.target.value.split(':').map(Number);
        if (Number.isNaN(h)) return;
        const end = new Date();
        end.setHours(h, m, 0, 0);
        if (end <= new Date()) end.setDate(end.getDate() + 1);
        const secs = Math.round((end - Date.now()) / 1000);
        setMinutes(el, Math.ceil(secs / 60));
        st.left = secs;  // count down to the exact clock time
        run(el);
        e.target.value = '';
      });
      btns.after(edit);
    }
    setMinutes(el, st.minutes);
  });

  /* ---------- quizzes: click an option to reveal ---------- */
  document.querySelectorAll('.quiz').forEach((q) => {
    q.querySelectorAll('.opt').forEach((o) => o.addEventListener('click', () => q.classList.add('revealed')));
  });

  /* ---------- syntax highlighting (optional, works offline without it) ---------- */
  if (window.hljs) document.querySelectorAll('pre.code code').forEach((c) => window.hljs.highlightElement(c));

  /* ---------- layout self-check: open with ?check to list overflowing slides ---------- */
  if (location.search.includes("check")) (document.fonts ? document.fonts.ready : Promise.resolve()).then(() => {
    // measured after web fonts load: fallback fonts are narrower and hide overflow
    const issues = [];
    slides.forEach((s, i) => {
      show(i);
      const body = s.querySelector(".body");
      if (body.scrollHeight > body.clientHeight + 2) issues.push((i + 1) + ": body overflows by " + (body.scrollHeight - body.clientHeight) + "px");
      s.querySelectorAll("pre, table, .card, .flow, .callout").forEach((el) => {
        if (el.scrollHeight > el.clientHeight + 2) issues.push((i + 1) + ": " + el.tagName.toLowerCase() + "." + el.className + " overflows y by " + (el.scrollHeight - el.clientHeight) + "px");
        if (el.scrollWidth > el.clientWidth + 2) issues.push((i + 1) + ": " + el.tagName.toLowerCase() + "." + el.className + " overflows x by " + (el.scrollWidth - el.clientWidth) + "px");
      });
      s.querySelectorAll("pre.code > code").forEach((c) => {  // long lines can hide in the pre's right padding
        const pre = c.parentElement, cs = getComputedStyle(pre);
        const inner = pre.clientWidth - parseFloat(cs.paddingLeft) - parseFloat(cs.paddingRight);
        if (c.scrollWidth > inner + 2) issues.push((i + 1) + ": code line too long by " + Math.round(c.scrollWidth - inner) + "px");
      });
    });
    const out = document.createElement("pre");
    out.id = "check-report";
    out.textContent = "CHECK " + slides.length + " slides\n" + (issues.join("\n") || "OK");
    document.body.appendChild(out);
    show(Number.isFinite(start) ? start - 1 : 0);
  });

  const start = parseInt(location.hash.slice(1), 10);
  show(Number.isFinite(start) ? start - 1 : 0);
})();
