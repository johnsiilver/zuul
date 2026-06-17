// The UI updates panes in place via fragment fetches — clicking a namespace reloads only
// the records pane, clicking a record reloads only the detail pane, and Refresh reloads
// every open pane while preserving the selection, scroll positions, and search filter.

function paneFetch(url, targetId) {
  return fetch(url, { headers: { 'X-Requested-With': 'fetch' } })
    .then((r) => r.text())
    .then((html) => {
      const el = document.getElementById(targetId);
      if (el) el.innerHTML = html;
    })
    .catch((e) => console.error('zuul pane fetch failed', url, e));
}

function setParam(key, val) {
  const u = new URL(window.location);
  if (val) u.searchParams.set(key, val);
  else u.searchParams.delete(key);
  history.replaceState(null, '', u);
}

function param(key) {
  return new URL(window.location).searchParams.get(key) || '';
}

// markSelected sets the .selected class on the clicked link's <li>, clearing it from its
// siblings in the same list.
function markSelected(link) {
  const li = link.closest('li');
  if (!li || !li.parentElement) return;
  li.parentElement.querySelectorAll(':scope > li.selected').forEach((s) => s.classList.remove('selected'));
  li.classList.add('selected');
}

// applyFilter narrows the visible namespace list to entries containing the search text.
function applyFilter() {
  const input = document.querySelector('#search input');
  if (!input) return;
  const q = input.value.toLowerCase();
  document.querySelectorAll('#ns-list li').forEach((li) => {
    li.style.display = li.textContent.toLowerCase().includes(q) ? '' : 'none';
  });
}

// zuulRefresh reloads data for every open pane, keeping the current namespace/record
// selection, each pane's scroll position, the window scroll, and the search filter.
function zuulRefresh() {
  const panes = Array.from(document.querySelectorAll('.pane'));
  const scrolls = panes.map((p) => p.scrollTop);
  const winY = window.scrollY;
  const ns = param('ns');
  const rec = param('rec');

  const jobs = [paneFetch('/frag/namespaces?ns=' + encodeURIComponent(ns), 'pane-namespaces')];
  if (ns) {
    jobs.push(paneFetch('/frag/records?ns=' + encodeURIComponent(ns) + '&rec=' + encodeURIComponent(rec), 'pane-records'));
  }
  if (rec) {
    jobs.push(paneFetch('/frag/detail?rec=' + encodeURIComponent(rec), 'pane-detail'));
  }
  Promise.all(jobs).then(() => {
    applyFilter();
    panes.forEach((p, i) => { p.scrollTop = scrolls[i]; });
    window.scrollTo(0, winY);
  });
}

// Delegate clicks so the handlers survive pane innerHTML swaps. A namespace click updates
// only the records pane; a record click updates only the detail pane.
document.addEventListener('click', (e) => {
  const nsLink = e.target.closest('.ns-link');
  if (nsLink) {
    e.preventDefault();
    const ns = nsLink.dataset.ns;
    setParam('ns', ns);
    markSelected(nsLink);
    paneFetch('/frag/records?ns=' + encodeURIComponent(ns) + '&rec=' + encodeURIComponent(param('rec')), 'pane-records');
    return;
  }
  const recLink = e.target.closest('.rec-link');
  if (recLink) {
    e.preventDefault();
    const rec = recLink.dataset.rec;
    setParam('rec', rec);
    markSelected(recLink);
    paneFetch('/frag/detail?rec=' + encodeURIComponent(rec), 'pane-detail');
  }
});

document.addEventListener('DOMContentLoaded', () => {
  const form = document.getElementById('search');
  const input = form ? form.querySelector('input') : null;
  if (!input) return;

  // Typing narrows the namespace list live, without a round trip.
  input.addEventListener('keyup', (e) => {
    if (e.key === 'Enter') return; // submit handles Enter
    applyFilter();
  });

  // Submitting loads the records for the typed prefix, in place.
  form.addEventListener('submit', (e) => {
    e.preventDefault();
    const ns = input.value;
    setParam('ns', ns);
    paneFetch('/frag/records?ns=' + encodeURIComponent(ns) + '&rec=' + encodeURIComponent(param('rec')), 'pane-records');
  });
});
