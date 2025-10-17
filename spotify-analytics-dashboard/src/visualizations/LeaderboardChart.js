// visualizations/LeaderboardChart.js
// DOM renderer for the Leaderboard view: 3 sections with Top-5 + carousel of 7

export class LeaderboardChart {
  constructor(containerRef, options = {}) {
    this.containerRef = containerRef;
    this.root = null;
    this.data = { artists: [], albums: [], tracks: [] };
    this.index = { artist: new Map(), album: new Map(), track: new Map() };
    this.onClick = null; // optional callback
    this.onItemSelect = options.onItemSelect || null;
    this._boundHandler = this._handleClick.bind(this);
  }

  destroy() {
    if (this.root) {
      this.root.removeEventListener('click', this._boundHandler);
      this.root.innerHTML = "";
    }
  }

  render({ artists = [], albums = [], tracks = [] } = {}) {
    const el = this.containerRef.current;
    if (!el) return;
    this.root = el;
    this.root.removeEventListener('click', this._boundHandler);
    this.root.addEventListener('click', this._boundHandler);

    this.data = { artists, albums, tracks };
    this.index.artist = new Map(artists.map(d => [d.id, d]));
    this.index.album = new Map(albums.map(d => [d.id, d]));
    this.index.track = new Map(tracks.map(d => [d.id || d.track_id || d.name, d]));

    this.root.innerHTML = [
      this._section("Top Artists", artists, 'artist', 'lb-artists'),
      this._section("Top Albums", albums, 'album', 'lb-albums'),
      this._section("Top Tracks", tracks, 'track', 'lb-tracks'),
    ].join("\n");

    [
      { id: 'lb-artists-rest', kind: 'artist' },
      { id: 'lb-albums-rest', kind: 'album' },
      { id: 'lb-tracks-rest', kind: 'track' }
    ].forEach(({ id, kind }) => {
      const grid = this.root.querySelector(`#${id}`);
      if (!grid) return;
      grid.dataset.kind = kind;
      grid.dataset.page = '0';
      grid.dataset.pageSize = '7';
      const src = this._idsForKind(kind).slice(5);
      grid.dataset.source = JSON.stringify(src);
      this._renderPage(grid);
    });
  }

  _idsForKind(kind) {
    const arr = kind === 'artist' ? this.data.artists : kind === 'album' ? this.data.albums : this.data.tracks;
    return arr.map(d => d.id || d.track_id || d.name);
  }

  _section(title, items, kind, slug) {
    const top5 = items.slice(0, 5);
    const heroes = top5.map((d, i) => this._heroCard(d, kind, i + 1)).join("\n");
    const heroContent = heroes || this._emptyHeroState(kind);
    const gridId = `${slug}-rest`;
    const prevId = `${slug}-prev`;
    const nextId = `${slug}-next`;
    const total = items.length;
    const overflow = Math.max(0, total - 5);
    const hasOverflow = overflow > 0;
    const carouselClasses = hasOverflow
      ? 'rank-section__carousel'
      : 'rank-section__carousel rank-section__carousel--empty';
    const disableAttr = hasOverflow ? '' : ' disabled';
    return `
      <section class="rank-section" id="${slug}">
        <header class="rank-section__header">
          <h3 class="rank-section-title">${title}</h3>
        </header>
        <div class="rank-section__top5">
          <div class="top5-row">
            ${heroContent}
          </div>
        </div>
        <div class="${carouselClasses}">
          <div class="rank-section__carousel-header">
            <span class="rank-section__carousel-title">Runners-up</span>
          </div>
          <div class="mini-row">
            <button class="carousel-btn prev" id="${prevId}" data-target="${gridId}" aria-label="Previous"${disableAttr}>&#x2039;</button>
            <div class="mini-grid" id="${gridId}"></div>
            <button class="carousel-btn next" id="${nextId}" data-target="${gridId}" aria-label="Next"${disableAttr}>&#x203A;</button>
          </div>
          <div class="rank-section__carousel-footer">
            <span class="carousel-indicator"></span>
          </div>
        </div>
      </section>`;
  }

  _heroCard(d, kind, rank) {
    const img = d.image || "";
    const name = escapeHtml(d.name || "");
    return `
      <article class="hero-card hero-card--rank-${rank}" data-expand-kind="${kind}" data-expand-id="${escapeHtml(d.id || d.track_id || name)}">
        <div class="hero-card__media">
          <img src="${img}" alt="" class="hero-card__art"/>
          <span class="hero-card__badge hero-rank">#${rank}</span>
        </div>
        <div class="hero-card__info">
          <h4 class="hero-card__title" title="${name}">${name}</h4>
        </div>
      </article>`;
  }

  _miniCard(d, kind, rank) {
    const img = d.image || '';
    const name = escapeHtml(d.name || '');
    return `
      <div class="mini-card" title="${name}" data-expand-kind="${kind}" data-expand-id="${escapeHtml(d.id || d.track_id || name)}">
        <div class="mini-card__media">
          <img src="${img}" alt="">
          <span class="mini-rank">#${rank}</span>
        </div>
        <div class="mini-card__name truncate">${name}</div>
      </div>`;
  }

  _emptyHeroState(kind) {
    const pluralLabel = this._kindPlural(kind);
    return `
            <div class="hero-empty">
              <div class="hero-empty__title">Awaiting data</div>
              <p class="hero-empty__copy">Once you listen more, your top ${pluralLabel} will appear here.</p>
            </div>`;
  }

  _miniEmptyState(kind) {
    const pluralLabel = this._kindPlural(kind);
    return `<div class="mini-placeholder">No other ${pluralLabel} yet</div>`;
  }

  _renderPage(gridEl) {
    const ids = JSON.parse(gridEl.dataset.source || '[]');
    const page = parseInt(gridEl.dataset.page || '0', 10);
    const size = parseInt(gridEl.dataset.pageSize || '7', 10);
    const kind = gridEl.dataset.kind;
    const start = page * size;
    const slice = ids.slice(start, start + size);
    const map = this.index[kind] || new Map();
    const rankOffset = 5 + start;
    const content = slice
      .map((id, i) => this._miniCard(map.get(id) || { id, name: id }, kind, rankOffset + i + 1))
      .join('');

    gridEl.classList.toggle('mini-grid--empty', ids.length === 0);
    gridEl.innerHTML = content || this._miniEmptyState(kind);

    this._syncCarouselControls(gridEl, {
      total: ids.length,
      page,
      size,
      maxPage: Math.max(0, Math.ceil(ids.length / size) - 1)
    });
  }

  _syncCarouselControls(gridEl, { total, page, size, maxPage }) {
    const carousel = gridEl.closest('.rank-section__carousel');
    if (!carousel) return;
    carousel.classList.toggle('rank-section__carousel--empty', total === 0);

    const prevBtn = carousel.querySelector('.carousel-btn.prev');
    const nextBtn = carousel.querySelector('.carousel-btn.next');

    if (prevBtn) {
      const disabled = total === 0 || page <= 0;
      prevBtn.disabled = disabled;
      prevBtn.classList.toggle('carousel-btn--disabled', disabled);
    }

    if (nextBtn) {
      const disabled = total === 0 || page >= maxPage;
      nextBtn.disabled = disabled;
      nextBtn.classList.toggle('carousel-btn--disabled', disabled);
    }

    const indicator = carousel.querySelector('.carousel-indicator');
    if (indicator) {
      if (total === 0) {
        indicator.textContent = 'Add more to unlock runners-up';
      } else {
        const start = page * size + 1;
        const end = Math.min(total, page * size + size);
        indicator.textContent = `${start}-${end} of ${total}`;
      }
    }
  }

  _kindLabel(kind) {
    if (kind === 'track') return 'Track';
    if (kind === 'album') return 'Album';
    return 'Artist';
  }

  _kindPlural(kind) {
    const label = this._kindLabel(kind).toLowerCase();
    return label.endsWith('s') ? label : `${label}s`;
  }

  _handleClick(e) {
    const btn = e.target.closest('.carousel-btn');
    if (btn) {
      if (btn.disabled) return;
      const gridId = btn.getAttribute('data-target');
      const grid = this.root.querySelector(`#${gridId}`);
      if (!grid) return;
      const ids = JSON.parse(grid.dataset.source || '[]');
      const size = parseInt(grid.dataset.pageSize || '7', 10);
      const maxPage = Math.max(0, Math.ceil(ids.length / size) - 1);
      let page = parseInt(grid.dataset.page || '0', 10);
      const dir = btn.classList.contains('next') ? 1 : -1;
      page = Math.min(maxPage, Math.max(0, page + dir));
      grid.dataset.page = String(page);
      this._renderPage(grid);
      return;
    }

    const card = e.target.closest('[data-expand-kind]');
    if (card && this.onItemSelect) {
      const kind = card.getAttribute('data-expand-kind');
      const key = card.getAttribute('data-expand-id');
      const map = this.index[kind] || new Map();
      const item = map.get(key);
      this.onItemSelect(kind, item, card.getBoundingClientRect());
    }
  }
}

function escapeHtml(s) {
  return s?.replace?.(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])) || '';
}
