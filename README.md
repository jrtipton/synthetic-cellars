# Synthetic CellarTracker cellars

Three fake wine cellars for testing data-serving / semantic-model MCPs
against Claude. Each cellar is structurally indistinguishable from a
real CellarTracker export (column-for-column schema match) but encodes
specific, detectable behavioral patterns that a good model + good MCP
should be able to surface.

## Directory layout

Each persona has the four CT-standard files:

```
<persona>/
  inventory.csv   — one row per physical bottle still in the cellar
  purchase.csv    — one row per purchase lot (1-12 bottles each)
  consumed.csv    — one row per bottle that left the cellar (drank/sold/missing/flawed/given)
  notes.csv       — one row per tasting note
```

Schemas exactly match real CT exports — same columns, same order, same
quoting (`QUOTE_ALL`), `M/D/YYYY` dates.

## The three personas

### `a_auction_veteran/` — 13 years, ~5000 bottles
- **Buying**: 70% auction (HDH / Acker / Zachys / WineBid / K&L / Spectrum)
- **Persona**: spouse Ellen, no kids; sparse note-taking (~10%); low drinking-window adherence
- **Taste drift**: Bordeaux + California + Tuscany in early years, drifts to Burgundy + Piedmont + Champagne in late years (linear interp across 13yr)
- **Signature producers**: Bordeaux classed growths (Latour, Lafite, Margaux, Mouton, Haut-Brion, Léoville-Las Cases, Pichon, Cos, Lynch-Bages, Montrose, Ducru, Calon-Ségur) + Italian benchmarks (Conterno, Bartolo Mascarello, Soldera, Sassicaia, Ornellaia, Bruno Giacosa, Vietti)
- **Sell event**: ~400-bottle Zachys consignment in June 2022, biased toward CA/Bordeaux/Tuscany bottles bought in the early years (regret-pattern)
- **Events**: quarterly verticals — Rousseau retrospective, Bordeaux first-growth flight, California Cab vertical, Burgundy GC night, Piedmont Riserva tasting, Right Bank vertical (themes are coherent: 6/6 Rousseau bottles in a Rousseau retrospective)

### `b_allocation_diarist/` — 7 years, ~2500 bottles
- **Buying**: 55% winery / mailing-list allocations
- **Persona**: spouse Sam, kids Iris & Theo, friends "the Nguyen family" / "Kate and Josh" / "book club"
- **Style**: meticulous tasting notes (~55%), heavy meal-context notes (~80%), high window adherence
- **Signature producers**: West Coast cult Pinot/Chard (Hirsch, Rhys, Whitcraft, Sandlands, Kistler, Aubert, Marcassin, Kosta Browne, Patricia Green, Rivers-Marie, Cobb) + Oregon (Drouhin Oregon, Cameron, Beaux Frères, Eyrie) + grower Champagne (Bérêche, Egly-Ouriet, Larmandier-Bernier, Chartogne-Taillet, Pierre Péters)
- **Recurring specials**: opens a special bottle on Iris's birthday (3/17, Burgundy red), Theo's birthday (10/11, Burgundy red), and the anniversary (6/28, Champagne) every year
- **Events**: monthly book-club Friday (3-4 bottles per night, shared `iEvent`)
- **Shipments**: 215 multi-lot orders representing real winery-shipment days (3-6 different wines under one shared OrderId on the same day)

### `c_champagne_newcomer/` — 3 years, ~1000 bottles
- **Buying**: mixed sources, heavy Champagne (~8× region weight vs others)
- **Persona**: spouse Alex, no kids; modest note-taking (~30%)
- **Signature producers**: grower-Champagne deep cuts — Roses de Jeanne, Ulysse Collin, Jacques Lassaigne, Marie-Courtin, Olivier Horiot, Vouette et Sorbée, Savart, Jérôme Prévost, Emmanuel Brochet, David Léclapart, Tarlant, Bourgeois-Diaz, Vilmart, Doyard, Pierre Péters, Bollinger
- **Torture seeds**: the cellar is intentionally dense with Krug Grande Cuvée éditions, Roses de Jeanne single-vineyard cuvées (R18/R19/etc.), Ulysse Collin bottlings — modal vintage in purchases is `1001` (NV), but many wine names contain a 4-digit year (Krug "Edition 169eme", Roses de Jeanne "(2018)")
- **Sell event**: 12 bottles via WineBid in March 2025
- **Events**: biweekly Champagne Saturdays — sometimes themed ("Krug édition night" pulls only Krug)

## Detectable signals (acceptance criteria)

These are the patterns a good MCP + Claude should be able to surface:

### Cohort / occasion
- Diarist median consumed price: kids-present ~$75 < adults-only ~$120 < special-occasion ~$200+
- Diarist anniversary (6/28) and kids' birthdays (3/17, 10/11) recur annually with consistent bottle-class

### Time-series & drift
- Veteran region-share drift over 13 years: California ~46% → ~10%, Burgundy ~18% → ~64%
- Burgundy median nominal price climbs ~$80 (2013) → ~$210 (2026) reflecting region-aware inflation
- Seasonality: Veteran heavy Sept/Oct/May (auction season), Diarist heavy Mar/Apr/Sept/Oct (release windows), Newcomer heavy Nov/Dec (gifting)

### Sells
- Veteran: ~400 rows with `ShortType=Sold` clustered in a ~5-day window in June 2022
- Veteran sells biased toward bottles bought in the first ~2 years of history (median 2.0yr-from-history-start)
- Newcomer: 12 sold bottles in March 2025, also early-history-biased

### Multi-bottle events (`iEvent` / `EventTitle` / `EventDate` columns in notes.csv)
- Veteran: 52 verticals across 6 themes, content coherent with theme (e.g. "Rousseau retrospective" = 6/6 Rousseau)
- Diarist: ~80 monthly book-club Fridays (3-4 bottles each)
- Newcomer: ~76 biweekly Champagne Saturdays (2-3 bottles each)

### Shipments (shared `OrderNumber` across rows)
- Diarist: 215 multi-lot mailing-list shipments, 3-6 different wines per shipment day
- Veteran: ~28 multi-lot orders (auction-buying produces mostly single-lot wins)
- Newcomer: ~35 multi-lot retail/winery orders

### Vintage / wine-name quirks
- Newcomer modal vintage = `1001` (NV) — Champagne dominates
- ≥7 distinct Krug Grande Cuvée édition iWines in newcomer's cellar
- Many newcomer wine names contain a 4-digit year while `Vintage=1001` (NV with édition year embedded in the name)

### Data-quality artifacts
- Veteran: ~7 anachronistic-vintage purchase lots, ~24 fat-finger price typos (10× / 0.1×)
- Diarist: near-zero of either (meticulous data hygiene)

### Signature producers
Each persona's signature-producer pool is over-represented by 23-37%
of total bottles, with **zero overlap** between personas. A good MCP
should be able to characterize each cellar from the producer mix
alone.

## Reproducibility

`generate_fake_cellars.py` is the generator. It reads a real
CellarTracker export (`data/inventory.csv`, `data/purchases.csv`,
`data/consumed.csv`, `data/notes.csv` — not included in this repo) to
construct an authentic wine catalog, then simulates 13/7/3 years of
purchase + consumption history for each persona.

Reproducing requires that source CT data plus `numpy`. Seed is fixed
(`SEED = 20260501`) and `TODAY` is pinned to `2026-05-01` so output is
deterministic.

## Provenance

Generated 2026-05 by Claude (Opus 4.7) as a torture-test corpus for a
Malloy semantic-model-based MCP server.
