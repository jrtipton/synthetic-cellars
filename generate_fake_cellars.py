#!/usr/bin/env python3
"""Generate three synthetic CellarTracker cellars for testing.

Reads real CT exports from ``data/`` to build a catalog of authentic
iWines (one row per distinct wine, with all metadata + a ref_price
median). Simulates 13yr / 7yr / 3yr histories of three personas, each
with distinctive purchase, consumption, sell, and tasting-note patterns.

Output:
    data/fake_cellars/<persona>/{inventory,purchase,consumed,notes}.csv

Schemas mirror the real CT exports column-for-column.
"""
from __future__ import annotations

import csv
import math
import re
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import numpy as np


# ============================================================
# Paths, schema, constants
# ============================================================

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"
OUT_DIR = DATA_DIR / "fake_cellars"
TODAY = date(2026, 5, 1)
SEED = 20260501

INVENTORY_COLS = [
    "iInventory", "Pending", "Barcode", "WineBarcode", "Price", "Size",
    "iWine", "Type", "Vintage", "Wine", "Producer", "Varietal", "Designation",
    "Vineyard", "Country", "Region", "SubRegion", "Appellation", "Note",
    "StoreName", "PurchaseDate", "PurchaseNote", "Location", "Bin",
]

PURCHASE_COLS = [
    "iWine", "iPurchase", "PurchaseDate", "DeliveryDate", "StoreName",
    "Currency", "ExchangeRate", "Price", "NativePrice", "NativePriceCurrency",
    "Quantity", "Remaining", "OrderNumber", "Delivered", "Size", "Vintage",
    "Wine", "Locale", "Type", "Color", "Category", "Producer", "Varietal",
    "MasterVarietal", "Designation", "Vineyard", "Country", "Region",
    "SubRegion", "Appellation",
]

CONSUMED_COLS = [
    "iConsumed", "iWine", "Type", "Consumed", "ConsumedYear", "ConsumedMonth",
    "ConsumedDay", "Size", "ShortType", "Currency", "Value", "Price",
    "Revenue", "RevenueCurrency", "ConsumptionNote", "PurchaseNote",
    "BottleNote", "Location", "Bin", "Vintage", "Wine", "Locale", "Color",
    "Category", "Varietal", "MasterVarietal", "Designation", "Vineyard",
    "Country", "Region", "SubRegion", "Appellation",
]

NOTE_COLS = [
    "iNote", "iWine", "Type", "Vintage", "Wine", "Locale", "Producer",
    "Varietal", "MasterVarietal", "Designation", "Vineyard", "Country",
    "Region", "SubRegion", "Appellation", "TastingDate", "Defective",
    "Views", "fHelpful", "fFavorite", "Rating", "EventLocation", "EventTitle",
    "iEvent", "EventDate", "EventEndDate", "TastingNotes", "fLikeIt",
    "CNotes", "CScore", "LikeVotes", "LikePercent", "Votes", "Comments",
]

META_COLS = [
    "Vintage", "Wine", "Producer", "Region", "SubRegion", "Appellation",
    "Country", "Locale", "Type", "Color", "Category", "Varietal",
    "MasterVarietal", "Designation", "Vineyard",
]


def fmt_date(d: date) -> str:
    return f"{d.month}/{d.day}/{d.year}"


# ============================================================
# Catalog construction
# ============================================================

REGION_KEYS = (
    "Burgundy", "Bordeaux", "Champagne", "Rhône", "Loire", "Alsace",
    "California", "Oregon", "Washington", "Tuscany", "Piedmont", "Veneto",
    "Spain", "Germany", "Australia", "Other",
)


def region_key(region: str, country: str = "") -> str:
    r = (region or "").strip()
    c = (country or "").strip()
    if r in {"Burgundy", "Bordeaux", "Champagne", "Loire", "Alsace",
             "Tuscany", "Piedmont", "Veneto", "California", "Oregon",
             "Washington"}:
        return r
    if r in {"Rhône", "Rhone"}:
        return "Rhône"
    if c == "Spain":
        return "Spain"
    if c == "Germany":
        return "Germany"
    if c == "Australia":
        return "Australia"
    return "Other"


def parse_price(x: str) -> float | None:
    try:
        v = float(x)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def parse_vintage(v: str) -> int:
    try:
        n = int(v)
        return n
    except (ValueError, TypeError):
        return 1001


def read_csv_rows(path: Path) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def infer_window(vintage: int, region: str, color: str, category: str,
                 type_: str) -> tuple[int, int]:
    # NV / unparseable vintage: treat as always in window (drink-anytime).
    if vintage <= 1001:
        return (1, 9999)
    v = vintage
    rl = (region or "").lower()
    cl = (color or "").lower()
    cat = (category or "").lower()
    tl = (type_ or "").lower()
    if "champagne" in rl:
        return (v + 5, v + 14)
    if "burgundy" in rl:
        return (v + 6, v + 13) if "red" in cl else (v + 2, v + 8)
    if "bordeaux" in rl:
        return (v + 10, v + 22)
    if "piedmont" in rl:
        return (v + 10, v + 20)
    if "rhône" in rl or "rhone" in rl:
        return (v + 6, v + 15) if "red" in cl else (v + 1, v + 5)
    if "tuscany" in rl:
        return (v + 6, v + 14)
    if "california" in rl:
        return (v + 5, v + 13) if "red" in cl else (v + 1, v + 5)
    if cat in ("sweet", "fortified") or "port" in tl:
        return (v + 8, v + 30)
    if "white" in cl:
        return (v + 1, v + 5)
    if "rosé" in cl or "rose" in cl:
        return (v, v + 2)
    return (v + 3, v + 9)


class Catalog:
    """One entry per distinct iWine, plus aligned numpy arrays."""

    def __init__(self, meta: list[dict]):
        self.meta = meta
        self.iwine = np.array([m["iWine"] for m in meta])
        self.vint = np.array([m["VintageInt"] for m in meta], dtype=np.int32)
        self.region_k = np.array([m["RegionKey"] for m in meta])
        self.color = np.array([m["Color"] for m in meta])
        self.type_ = np.array([m["Type"] for m in meta])
        self.category = np.array([m["Category"] for m in meta])
        self.ref_price = np.array([m["ref_price"] for m in meta], dtype=np.float64)
        self.window_b = np.array([m["window_begin"] for m in meta], dtype=np.int32)
        self.window_e = np.array([m["window_end"] for m in meta], dtype=np.int32)
        self._index = {iw: i for i, iw in enumerate(self.iwine)}

    def __len__(self):
        return len(self.meta)

    def index_of(self, iwine: str) -> int | None:
        return self._index.get(iwine)


def load_catalog() -> Catalog:
    sources = [
        read_csv_rows(DATA_DIR / "purchases.csv"),
        read_csv_rows(DATA_DIR / "consumed.csv"),
        read_csv_rows(DATA_DIR / "inventory.csv"),
        read_csv_rows(DATA_DIR / "notes.csv"),
    ]

    by_iwine: dict[str, dict] = {}
    for rows in sources:
        for r in rows:
            iw = (r.get("iWine") or "").strip()
            if not iw:
                continue
            slot = by_iwine.setdefault(iw, {"iWine": iw})
            for c in META_COLS:
                if c in r and r[c] and not slot.get(c):
                    slot[c] = r[c].strip()

    # Defaults for missing fields
    for slot in by_iwine.values():
        for c in META_COLS:
            slot.setdefault(c, "")
        if not slot["Color"]:
            slot["Color"] = "Red"
        if not slot["Category"]:
            slot["Category"] = "Dry"

    # ref_price = median of observed Prices and Values across all sources
    price_obs: dict[str, list[float]] = defaultdict(list)
    for rows, cols in (
        (sources[0], ("Price",)),
        (sources[1], ("Price", "Value")),
        (sources[2], ("Price",)),
    ):
        for r in rows:
            iw = (r.get("iWine") or "").strip()
            if not iw:
                continue
            for c in cols:
                p = parse_price(r.get(c, ""))
                if p is not None:
                    price_obs[iw].append(p)

    all_prices = [p for ps in price_obs.values() for p in ps]
    global_med = float(np.median(all_prices)) if all_prices else 50.0

    meta: list[dict] = []
    for iw, slot in by_iwine.items():
        slot["VintageInt"] = parse_vintage(slot["Vintage"])
        slot["RegionKey"] = region_key(slot["Region"], slot["Country"])
        ps = price_obs.get(iw)
        slot["ref_price"] = float(np.median(ps)) if ps else global_med
        b, e = infer_window(
            slot["VintageInt"], slot["Region"], slot["Color"],
            slot["Category"], slot["Type"],
        )
        slot["window_begin"] = b
        slot["window_end"] = e
        meta.append(slot)

    return Catalog(meta)


# ============================================================
# Personas
# ============================================================

PERSONAS: dict[str, dict] = {
    "a_auction_veteran": {
        "target_inventory": 5000,
        "history_years": 13,
        "buy_rate": 60,        # bottles/month (lots = buy_rate/3)
        "drink_rate": 20,      # events/month (reduced; ~2/mo come from verticals)
        "note_rate": 0.10,
        "meal_note_rate": 0.05,
        "inventory_sweep_rate": 0.08,
        "window_adherence": 0.10,
        "data_error_rate": 0.02,
        # Auction season skew: hot Sept/Oct + Hospices May, dead summer + Dec.
        "month_multipliers": [0.9, 0.9, 1.0, 1.0, 1.3, 1.0, 0.7, 0.7, 1.4, 1.4, 1.1, 0.6],
        "signature_bias": 0.20,
        "signature_producers": [
            "Latour", "Lafite", "Margaux", "Mouton", "Haut-Brion",
            "Léoville-Las Cases", "Pichon Longueville", "Cos d'Estournel",
            "Lynch-Bages", "Montrose", "Ducru-Beaucaillou", "Calon-Ségur",
            "Conterno", "Bartolo Mascarello", "Soldera",
            "Sassicaia", "Ornellaia", "Bruno Giacosa", "Vietti",
        ],
        "events": [{
            "name": "vertical",
            "frequency": "quarterly",
            "bottles": (4, 6),
            "themes": [
                {"title": "Rousseau retrospective",
                 "filter": {"producer_contains": "Rousseau"}},
                {"title": "Bordeaux first-growth flight",
                 "filter": {"producer_in": ["Lafite", "Latour", "Margaux", "Mouton", "Haut-Brion"]}},
                {"title": "California Cab vertical",
                 "filter": {"region_in": ["California"], "varietal_contains": "Cabernet"}},
                {"title": "Burgundy GC night",
                 "filter": {"region_in": ["Burgundy"], "appellation_contains": "Grand Cru"}},
                {"title": "Piedmont Riserva tasting",
                 "filter": {"region_in": ["Piedmont"], "designation_contains": "Riserva"}},
                {"title": "Right Bank vertical",
                 "filter": {"appellation_contains": "Saint-Émilion"}},
            ],
            "location": "the cellar room",
        }],
        "recurring_specials": [],
        "spouse": "Ellen",
        "kids": [],
        "friends": ["the Hartwells", "Bill", "Marcus", "Tess and David"],
        "store_mix": {
            "auction": (0.70, ["Hart Davis Hart", "Acker Merrall",
                               "Zachys", "WineBid", "K&L Auctions",
                               "Spectrum Wine Auctions"]),
            "retail":  (0.18, ["K&L Wine Merchants", "Total Wine", "Wally's",
                               "Premier Cru", "Chambers Street", "Garagiste"]),
            "winery":  (0.04, ["Winery Direct", "Mailing List"]),
            "local":   (0.06, ["Liner & Elsen", "Vinopolis", "Storyteller"]),
            "gift":    (0.02, ["Gift"]),
        },
        "region_weights_early": {
            "Bordeaux": 4.0, "California": 4.0, "Tuscany": 2.5,
            "Burgundy": 1.0, "Piedmont": 0.8, "Champagne": 0.6,
            "Rhône": 1.2, "Oregon": 0.3, "Washington": 0.3,
            "Loire": 0.3, "Alsace": 0.2, "Spain": 0.4,
            "Germany": 0.3, "Australia": 0.3, "Other": 0.5,
        },
        "region_weights_late": {
            "Burgundy": 5.0, "Piedmont": 3.0, "Champagne": 2.5,
            "Bordeaux": 1.0, "California": 1.0, "Tuscany": 0.8,
            "Rhône": 1.5, "Oregon": 0.6, "Washington": 0.4,
            "Loire": 0.5, "Alsace": 0.4, "Spain": 0.3,
            "Germany": 0.4, "Australia": 0.2, "Other": 0.5,
        },
        "favorite_seeds": [],
        "locations": ["Cellar Room", "Off-site Storage", "Kitchen Cooler"],
        "sell_events": [{
            "date": date(2022, 6, 15),
            "store": "Zachys Wine Auctions",
            "n": 400,
            "regions": ["California", "Bordeaux", "Tuscany"],
            "bought_before": date(2018, 1, 1),
            "prefer_early": True,
            "note": "Zachys consignment, June 2022",
        }],
    },
    "b_allocation_diarist": {
        "target_inventory": 2500,
        "history_years": 7,
        "buy_rate": 56,
        "drink_rate": 14,    # reduced; ~4/mo come from book club + specials
        "note_rate": 0.55,
        "meal_note_rate": 0.80,
        "inventory_sweep_rate": 0.0,
        "window_adherence": 0.85,
        "data_error_rate": 0.004,
        # Allocation rhythm: spring + fall releases, lull in summer + Dec.
        "month_multipliers": [0.7, 0.7, 1.5, 1.5, 1.0, 0.8, 0.8, 0.9, 1.3, 1.3, 1.0, 0.5],
        "signature_bias": 0.22,
        "signature_producers": [
            "Hirsch", "Rhys", "Whitcraft", "Sandlands", "Kistler",
            "Aubert", "Marcassin", "Kosta Browne", "Patricia Green",
            "Rivers-Marie", "Cobb",
            "Drouhin Oregon", "Cameron", "Beaux Frères", "Eyrie",
            "Bérêche", "Egly-Ouriet", "Larmandier-Bernier",
            "Chartogne-Taillet", "Pierre Péters",
        ],
        "events": [{
            "name": "book club",
            "frequency": "monthly",
            "bottles": (3, 4),
            "themes": [{"title": "book club Friday", "filter": {}}],
            "location": "our place",
            "weekday": 4,  # Friday
        }],
        "recurring_specials": [
            {"date": (3, 17), "title": "Iris's birthday",
             "criteria": {"region_in": ["Burgundy"], "color": "Red"},
             "location": "home"},
            {"date": (10, 11), "title": "Theo's birthday",
             "criteria": {"region_in": ["Burgundy"], "color": "Red"},
             "location": "home"},
            {"date": (6, 28), "title": "Anniversary",
             "criteria": {"region_in": ["Champagne"]},
             "location": "home"},
        ],
        "spouse": "Sam",
        "kids": ["Iris", "Theo"],
        "friends": ["the Nguyen family", "Kate and Josh", "book club", "Mira"],
        "store_mix": {
            "winery":  (0.55, ["Winery Direct", "Mailing List",
                               "Cellar Allocation", "Mailer", "List Allocation"]),
            "retail":  (0.25, ["K&L Wine Merchants", "Chambers Street",
                               "Astor Wines", "Flatiron Wines", "Liner & Elsen"]),
            "auction": (0.05, ["WineBid", "K&L Auctions"]),
            "local":   (0.10, ["Liner & Elsen", "Vinopolis", "Bottleshop"]),
            "gift":    (0.05, ["Gift"]),
        },
        "region_weights_early": {
            "California": 3.5, "Oregon": 3.0, "Champagne": 3.5,
            "Burgundy": 3.0, "Washington": 1.0, "Bordeaux": 0.5,
            "Rhône": 0.8, "Loire": 1.0, "Alsace": 0.8,
            "Tuscany": 0.5, "Piedmont": 0.5, "Spain": 0.4,
            "Germany": 0.5, "Australia": 0.3, "Other": 0.4,
        },
        "favorite_seeds": [],
        "locations": ["Home Cellar", "Off-site Storage", "Kitchen"],
        "sell_events": [],
    },
    "c_champagne_newcomer": {
        "target_inventory": 1000,
        "history_years": 3,
        "buy_rate": 50,
        "drink_rate": 8,     # reduced; biweekly Champagne Saturdays add ~5/mo
        "note_rate": 0.30,
        "meal_note_rate": 0.35,
        "inventory_sweep_rate": 0.0,
        "window_adherence": 0.40,
        "data_error_rate": 0.015,
        # Holiday gifting + early-summer Champagne kick. Quiet late winter.
        "month_multipliers": [0.6, 0.5, 0.6, 0.7, 1.0, 1.3, 1.0, 1.0, 1.0, 1.1, 1.4, 1.8],
        "signature_bias": 0.25,
        "signature_producers": [
            "Roses de Jeanne",
            "Ulysse Collin", "Jacques Lassaigne", "Marie-Courtin",
            "Olivier Horiot", "Vouette et Sorbée", "Savart",
            "Jérôme Prévost", "Emmanuel Brochet", "David Léclapart",
            "Tarlant", "Bourgeois-Diaz", "Vilmart", "Doyard",
            "Pierre Péters", "Bollinger",
        ],
        "events": [{
            "name": "Champagne Saturday",
            "frequency": "biweekly",
            "bottles": (2, 3),
            "themes": [
                {"title": "Champagne Saturday", "filter": {}},
                {"title": "blind Champagne flight", "filter": {}},
                {"title": "Saturday bubbles", "filter": {}},
                {"title": "Krug édition night", "filter": {"producer_contains": "Krug"}},
            ],
            "location": "home",
            "weekday": 5,  # Saturday
            "criteria": {"region_in": ["Champagne"]},
        }],
        "recurring_specials": [],
        "spouse": "Alex",
        "kids": [],
        "friends": ["Robin", "Casey", "the dinner crew"],
        "store_mix": {
            "retail":  (0.45, ["K&L Wine Merchants", "Flatiron Wines",
                               "Astor Wines", "Chambers Street", "Bottle Barn"]),
            "winery":  (0.20, ["Mailing List", "Winery Direct"]),
            "auction": (0.20, ["WineBid", "K&L Auctions"]),
            "local":   (0.10, ["Bottleshop", "Liner & Elsen"]),
            "gift":    (0.05, ["Gift"]),
        },
        "region_weights_early": {
            "Champagne": 8.0, "Burgundy": 2.0, "California": 1.5,
            "Oregon": 1.0, "Loire": 1.0, "Alsace": 0.6,
            "Bordeaux": 0.4, "Rhône": 0.5, "Tuscany": 0.4,
            "Piedmont": 0.5, "Washington": 0.3, "Spain": 0.3,
            "Germany": 0.4, "Australia": 0.2, "Other": 0.4,
        },
        "favorite_seeds": [
            r"Krug.*Grande Cuvée.*Edition",
            r"Roses de Jeanne.*R\d{2}",
            r"Ulysse Collin.*\(20\d{2}\)",
            r"Bérêche.*Brut Réserve$",
            r"Bollinger.*Special Cuvée",
            r"Joliette.*Millésime",
        ],
        "locations": ["Apartment Cellar", "Closet Cooler"],
        "sell_events": [{
            "date": date(2025, 3, 12),
            "store": "WineBid",
            "n": 12,
            "regions": [],
            "bought_before": None,
            "prefer_early": True,
            "note": "WineBid consignment, March 2025",
        }],
    },
}


# ============================================================
# Sampling helpers
# ============================================================

def normalize(w: np.ndarray) -> np.ndarray:
    s = w.sum()
    if s <= 0:
        return np.full(len(w), 1.0 / len(w)) if len(w) else w
    return w / s


def lerp_weights(early: dict, late: dict | None, t: float) -> dict:
    if late is None:
        return dict(early)
    keys = set(early) | set(late)
    return {k: (1 - t) * early.get(k, 0.0) + t * late.get(k, 0.0) for k in keys}


def region_weight_array(region_keys: np.ndarray, weights: dict) -> np.ndarray:
    other = weights.get("Other", 0.5)
    return np.array(
        [weights.get(k, other) for k in region_keys], dtype=np.float64
    )


def add_months(d: date, months: int) -> date:
    m = d.month - 1 + months
    y = d.year + m // 12
    m = m % 12 + 1
    return date(y, m, 1)


# ============================================================
# Favorites
# ============================================================

def build_favorites(catalog: Catalog, persona: dict, rng) -> list[int]:
    """Return list of catalog indices."""
    weights = persona["region_weights_early"]
    region_w = region_weight_array(catalog.region_k, weights)

    favs: set[int] = set()

    # 12 from cheapest quartile
    q1 = float(np.quantile(catalog.ref_price, 0.25))
    cheap = np.where(catalog.ref_price <= q1)[0]
    if len(cheap):
        w = region_w[cheap]
        w = normalize(w)
        n = min(12, len(cheap))
        sel = rng.choice(len(cheap), size=n, replace=False, p=w)
        favs.update(int(cheap[i]) for i in np.atleast_1d(sel))

    # 6 from priciest decile
    q9 = float(np.quantile(catalog.ref_price, 0.90))
    rich = np.where(catalog.ref_price >= q9)[0]
    if len(rich):
        w = region_w[rich]
        w = normalize(w)
        n = min(6, len(rich))
        sel = rng.choice(len(rich), size=n, replace=False, p=w)
        favs.update(int(rich[i]) for i in np.atleast_1d(sel))

    # Favorite seeds (regex match on Wine name)
    for pat in persona.get("favorite_seeds") or []:
        regex = re.compile(pat)
        for i, m in enumerate(catalog.meta):
            if regex.search(m["Wine"] or ""):
                favs.add(i)

    return sorted(favs)


# ============================================================
# Purchase simulation
# ============================================================

def pick_size(color: str, category: str, region: str, type_: str, rng) -> str:
    cl = (color or "").lower()
    cat = (category or "").lower()
    rl = (region or "").lower()
    tl = (type_ or "").lower()
    classic_red = "red" in cl and any(
        x in rl for x in ("burgundy", "bordeaux", "piedmont", "rhône", "rhone")
    )
    if cat in ("sweet", "fortified") or "port" in tl:
        p_half, p_mag = 0.25, 0.02
    elif "sparkling" in cat or "champagne" in rl or "sparkling" in tl:
        p_half, p_mag = 0.08, 0.12
    elif classic_red:
        p_half, p_mag = 0.01, 0.08
    elif "california" in rl and "red" in cl:
        p_half, p_mag = 0.005, 0.06
    else:
        p_half, p_mag = 0.01, 0.02
    r = rng.random()
    if r < p_half:
        return "375ml"
    if r < p_half + p_mag:
        return "1.5L"
    return "750ml"


def size_factor(size: str) -> float:
    return {"375ml": 0.5, "750ml": 1.0, "1.5L": 2.0, "3.0L": 4.0}.get(size, 1.0)


def pick_delivery_gap(region: str, store_kind: str, rng) -> int:
    rl = (region or "").lower()
    if store_kind == "retail" and any(
        x in rl for x in ("bordeaux", "burgundy", "rhône", "rhone")
    ):
        if rng.random() < 0.35:
            return int(rng.uniform(540, 1100))
    if store_kind == "winery":
        return int(rng.uniform(60, 420))
    return int(rng.uniform(7, 90))


# Annual inflation by region. Bottles bought N years ago cost
# ref_price / (1+rate)^N (in then-dollars), so 2014 Burgundy is much cheaper
# than 2026 Burgundy in nominal terms.
INFLATION_RATES = {
    "Burgundy":   0.10,
    "Bordeaux":   0.05,
    "Champagne":  0.06,
    "California": 0.07,
    "Piedmont":   0.07,
    "Tuscany":    0.04,
    "Rhône":      0.04,
    "Oregon":     0.05,
    "Washington": 0.04,
    "Loire":      0.03,
    "Alsace":     0.03,
    "Spain":      0.02,
    "Germany":    0.03,
    "Australia":  0.03,
    "Other":      0.03,
}


def inflation_factor(region_k: str, purchase_year: int) -> float:
    rate = INFLATION_RATES.get(region_k, 0.03)
    return (1.0 + rate) ** (purchase_year - TODAY.year)


def pick_store(persona: dict, rng) -> tuple[str, str]:
    mix = persona["store_mix"]
    kinds = list(mix.keys())
    probs = np.array([mix[k][0] for k in kinds], dtype=np.float64)
    probs = probs / probs.sum()
    kind = str(rng.choice(kinds, p=probs))
    name = str(rng.choice(mix[kind][1]))
    return kind, name


def simulate_purchases(catalog: Catalog, persona: dict, fav_idx: list[int], rng):
    history_years = persona["history_years"]
    start = add_months(date(TODAY.year, TODAY.month, 1), -history_years * 12)
    months = history_years * 12
    fav_set = set(fav_idx)
    fav_arr = np.array(fav_idx, dtype=np.int64) if fav_idx else np.array([], dtype=np.int64)

    early = persona["region_weights_early"]
    late = persona.get("region_weights_late")

    # Pre-compute signature producer indices once per persona.
    sig_patterns = persona.get("signature_producers") or []
    sig_bias = float(persona.get("signature_bias", 0.0))
    sig_mask = np.zeros(len(catalog), dtype=bool)
    for pat in sig_patterns:
        pl = pat.lower()
        for i, m in enumerate(catalog.meta):
            if pl in (m["Producer"] or "").lower():
                sig_mask[i] = True
    sig_idx = np.where(sig_mask)[0]

    purchases: list[dict] = []

    for i in range(months):
        cur = add_months(start, i)
        t = i / max(months - 1, 1)
        w_today = lerp_weights(early, late, t)
        full_w = region_weight_array(catalog.region_k, w_today)

        buy_year = cur.year
        era_mask = (catalog.vint <= buy_year + 1) | (catalog.vint == 1001)
        era_idx = np.where(era_mask)[0]
        era_w = full_w[era_idx]
        era_p = normalize(era_w)

        if len(fav_arr):
            fav_era_mask = era_mask[fav_arr]
            fav_era = fav_arr[fav_era_mask]
        else:
            fav_era = fav_arr
        if len(fav_era):
            fav_w = full_w[fav_era]
            fav_p = normalize(fav_w)
        else:
            fav_p = None

        if len(sig_idx):
            sig_era_mask = era_mask[sig_idx]
            sig_era = sig_idx[sig_era_mask]
        else:
            sig_era = sig_idx
        sig_p = normalize(full_w[sig_era]) if len(sig_era) else None

        month_mult = (persona.get("month_multipliers") or [1.0] * 12)[cur.month - 1]
        n_lots = int(rng.poisson(persona["buy_rate"] * month_mult / 3.0))
        for _ in range(n_lots):
            if rng.random() < persona["data_error_rate"] * 0.5:
                # Anachronistic vintage: ignore era filter
                p = normalize(full_w)
                ci = int(rng.choice(len(catalog), p=p))
            elif sig_p is not None and rng.random() < sig_bias:
                ci = int(rng.choice(sig_era, p=sig_p))
            elif rng.random() < 0.25 and fav_p is not None:
                ci = int(rng.choice(fav_era, p=fav_p))
            else:
                ci = int(rng.choice(era_idx, p=era_p))

            meta = catalog.meta[ci]
            qty = int(np.clip(rng.lognormal(1.0, 0.6), 1, 12))
            store_kind, store_name = pick_store(persona, rng)

            ref = float(catalog.ref_price[ci])
            if not math.isfinite(ref) or ref <= 0:
                ref = 50.0
            # Region-aware price evolution: 2014 Burgundy nominal price
            # is much lower than 2026 Burgundy nominal price.
            inflation = inflation_factor(meta["RegionKey"], buy_year)
            price = ref * inflation * float(rng.uniform(0.85, 1.15))
            if rng.random() < persona["data_error_rate"] * 0.5:
                price *= float(rng.choice([10.0, 0.1]))
            if store_kind == "gift":
                price = 0.0

            size = pick_size(meta["Color"], meta["Category"], meta["Region"],
                             meta["Type"], rng)
            unit_price = round(price * size_factor(size), 2)

            day = int(rng.integers(1, 28))
            try:
                p_date = cur.replace(day=day)
            except ValueError:
                p_date = cur
            gap = pick_delivery_gap(meta["Region"], store_kind, rng)
            d_date = p_date + timedelta(days=gap)

            purchases.append({
                "ci": ci,
                "iWine": meta["iWine"],
                "PurchaseDate": p_date,
                "DeliveryDate": d_date,
                "StoreName": store_name,
                "store_kind": store_kind,
                "Price": unit_price,
                "Quantity": qty,
                "Size": size,
            })

    return purchases


def batch_shipments(purchases: list[dict], rng,
                    ship_id_start: int = 500_000_000) -> int:
    """Group winery / mailing-list lots in the same month into shipments
    of 3-6 lots that share OrderNumber + PurchaseDate + DeliveryDate +
    StoreName. Mutates ``purchases`` in place. Returns next free ship id.

    Real CT data: a winery shipment lands once with multiple wines under
    the same OrderId. Without batching, every lot has its own OrderId,
    which makes shipment-level analysis impossible.
    """
    next_id = ship_id_start
    groups: dict[tuple[int, int], list[int]] = defaultdict(list)
    for idx, p in enumerate(purchases):
        if p["store_kind"] != "winery":
            continue
        ym = (p["PurchaseDate"].year, p["PurchaseDate"].month)
        groups[ym].append(idx)

    for ym, idxs in groups.items():
        if len(idxs) < 2:
            continue
        order = rng.permutation(len(idxs))
        idxs = [idxs[i] for i in order]
        i = 0
        while i < len(idxs):
            n = min(int(rng.integers(3, 7)), len(idxs) - i)
            if n < 2:
                i += 1
                continue
            chunk = idxs[i:i + n]
            ship_id = next_id
            next_id += 1
            anchor = purchases[chunk[0]]
            shared_pdate = anchor["PurchaseDate"]
            shared_store = anchor["StoreName"]
            for cj in chunk:
                lot = purchases[cj]
                lot["PurchaseDate"] = shared_pdate
                lot["StoreName"] = shared_store
                # Re-anchor delivery off the shared purchase date so the
                # gap-distribution stays sensible per region.
                gap = (lot["DeliveryDate"] - lot["PurchaseDate"]).days
                lot["DeliveryDate"] = shared_pdate + timedelta(days=max(gap, 0))
                lot["OrderNumber"] = f"OrderId:{ship_id}"
                lot["shipment_id"] = ship_id
            i += n
    return next_id


# ============================================================
# Bottle expansion
# ============================================================

BOTTLE_NOTES = [
    "bin-soiled label",
    "top-shoulder fill",
    "slightly depressed cork",
    "signs of past seepage",
    "nicked capsule",
    "lightly scuffed label",
]


def make_bin(rng) -> str:
    if rng.random() < 0.10:
        return ""
    n = int(rng.integers(1, 100))
    letter = chr(ord("A") + int(rng.integers(0, 6)))
    return f"{n:02d}-{letter}"


def expand_to_bottles(purchases: list[dict], persona: dict, rng):
    locations = persona["locations"]
    primary = locations[0]
    other_locs = locations[1:] if len(locations) > 1 else locations
    bottles: list[dict] = []
    next_iinv = 200_000_000
    next_ipur = 100_000_000
    for p in purchases:
        ipur = next_ipur
        next_ipur += 1
        p["iPurchase"] = ipur
        for _ in range(p["Quantity"]):
            iinv = next_iinv
            next_iinv += 1
            if rng.random() < 0.5 or len(other_locs) == 0:
                loc = primary
            else:
                loc = str(rng.choice(other_locs))
            bin_ = make_bin(rng)
            bnote = ""
            if p["store_kind"] == "auction" and rng.random() < 0.15:
                bnote = str(rng.choice(BOTTLE_NOTES))
            order_number = p.get("OrderNumber") or f"OrderId:{ipur}"
            bottles.append({
                "iInventory": iinv,
                "iPurchase": ipur,
                "ci": p["ci"],
                "iWine": p["iWine"],
                "PurchaseDate": p["PurchaseDate"],
                "DeliveryDate": p["DeliveryDate"],
                "StoreName": p["StoreName"],
                "store_kind": p["store_kind"],
                "Price": p["Price"],
                "Size": p["Size"],
                "Location": loc,
                "Bin": bin_,
                "BottleNote": bnote,
                "PurchaseNote": order_number,
                "OrderNumber": order_number,
                "consumed_date": None,
                "consumed_kind": None,
                "consumption_note": "",
                "tasting_note": None,
            })
    return bottles


# ============================================================
# Consumption simulation
# ============================================================

WEEKNIGHT_MEALS = [
    "weeknight pasta", "roast chicken", "grilled salmon",
    "salad and leftovers", "burgers", "sheet-pan dinner", "ramen", "tacos",
    "Tuesday risotto", "stir-fry",
]
WEEKEND_MEALS = [
    "Saturday roast", "Sunday braise", "homemade pizza", "lamb chops",
    "duck breast", "ribeye on the grill", "porchetta", "paella",
    "slow-cooked short ribs",
]
HOLIDAY_NAMES = {
    (12, 24): "Christmas Eve",
    (12, 25): "Christmas",
    (12, 31): "New Year's Eve",
    (1, 1): "New Year's Day",
    (11, 28): "Thanksgiving",
    (7, 4): "the Fourth",
    (3, 31): "Easter",
    (5, 12): "Mother's Day",
    (6, 16): "Father's Day",
}
RESTAURANTS = [
    "Le Pigeon", "Beast", "Coquine", "Higgins", "Ava Gene's",
    "Castagna", "Genoa", "Toro Bravo", "Nostrana",
]


def pick_context(persona: dict, dt: date, rng) -> tuple[str, bool, list[str]]:
    md = (dt.month, dt.day)
    if md in HOLIDAY_NAMES:
        kind = "holiday"
    else:
        is_weekend = dt.weekday() >= 5
        r = rng.random()
        if r < 0.04:
            kind = "special"
        elif is_weekend:
            kind = "weekend"
        else:
            kind = "weeknight"

    kids_present = False
    if persona.get("kids"):
        if kind == "weeknight":
            kids_present = rng.random() < 0.7
        elif kind == "holiday":
            kids_present = rng.random() < 0.7
        elif kind == "weekend":
            kids_present = rng.random() < 0.5
        else:
            kids_present = rng.random() < 0.2

    people: list[str] = []
    spouse = persona.get("spouse")
    if spouse and rng.random() < 0.95:
        people.append(spouse)
    if kids_present:
        people.extend(persona["kids"])

    p_friends = {"holiday": 0.7, "special": 0.5,
                 "weekend": 0.3, "weeknight": 0.05}[kind]
    friends = persona.get("friends") or []
    if friends and rng.random() < p_friends:
        n = 2 if kind == "holiday" else 1
        n = min(n, len(friends))
        idx = rng.choice(len(friends), size=n, replace=False)
        people.extend(friends[i] for i in np.atleast_1d(idx))

    return kind, kids_present, people


def compute_drink_weights(prices, color, type_arr, win_b, win_e,
                          dt: date, kind: str, kids: bool, adherence: float):
    n = len(prices)
    if kids:
        occ = 60.0 / (prices + 30.0)
    elif kind in ("special", "holiday"):
        occ = np.clip(prices / 80.0, 0.5, 8.0)
    elif kind == "weekend":
        occ = np.full(n, 1.3)
    else:
        occ = np.ones(n)

    is_white = color == "White"
    is_red = color == "Red"
    is_rose = (color == "Rosé") | (color == "Rose")
    # Sparkling detection works on Type column ("White - Sparkling" etc.)
    is_sparkle = np.array(["Sparkling" in (t or "") for t in type_arr])

    season = np.ones(n)
    m = dt.month
    if 5 <= m <= 9:
        s = is_white | is_rose | is_sparkle
        season = np.where(s, season * 2.2, season * 0.8)
    if m in (11, 12, 1, 2):
        season = np.where(is_red, season * 1.6, season)
    if m in (11, 12, 1):
        season = np.where(is_sparkle, season * 2.5, season)

    yr = dt.year
    in_win = (yr >= win_b) & (yr <= win_e)
    win = np.where(in_win, 1.0 + 3.0 * adherence, 1.0 - 0.7 * adherence)
    win = np.clip(win, 0.05, None)
    return occ * season * win


# ============================================================
# Notes
# ============================================================

RED_NOSE = ["dark cherry", "raspberry", "violets", "tobacco leaf",
            "smoked meat", "graphite", "wet stone", "cedar",
            "blackberry", "rose petal", "iron filings", "forest floor"]
RED_PALATE = ["taut tannins", "bright acidity", "long savory finish",
              "polished texture", "pretty fruit core", "earthy depth",
              "iron and minerals", "structured grip"]
WHITE_NOSE = ["lemon zest", "white flowers", "green apple", "stone fruit",
              "wet wool", "honeycomb", "chalk", "ginger", "yellow plum"]
WHITE_PALATE = ["nervy acidity", "saline finish", "creamy mid-palate",
                "tight focus", "ripe pear", "lingering minerality",
                "vibrant cut"]
SPARKLING_NOSE = ["brioche", "lemon curd", "toasted almond", "white peach",
                  "crushed chalk", "yellow apple", "iodine", "biscuit"]
SPARKLING_PALATE = ["fine mousse", "cutting acidity", "saline grip",
                    "rich autolysis", "pinpoint bead", "long chalky finish"]
ROSE_NOSE = ["watermelon", "pink grapefruit", "wild strawberry", "wet stone"]
ROSE_PALATE = ["bone-dry", "vivid acidity", "savory finish", "lingering pith"]
SWEET_NOSE = ["botrytis", "marmalade", "saffron", "candied apricot", "honey"]
SWEET_PALATE = ["unctuous", "balanced acidity", "long finish",
                "lifted by acidity", "sea salt"]

VERDICTS_GOOD = ["Singing right now.", "Drinking beautifully.",
                 "Hits its stride.", "Outstanding bottle.",
                 "Better than expected."]
VERDICTS_MID = ["Solid showing.", "On track.", "Good but not memorable.",
                "Needs more time.", "Pleasant if uncomplicated."]
VERDICTS_BAD = ["Disappointing.", "Past its prime.",
                "Tired and sloppy.", "Just not for me."]
SUPERLATIVES = [" A standout.", " Bottle of the year so far.",
                " One of the best of this wine I've ever had.",
                " Keeps showing more in the glass."]


def make_tasting_note(meta: dict, rng) -> tuple[float, str]:
    rating = float(np.clip(rng.normal(91.0, 2.5), 78, 99))
    color = (meta.get("Color") or "").strip()
    type_ = (meta.get("Type") or "").strip()
    cat = (meta.get("Category") or "").lower()

    if "Sparkling" in type_:
        nose, pal = SPARKLING_NOSE, SPARKLING_PALATE
    elif color == "Red":
        nose, pal = RED_NOSE, RED_PALATE
    elif color in ("Rosé", "Rose"):
        nose, pal = ROSE_NOSE, ROSE_PALATE
    elif cat in ("sweet", "fortified"):
        nose, pal = SWEET_NOSE, SWEET_PALATE
    else:
        nose, pal = WHITE_NOSE, WHITE_PALATE

    if rating <= 87:
        return rating, str(rng.choice(VERDICTS_BAD))

    n_nose = int(rng.integers(2, 4))
    n_pal = int(rng.integers(2, 4))
    nose_terms = ", ".join(rng.choice(nose, size=n_nose, replace=False))
    pal_terms = ", ".join(rng.choice(pal, size=n_pal, replace=False))
    if rating >= 95:
        verdict = str(rng.choice(VERDICTS_GOOD)) + str(rng.choice(SUPERLATIVES))
    elif rating >= 92:
        verdict = str(rng.choice(VERDICTS_GOOD))
    else:
        verdict = str(rng.choice(VERDICTS_MID))
    return rating, f"Nose of {nose_terms}. Palate shows {pal_terms}. {verdict}"


def make_consumption_note(kind: str, dt: date, people: list[str], rng) -> str:
    people_str = " and ".join(people) if people else "alone"
    md = (dt.month, dt.day)
    if kind == "holiday":
        holiday = HOLIDAY_NAMES.get(md, "holiday")
        meal = str(rng.choice(WEEKEND_MEALS + ["roast goose", "prime rib"]))
        return f"{holiday} — {meal} with {people_str}"
    if kind == "special":
        if rng.random() < 0.5:
            return f"Dinner at {rng.choice(RESTAURANTS)} with {people_str}"
        return f"Anniversary dinner with {people_str}"
    if kind == "weekend":
        return f"{rng.choice(WEEKEND_MEALS)} with {people_str}"
    return f"{rng.choice(WEEKNIGHT_MEALS)} with {people_str}"


# ============================================================
# Consumption + sells
# ============================================================

# ============================================================
# Tasting events (multi-bottle nights with shared iEvent)
# ============================================================

def event_dates_in_range(freq: str, start: date, end: date,
                         weekday: int | None = None) -> list[date]:
    out: list[date] = []
    if freq == "monthly":
        cur = date(start.year, start.month, 1)
        while cur <= end:
            if weekday is not None:
                d = cur
                while d.weekday() != weekday:
                    d += timedelta(days=1)
            else:
                d = cur.replace(day=15)
            if start <= d <= end:
                out.append(d)
            cur = add_months(cur, 1)
    elif freq == "biweekly":
        d = start
        if weekday is not None:
            while d.weekday() != weekday:
                d += timedelta(days=1)
        while d <= end:
            out.append(d)
            d += timedelta(days=14)
    elif freq == "quarterly":
        m0 = ((start.month - 1) // 3) * 3 + 1
        cur = date(start.year, m0, 1)
        while cur <= end:
            d = cur.replace(day=15)
            if start <= d <= end:
                out.append(d)
            cur = add_months(cur, 3)
    return out


def _meta_matches(meta: dict, crit: dict) -> bool:
    if not crit:
        return True
    if "region_in" in crit and crit["region_in"]:
        regs = set(crit["region_in"])
        if meta["RegionKey"] not in regs and meta["Region"] not in regs:
            return False
    if "color" in crit and meta["Color"] != crit["color"]:
        return False
    if "producer_contains" in crit:
        if crit["producer_contains"].lower() not in (meta["Producer"] or "").lower():
            return False
    if "producer_in" in crit:
        prod = (meta["Producer"] or "").lower()
        if not any(p.lower() in prod for p in crit["producer_in"]):
            return False
    if "varietal_contains" in crit:
        v = ((meta["Varietal"] or "") + " " + (meta["MasterVarietal"] or "")).lower()
        if crit["varietal_contains"].lower() not in v:
            return False
    if "appellation_contains" in crit:
        if crit["appellation_contains"].lower() not in (meta["Appellation"] or "").lower():
            return False
    if "designation_contains" in crit:
        if crit["designation_contains"].lower() not in (meta["Designation"] or "").lower():
            return False
    if "vintage" in crit and meta["VintageInt"] != crit["vintage"]:
        return False
    if "vintage_in" in crit and meta["VintageInt"] not in crit["vintage_in"]:
        return False
    return True


def _filter_by_criteria(elig: np.ndarray, bottles: list[dict],
                        catalog: "Catalog", criteria: dict) -> np.ndarray:
    if not criteria:
        return elig
    elig = elig.copy()
    for i in np.where(elig)[0]:
        meta = catalog.meta[bottles[i]["ci"]]
        if not _meta_matches(meta, criteria):
            elig[i] = False
    return elig


def simulate_events(bottles: list[dict], catalog: "Catalog",
                    persona: dict, rng, ievent_start: int = 90_000_000) -> int:
    """Claim bottles for multi-bottle tasting events. Mutates bottles."""
    if not persona.get("events"):
        return ievent_start
    history_years = persona["history_years"]
    start = add_months(date(TODAY.year, TODAY.month, 1), -history_years * 12)

    delivery_ord = np.array(
        [b["DeliveryDate"].toordinal() for b in bottles], dtype=np.int64
    )
    consumed = np.array(
        [b["consumed_date"] is not None for b in bottles], dtype=bool
    )

    next_id = ievent_start
    for cfg in persona["events"]:
        dates = event_dates_in_range(
            cfg["frequency"], start, TODAY, weekday=cfg.get("weekday")
        )
        themes = cfg["themes"]
        for ev_date in dates:
            ev_ord = ev_date.toordinal()
            base_elig = (~consumed) & (delivery_ord <= ev_ord)
            base_elig = _filter_by_criteria(base_elig, bottles, catalog,
                                            cfg.get("criteria") or {})
            if not base_elig.any():
                continue
            n_min, n_max = cfg["bottles"]
            # Try themes in shuffled order; pick the first that yields ≥ n_min
            # eligible bottles. If none do, fall back to no theme filter.
            order = rng.permutation(len(themes))
            chosen_theme = None
            chosen_elig = None
            for ti in order:
                t_cfg = themes[int(ti)]
                t_filter = t_cfg.get("filter") or {}
                if not t_filter:
                    chosen_theme = t_cfg
                    chosen_elig = base_elig
                    break
                t_elig = _filter_by_criteria(base_elig.copy(), bottles, catalog, t_filter)
                if int(t_elig.sum()) >= n_min:
                    chosen_theme = t_cfg
                    chosen_elig = t_elig
                    break
            if chosen_elig is None:
                # Fallback: ignore the theme but mark the event with the
                # picked theme's title anyway, so the title is decoupled
                # from contents (this should be rare).
                chosen_theme = themes[int(order[0])]
                chosen_elig = base_elig
            elig_idx = np.where(chosen_elig)[0]
            n = min(int(rng.integers(n_min, n_max + 1)), len(elig_idx))
            picks = rng.choice(elig_idx, size=n, replace=False)

            title = f"{chosen_theme['title']} — {ev_date.strftime('%B %Y')}"
            location = cfg.get("location", "")
            ievent_id = str(next_id)
            next_id += 1

            for raw in np.atleast_1d(picks):
                i = int(raw)
                consumed[i] = True
                b = bottles[i]
                b["consumed_date"] = ev_date
                b["consumed_kind"] = "Drank from my cellar"
                b["consumption_note"] = title
                rating, text = make_tasting_note(catalog.meta[b["ci"]], rng)
                b["tasting_note"] = {
                    "rating": rating, "text": text, "date": ev_date,
                    "iEvent": ievent_id,
                    "EventTitle": title,
                    "EventLocation": location,
                    "EventDate": ev_date,
                }
    return next_id


def apply_recurring_specials(bottles: list[dict], catalog: "Catalog",
                             persona: dict, rng):
    specials = persona.get("recurring_specials") or []
    if not specials:
        return
    history_years = persona["history_years"]
    start = add_months(date(TODAY.year, TODAY.month, 1), -history_years * 12)
    delivery_ord = np.array(
        [b["DeliveryDate"].toordinal() for b in bottles], dtype=np.int64
    )
    for sp in specials:
        m, dd = sp["date"]
        for y in range(start.year, TODAY.year + 1):
            try:
                ev_date = date(y, m, dd)
            except ValueError:
                continue
            if ev_date < start or ev_date > TODAY:
                continue
            consumed = np.array(
                [b["consumed_date"] is not None for b in bottles], dtype=bool
            )
            elig = (~consumed) & (delivery_ord <= ev_date.toordinal())
            elig = _filter_by_criteria(elig, bottles, catalog, sp.get("criteria") or {})
            if not elig.any():
                continue
            elig_idx = np.where(elig)[0]
            prices = np.array([bottles[i]["Price"] for i in elig_idx])
            w = np.clip(prices / 80.0, 0.5, 8.0)
            w = w / w.sum()
            i = int(elig_idx[int(rng.choice(len(elig_idx), p=w))])
            b = bottles[i]
            b["consumed_date"] = ev_date
            b["consumed_kind"] = "Drank from my cellar"
            b["consumption_note"] = f"{sp['title']} ({y})"
            rating, text = make_tasting_note(catalog.meta[b["ci"]], rng)
            b["tasting_note"] = {
                "rating": rating, "text": text, "date": ev_date,
                "EventTitle": sp["title"],
                "EventLocation": sp.get("location", "home"),
                "EventDate": ev_date,
            }


FLAW_TEXTS = [
    ("corked", "Clearly TCA — wet cardboard, dead palate."),
    ("cooked", "Heat-damaged. Stewed and flat."),
    ("oxidized", "Premature oxidation. Sherry character throughout."),
    ("reductive", "Reductive and never came around."),
]


def simulate_consumption(bottles: list[dict], catalog: Catalog,
                         persona: dict, rng):
    history_years = persona["history_years"]
    start = add_months(date(TODAY.year, TODAY.month, 1), -history_years * 12)
    months = history_years * 12

    drink_rate = persona["drink_rate"]
    sweep_rate = persona["inventory_sweep_rate"]
    note_rate = persona["note_rate"]
    meal_rate = persona["meal_note_rate"]
    adherence = persona["window_adherence"]

    n = len(bottles)
    delivery_ord = np.array(
        [b["DeliveryDate"].toordinal() for b in bottles], dtype=np.int64
    )
    prices = np.array([b["Price"] for b in bottles], dtype=np.float64)
    ci_arr = np.array([b["ci"] for b in bottles], dtype=np.int64)
    color = catalog.color[ci_arr]
    type_arr = catalog.type_[ci_arr]
    win_b = catalog.window_b[ci_arr]
    win_e = catalog.window_e[ci_arr]

    consumed = np.zeros(n, dtype=bool)

    for i in range(months):
        cur = add_months(start, i)
        n_events = int(rng.poisson(drink_rate))
        for _ in range(n_events):
            day = int(rng.integers(1, 29))
            try:
                event_dt = cur.replace(day=day)
            except ValueError:
                event_dt = cur
            event_ord = event_dt.toordinal()
            elig = (~consumed) & (delivery_ord <= event_ord)
            if not elig.any():
                continue
            elig_idx = np.where(elig)[0]
            kind, kids, people = pick_context(persona, event_dt, rng)
            w = compute_drink_weights(
                prices[elig_idx], color[elig_idx], type_arr[elig_idx],
                win_b[elig_idx], win_e[elig_idx],
                event_dt, kind, kids, adherence,
            )
            w = normalize(w)
            choice = int(rng.choice(len(elig_idx), p=w))
            chosen = int(elig_idx[choice])
            consumed[chosen] = True
            b = bottles[chosen]

            r = rng.random()
            cum = 0.0
            cum += sweep_rate
            if r < cum:
                b["consumed_date"] = event_dt
                b["consumed_kind"] = "Missing"
                b["consumption_note"] = "Inventory audit — bottle not found."
                continue
            cum += 0.025
            if r < cum:
                _, ftxt = FLAW_TEXTS[int(rng.integers(0, len(FLAW_TEXTS)))]
                b["consumed_date"] = event_dt
                b["consumed_kind"] = "Flawed"
                b["consumption_note"] = ftxt
                continue
            cum += 0.015
            if r < cum:
                friend = (rng.choice(persona["friends"])
                          if persona.get("friends") else "a friend")
                b["consumed_date"] = event_dt
                b["consumed_kind"] = "Gave away"
                b["consumption_note"] = f"Gift to {friend}"
                continue

            b["consumed_date"] = event_dt
            b["consumed_kind"] = "Drank from my cellar"
            if rng.random() < meal_rate:
                b["consumption_note"] = make_consumption_note(
                    kind, event_dt, people, rng
                )
            if rng.random() < note_rate:
                meta = catalog.meta[b["ci"]]
                rating, text = make_tasting_note(meta, rng)
                b["tasting_note"] = {
                    "rating": rating, "text": text, "date": event_dt
                }


def apply_sell_events(bottles: list[dict], catalog: Catalog,
                      persona: dict, rng):
    history_start = add_months(date(TODAY.year, TODAY.month, 1),
                               -persona["history_years"] * 12)
    for ev in persona.get("sell_events", []):
        ev_date = ev["date"]
        regions = set(ev.get("regions") or [])
        bought_before = ev.get("bought_before")
        wanted = ev["n"]
        elig: list[int] = []
        for i, b in enumerate(bottles):
            if b["consumed_date"] is not None:
                continue
            if b["DeliveryDate"] > ev_date:
                continue
            if regions:
                meta = catalog.meta[b["ci"]]
                rk = meta["RegionKey"]
                rg = meta["Region"]
                if rk not in regions and rg not in regions:
                    continue
            if bought_before and b["PurchaseDate"] >= bought_before:
                continue
            elig.append(i)
        if not elig:
            continue
        n = min(wanted, len(elig))
        if ev.get("prefer_early"):
            # Bias toward bottles bought soon after history started
            # ("regret purchases"). Exponential decay with ~2yr half-life.
            days = np.array([
                max(0, (bottles[i]["PurchaseDate"] - history_start).days)
                for i in elig
            ], dtype=np.float64)
            weights = np.exp(-days / 730.0)
            weights /= weights.sum()
            sel = rng.choice(len(elig), size=n, replace=False, p=weights)
        else:
            sel = rng.choice(len(elig), size=n, replace=False)
        for s in np.atleast_1d(sel):
            i = elig[int(s)]
            b = bottles[i]
            offset = int(rng.integers(0, 6))
            b["consumed_date"] = ev_date + timedelta(days=offset)
            b["consumed_kind"] = "Sold"
            b["consumption_note"] = ev["note"]
            b["tasting_note"] = None
            b["sell_store"] = ev.get("store", "Auction")


# ============================================================
# Trim + write
# ============================================================

def trim_inventory(bottles: list[dict], target: int, rng):
    inv_all = [i for i, b in enumerate(bottles) if b["consumed_date"] is None]
    cap = int(target * 1.15)
    if len(inv_all) <= cap:
        return
    excess = len(inv_all) - target
    # Drop oldest-purchased that are already delivered (don't drop pending —
    # they haven't arrived yet, "drinking" them is nonsensical).
    delivered = [i for i in inv_all if bottles[i]["DeliveryDate"] <= TODAY]
    delivered.sort(key=lambda i: bottles[i]["PurchaseDate"])
    drop = delivered[:excess]
    for i in drop:
        b = bottles[i]
        span = (TODAY - b["DeliveryDate"]).days
        offset = int(rng.integers(0, span + 1)) if span > 0 else 0
        b["consumed_date"] = b["DeliveryDate"] + timedelta(days=offset)
        b["consumed_kind"] = "Drank from my cellar"
        b["consumption_note"] = ""
        b["tasting_note"] = None


def write_csv(path: Path, cols: list[str], rows: list[dict]):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, quoting=csv.QUOTE_ALL)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})


def write_outputs(persona_name: str, bottles: list[dict],
                  purchases: list[dict], catalog: Catalog) -> dict:
    out = OUT_DIR / persona_name
    out.mkdir(parents=True, exist_ok=True)

    # purchases
    pur_rows = []
    rem_count: dict[int, int] = defaultdict(int)
    for b in bottles:
        if b["consumed_date"] is None:
            rem_count[b["iPurchase"]] += 1

    for p in purchases:
        m = catalog.meta[p["ci"]]
        delivered = "True" if p["DeliveryDate"] <= TODAY else "False"
        pur_rows.append({
            "iWine": p["iWine"],
            "iPurchase": p["iPurchase"],
            "PurchaseDate": fmt_date(p["PurchaseDate"]),
            "DeliveryDate": fmt_date(p["DeliveryDate"]),
            "StoreName": p["StoreName"],
            "Currency": "USD",
            "ExchangeRate": "1",
            "Price": f"{p['Price']:.2f}",
            "NativePrice": f"{p['Price']:.2f}",
            "NativePriceCurrency": "USD",
            "Quantity": p["Quantity"],
            "Remaining": rem_count.get(p["iPurchase"], 0),
            "OrderNumber": p.get("OrderNumber") or f"OrderId:{p['iPurchase']}",
            "Delivered": delivered,
            "Size": p["Size"],
            "Vintage": m["Vintage"],
            "Wine": m["Wine"],
            "Locale": m["Locale"],
            "Type": m["Type"],
            "Color": m["Color"],
            "Category": m["Category"],
            "Producer": m["Producer"],
            "Varietal": m["Varietal"],
            "MasterVarietal": m["MasterVarietal"],
            "Designation": m["Designation"],
            "Vineyard": m["Vineyard"],
            "Country": m["Country"],
            "Region": m["Region"],
            "SubRegion": m["SubRegion"],
            "Appellation": m["Appellation"],
        })

    # inventory
    inv_rows = []
    for b in bottles:
        if b["consumed_date"] is not None:
            continue
        m = catalog.meta[b["ci"]]
        pending = "1" if b["DeliveryDate"] > TODAY else "0"
        loc = "(pending)" if b["DeliveryDate"] > TODAY else b["Location"]
        bin_ = "" if b["DeliveryDate"] > TODAY else b["Bin"]
        wbar = f"W{b['iWine']}_{b['Size']}"
        bar = f"0{int(b['iInventory']):09d}"
        inv_rows.append({
            "iInventory": b["iInventory"],
            "Pending": pending,
            "Barcode": bar,
            "WineBarcode": wbar,
            "Price": f"{b['Price']:.2f}",
            "Size": b["Size"],
            "iWine": b["iWine"],
            "Type": m["Type"],
            "Vintage": m["Vintage"],
            "Wine": m["Wine"],
            "Producer": m["Producer"],
            "Varietal": m["Varietal"],
            "Designation": m["Designation"],
            "Vineyard": m["Vineyard"],
            "Country": m["Country"],
            "Region": m["Region"],
            "SubRegion": m["SubRegion"],
            "Appellation": m["Appellation"],
            "Note": b["BottleNote"],
            "StoreName": b["StoreName"],
            "PurchaseDate": fmt_date(b["PurchaseDate"]),
            "PurchaseNote": b["PurchaseNote"],
            "Location": loc,
            "Bin": bin_,
        })

    # consumed + notes
    con_rows = []
    note_rows = []
    next_iconsumed = 50_000_000
    next_inote = 10_000_000
    for b in bottles:
        if b["consumed_date"] is None:
            continue
        m = catalog.meta[b["ci"]]
        cd = b["consumed_date"]
        if b["consumed_kind"] == "Sold":
            revenue = f"{b['Price'] * 0.85:.2f}"
        else:
            revenue = "0"
        con_rows.append({
            "iConsumed": next_iconsumed,
            "iWine": b["iWine"],
            "Type": m["Type"],
            "Consumed": fmt_date(cd),
            "ConsumedYear": cd.year,
            "ConsumedMonth": cd.month,
            "ConsumedDay": cd.day,
            "Size": b["Size"],
            "ShortType": b["consumed_kind"],
            "Currency": "USD",
            "Value": f"{b['Price']:.2f}",
            "Price": f"{b['Price']:.2f}",
            "Revenue": revenue,
            "RevenueCurrency": "USD",
            "ConsumptionNote": b["consumption_note"],
            "PurchaseNote": b["PurchaseNote"],
            "BottleNote": b["BottleNote"],
            "Location": b["Location"],
            "Bin": b["Bin"],
            "Vintage": m["Vintage"],
            "Wine": m["Wine"],
            "Locale": m["Locale"],
            "Color": m["Color"],
            "Category": m["Category"],
            "Varietal": m["Varietal"],
            "MasterVarietal": m["MasterVarietal"],
            "Designation": m["Designation"],
            "Vineyard": m["Vineyard"],
            "Country": m["Country"],
            "Region": m["Region"],
            "SubRegion": m["SubRegion"],
            "Appellation": m["Appellation"],
        })
        next_iconsumed += 1

        t = b.get("tasting_note")
        if t:
            note_rows.append({
                "iNote": next_inote,
                "iWine": b["iWine"],
                "Type": m["Type"],
                "Vintage": m["Vintage"],
                "Wine": m["Wine"],
                "Locale": m["Locale"],
                "Producer": m["Producer"],
                "Varietal": m["Varietal"],
                "MasterVarietal": m["MasterVarietal"],
                "Designation": m["Designation"],
                "Vineyard": m["Vineyard"],
                "Country": m["Country"],
                "Region": m["Region"],
                "SubRegion": m["SubRegion"],
                "Appellation": m["Appellation"],
                "TastingDate": fmt_date(t["date"]),
                "Defective": "False",
                "Views": "0",
                "fHelpful": "",
                "fFavorite": "0",
                "Rating": f"{t['rating']:.0f}",
                "EventLocation": t.get("EventLocation", ""),
                "EventTitle": t.get("EventTitle", ""),
                "iEvent": t.get("iEvent", ""),
                "EventDate": fmt_date(t["EventDate"]) if t.get("EventDate") else "",
                "EventEndDate": fmt_date(t["EventDate"]) if t.get("EventDate") else "",
                "TastingNotes": t["text"],
                "fLikeIt": "",
                "CNotes": "",
                "CScore": "",
                "LikeVotes": "",
                "LikePercent": "",
                "Votes": "",
                "Comments": "",
            })
            next_inote += 1

    write_csv(out / "inventory.csv", INVENTORY_COLS, inv_rows)
    write_csv(out / "purchase.csv", PURCHASE_COLS, pur_rows)
    write_csv(out / "consumed.csv", CONSUMED_COLS, con_rows)
    write_csv(out / "notes.csv", NOTE_COLS, note_rows)

    return {
        "inventory": len(inv_rows),
        "purchase": len(pur_rows),
        "consumed": len(con_rows),
        "notes": len(note_rows),
    }


# ============================================================
# Driver
# ============================================================

def generate_for_persona(name: str, persona: dict, catalog: Catalog) -> dict:
    seed = SEED + sum(ord(c) for c in name)
    rng = np.random.default_rng(seed)
    favorites = build_favorites(catalog, persona, rng)
    purchases = simulate_purchases(catalog, persona, favorites, rng)
    batch_shipments(purchases, rng)
    bottles = expand_to_bottles(purchases, persona, rng)
    simulate_events(bottles, catalog, persona, rng)
    apply_recurring_specials(bottles, catalog, persona, rng)
    simulate_consumption(bottles, catalog, persona, rng)
    apply_sell_events(bottles, catalog, persona, rng)
    trim_inventory(bottles, persona["target_inventory"], rng)
    counts = write_outputs(name, bottles, purchases, catalog)
    counts.update({
        "favorites": len(favorites),
        "lots": len(purchases),
        "bottles_made": len(bottles),
    })
    return counts


def main():
    print(f"Loading catalog from {DATA_DIR}/...")
    catalog = load_catalog()
    print(f"  {len(catalog)} unique iWines")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, persona in PERSONAS.items():
        print(f"\n{name}:")
        c = generate_for_persona(name, persona, catalog)
        for k, v in c.items():
            print(f"  {k}: {v}")
    print(f"\nWrote outputs to {OUT_DIR}/")


if __name__ == "__main__":
    main()
