"""Real company data from Wikidata (CC0) for the real-data benchmark.

Two things come from Wikidata's public SPARQL endpoint:

* **Companies**: items that are directly an instance of a company class and carry at least one
  dated employee count (P1128) or revenue (P2139) statement. Attributes: English label, official
  website (P856), country (P17), industry (P452), headquarters (P159), inception (P571) and
  dissolution (P576).
* **Dated facts**: every non-deprecated P1128 / P2139 statement with a point in time (P585).
  Revenue is kept only when it is stated in US dollars.

Those dated statements are a real change history: replaying them year by year tells us which
companies existed at a date (inception / dissolution) and what headcount and revenue they had
published by then. :func:`snapshot` builds the warehouse table "as of" a date and :func:`diff`
gives the change between two such dates.

Only companies are loaded; no data about people.
"""

from __future__ import annotations

import csv
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, fields
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "crm-sync-bench/0.1 (+https://github.com/goalgetter53/crm-sync-engine)"
LICENSE = "CC0 1.0 (Wikidata)"
ENTITY = "http://www.wikidata.org/entity/"
USD = ENTITY + "Q4917"
PREFERRED = "http://wikiba.se/ontology#PreferredRank"
DEPRECATED = "http://wikiba.se/ontology#DeprecatedRank"

# business, enterprise, public company, company, corporation, privately held company, holding
COMPANY_CLASSES = ("Q4830453", "Q6881511", "Q891723", "Q783794", "Q167037", "Q1589009", "Q219577")
FACT_PROPS = {"employees": "P1128", "revenue_usd": "P2139"}

ROW_COLUMNS = (
    "qid",
    "name",
    "domain",
    "website",
    "country",
    "hq_city",
    "industry",
    "founded_year",
    "employees",
    "revenue_usd",
)


@dataclass(frozen=True)
class Company:
    qid: str
    name: str | None
    website: str | None
    country: str | None
    industry: str | None
    hq_city: str | None
    inception: date | None
    dissolved: date | None


@dataclass(frozen=True)
class Fact:
    qid: str
    prop: str  # "employees" | "revenue_usd"
    on: date
    value: int
    preferred: bool


# -- SPARQL -------------------------------------------------------------------------------------
def facts_query(prop: str) -> str:
    pid = FACT_PROPS[prop]
    classes = " ".join(f"wd:{c}" for c in COMPANY_CLASSES)
    return f"""SELECT DISTINCT ?item ?s ?t ?amount ?unit ?rank WHERE {{
  VALUES ?cls {{ {classes} }}
  ?item wdt:P31 ?cls .
  ?item p:{pid} ?s .
  ?s pq:P585 ?t ; psv:{pid} ?v ; wikibase:rank ?rank .
  ?v wikibase:quantityAmount ?amount ; wikibase:quantityUnit ?unit .
}}"""


def attributes_query(qids: Sequence[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return f"""SELECT ?item (MIN(?l) AS ?name) (MIN(STR(?w)) AS ?website) (MIN(?cl) AS ?country)
  (MIN(?il) AS ?industry) (MIN(?hl) AS ?hq) (MIN(?inc) AS ?inception) (MIN(?dis) AS ?dissolved)
WHERE {{
  VALUES ?item {{ {values} }}
  OPTIONAL {{ ?item rdfs:label ?l FILTER(LANG(?l) = "en") }}
  OPTIONAL {{ ?item wdt:P856 ?w }}
  OPTIONAL {{ ?item wdt:P17 ?c . ?c rdfs:label ?cl FILTER(LANG(?cl) = "en") }}
  OPTIONAL {{ ?item wdt:P452 ?i . ?i rdfs:label ?il FILTER(LANG(?il) = "en") }}
  OPTIONAL {{ ?item wdt:P159 ?h . ?h rdfs:label ?hl FILTER(LANG(?hl) = "en") }}
  OPTIONAL {{ ?item wdt:P571 ?inc }}
  OPTIONAL {{ ?item wdt:P576 ?dis }}
}} GROUP BY ?item"""


def run_sparql(query: str, *, attempts: int = 5) -> list[dict[str, str]]:
    """POST a query; return bindings flattened to ``{var: value}``. Backs off on 429/5xx.

    Uses urllib: the endpoint's bot filter rejected httpx's default request shape (403).
    """
    req = urllib.request.Request(
        ENDPOINT,
        data=urllib.parse.urlencode({"query": query}).encode(),
        headers={"Accept": "application/sparql-results+json", "User-Agent": USER_AGENT},
    )
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                return flatten(json.loads(resp.read()))
        except urllib.error.HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or attempt == attempts - 1:
                raise
            ra = exc.headers.get("retry-after")
            time.sleep(float(ra) if ra and ra.isdigit() else 5.0 * (attempt + 1))
    raise RuntimeError("unreachable")


def flatten(payload: Mapping[str, Any]) -> list[dict[str, str]]:
    return [
        {k: str(v["value"]) for k, v in b.items()}
        for b in payload.get("results", {}).get("bindings", [])
    ]


def fetch(cache_dir: Path, *, refresh: bool = False, batch: int = 1500) -> dict[str, Any]:
    """Download (or reuse) the raw SPARQL results under ``cache_dir``. Network access."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    out: dict[str, Any] = {}
    for prop in FACT_PROPS:
        path = cache_dir / f"facts_{prop}.json"
        if refresh or not path.exists():
            path.write_text(json.dumps(run_sparql(facts_query(prop))))
            time.sleep(1.0)
        out[f"facts_{prop}"] = json.loads(path.read_text())
    qids = sorted({_qid(r["item"]) for prop in FACT_PROPS for r in out[f"facts_{prop}"]}, key=_qnum)
    path = cache_dir / "attributes.json"
    if refresh or not path.exists():
        rows: list[dict[str, str]] = []
        for i in range(0, len(qids), batch):
            rows.extend(run_sparql(attributes_query(qids[i : i + batch])))
            time.sleep(1.0)
        path.write_text(json.dumps(rows))
    out["attributes"] = json.loads(path.read_text())
    meta = cache_dir / "meta.json"
    if refresh or not meta.exists():
        meta.write_text(json.dumps({"retrieved": date.today().isoformat(), "endpoint": ENDPOINT}))
    out["meta"] = json.loads(meta.read_text())
    return out


# -- parsing ------------------------------------------------------------------------------------
def _qid(uri: str) -> str:
    return uri.rsplit("/", 1)[-1]


def _qnum(qid: str) -> int:
    return int(qid[1:]) if qid[1:].isdigit() else 0


def _date(text: str | None) -> date | None:
    if not text:
        return None
    t = text.lstrip("+")
    try:
        y, m, d = int(t[0:4]), int(t[5:7]), int(t[8:10])
        return date(y, max(m, 1), max(d, 1))
    except ValueError:
        return None


def parse_facts(prop: str, rows: Iterable[Mapping[str, str]]) -> list[Fact]:
    out: dict[str, Fact] = {}
    for r in rows:
        if r.get("rank") == DEPRECATED:
            continue
        if prop == "revenue_usd" and r.get("unit") != USD:
            continue
        on = _date(r.get("t"))
        try:
            value = int(Decimal(r["amount"]).to_integral_value())
        except (InvalidOperation, KeyError):
            continue
        if on is None or value < 0:
            continue
        # one row per statement (an item matching two company classes appears twice)
        out[r.get("s", "") or f"{r['item']}|{on}|{value}"] = Fact(
            _qid(r["item"]), prop, on, value, r.get("rank") == PREFERRED
        )
    return sorted(out.values(), key=lambda f: (_qnum(f.qid), f.prop, f.on, f.value))


def _clean(v: str | None) -> str | None:
    if v is None:
        return None
    v = " ".join(v.split())
    return v or None


def parse_companies(rows: Iterable[Mapping[str, str]]) -> dict[str, Company]:
    out: dict[str, Company] = {}
    for r in rows:
        q = _qid(r["item"])
        out[q] = Company(
            qid=q,
            name=_clean(r.get("name")),
            website=_clean(r.get("website")),
            country=_clean(r.get("country")),
            industry=_clean(r.get("industry")),
            hq_city=_clean(r.get("hq")),
            inception=_date(r.get("inception")),
            dissolved=_date(r.get("dissolved")),
        )
    return dict(sorted(out.items(), key=lambda kv: _qnum(kv[0])))


def domain_from_url(url: str | None) -> str | None:
    if not url:
        return None
    u = url.strip()
    if "://" not in u:
        u = "http://" + u
    try:
        host = urlsplit(u).hostname
    except ValueError:
        return None
    if not host or "." not in host:
        return None
    host = host.lower().rstrip(".")
    return host[4:] if host.startswith("www.") else host


# -- derived CSVs (what the repo commits) ------------------------------------------------------
def write_csv(companies: Mapping[str, Company], facts: Sequence[Fact], directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "wikidata_companies.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow([f.name for f in fields(Company)])
        for c in companies.values():
            w.writerow(["" if v is None else v for v in asdict(c).values()])
    with (directory / "wikidata_company_facts.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["qid", "prop", "on", "value", "preferred"])
        for f in facts:
            w.writerow([f.qid, f.prop, f.on.isoformat(), f.value, int(f.preferred)])


def read_csv(directory: Path) -> tuple[dict[str, Company], list[Fact]]:
    companies: dict[str, Company] = {}
    with (directory / "wikidata_companies.csv").open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            companies[r["qid"]] = Company(
                qid=r["qid"],
                name=r["name"] or None,
                website=r["website"] or None,
                country=r["country"] or None,
                industry=r["industry"] or None,
                hq_city=r["hq_city"] or None,
                inception=_date(r["inception"]),
                dissolved=_date(r["dissolved"]),
            )
    facts: list[Fact] = []
    with (directory / "wikidata_company_facts.csv").open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            on = _date(r["on"])
            if on is not None:
                facts.append(Fact(r["qid"], r["prop"], on, int(r["value"]), r["preferred"] == "1"))
    return companies, facts


# -- replay -------------------------------------------------------------------------------------
Row = dict[str, Any]


def snapshot(companies: Mapping[str, Company], facts: Sequence[Fact], at: date) -> dict[str, Row]:
    """The warehouse ``companies`` table as it would have looked on ``at``.

    A company exists from its inception (if known) until its dissolution. Employees and revenue
    are the latest figure with a point in time on or before ``at`` (a preferred-rank statement
    wins a tie on the same date).
    """
    latest: dict[tuple[str, str], tuple[date, bool, int]] = {}
    for f in facts:
        if f.on > at:
            continue
        k = (f.qid, f.prop)
        cand = (f.on, f.preferred, f.value)
        if k not in latest or cand > latest[k]:
            latest[k] = cand
    out: dict[str, Row] = {}
    for q, c in companies.items():
        if c.inception is not None and c.inception > at:
            continue
        if c.dissolved is not None and c.dissolved <= at:
            continue
        emp = latest.get((q, "employees"))
        rev = latest.get((q, "revenue_usd"))
        out[q] = {
            "qid": q,
            "name": c.name,
            "domain": domain_from_url(c.website),
            "website": c.website,
            "country": c.country,
            "hq_city": c.hq_city,
            "industry": c.industry,
            "founded_year": str(c.inception.year) if c.inception else None,
            "employees": emp[2] if emp else None,
            "revenue_usd": rev[2] if rev else None,
        }
    return out


@dataclass
class Delta:
    inserts: list[Row]
    updates: list[Row]
    deletes: list[str]
    changed_fields: dict[str, int]

    def counts(self) -> dict[str, Any]:
        return {
            "inserts": len(self.inserts),
            "updates": len(self.updates),
            "deletes": len(self.deletes),
            "changed_fields": dict(sorted(self.changed_fields.items())),
        }


def diff(prev: Mapping[str, Row], cur: Mapping[str, Row]) -> Delta:
    inserts = [cur[q] for q in cur if q not in prev]
    deletes = [q for q in prev if q not in cur]
    updates: list[Row] = []
    changed: dict[str, int] = {}
    for q, row in cur.items():
        old = prev.get(q)
        if old is None:
            continue
        cols = [c for c in ROW_COLUMNS if old.get(c) != row.get(c)]
        if cols:
            updates.append(row)
            for c in cols:
                changed[c] = changed.get(c, 0) + 1
    return Delta(inserts, updates, deletes, changed)
