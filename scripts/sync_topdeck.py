import json
import os
import re
import time
import unicodedata
from difflib import get_close_matches
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import unquote, urlparse, urlunparse

import pandas as pd
import requests

API_BASE = "https://topdeck.gg/api"
API_KEY = os.getenv("TOPDECK_API_KEY")  # NAO colocar key no arquivo
LEAGUE_ORDER = ["presencial", "online", "presencial2x2", "online2x2"]
VALID_LEAGUES = set(LEAGUE_ORDER)
TWO_BY_TWO_LEAGUES = {"presencial2x2", "online2x2"}
LEAGUE_ALIASES = {
    "presencial-2x2": "presencial2x2",
    "online-2x2": "online2x2",
    "2x2-presencial": "presencial2x2",
    "2x2-online": "online2x2",
}
DEFAULT_LEAGUE = "presencial"
TEAM_MAP_2X2_PATH = "Team_map_2x2.csv"
DECK_ALIASES_PATH = "deck_aliases.json"
TOPDECK_MAX_RETRIES = 5
TOPDECK_BASE_BACKOFF_SECONDS = 1.5
TOPDECK_MAX_BACKOFF_SECONDS = 45.0
TOPDECK_MIN_INTERVAL_SECONDS = 0.5
_LAST_TOPDECK_REQUEST_MONO = 0.0
_DECK_ALIASES_CACHE = None


def discard_count_for_league(league_type: str) -> int:
    return 2 if league_type in {"online", "online2x2"} else 3


def parse_retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        seconds = float(text)
        return max(0.0, seconds)
    except Exception:
        pass

    try:
        dt = parsedate_to_datetime(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = (dt - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, float(delta))
    except Exception:
        return None


def topdeck_request(method: str, path: str, json_payload: dict | None = None, timeout: int = 30) -> requests.Response:
    global _LAST_TOPDECK_REQUEST_MONO

    url = f"{API_BASE}{path}" if path.startswith("/") else path
    headers = {"Authorization": API_KEY}
    retry_statuses = {429, 500, 502, 503, 504}
    last_exc = None

    for attempt in range(1, TOPDECK_MAX_RETRIES + 1):
        now = time.monotonic()
        wait_gap = TOPDECK_MIN_INTERVAL_SECONDS - (now - _LAST_TOPDECK_REQUEST_MONO)
        if wait_gap > 0:
            time.sleep(wait_gap)

        _LAST_TOPDECK_REQUEST_MONO = time.monotonic()

        try:
            resp = requests.request(
                method=method.upper(),
                url=url,
                headers=headers,
                json=json_payload,
                timeout=timeout,
            )
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt >= TOPDECK_MAX_RETRIES:
                raise

            delay = min(TOPDECK_MAX_BACKOFF_SECONDS, TOPDECK_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
            print(
                f"Aviso: erro de rede na TopDeck ({type(exc).__name__}). "
                f"Retry {attempt}/{TOPDECK_MAX_RETRIES} em {delay:.1f}s."
            )
            time.sleep(delay)
            continue

        if resp.status_code in retry_statuses and attempt < TOPDECK_MAX_RETRIES:
            retry_after = parse_retry_after_seconds(resp.headers.get("Retry-After"))
            delay = retry_after if retry_after is not None else min(
                TOPDECK_MAX_BACKOFF_SECONDS,
                TOPDECK_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)),
            )
            print(
                f"Aviso: TopDeck retornou HTTP {resp.status_code} para {path}. "
                f"Retry {attempt}/{TOPDECK_MAX_RETRIES} em {delay:.1f}s."
            )
            time.sleep(delay)
            continue

        resp.raise_for_status()
        return resp

    if last_exc:
        raise last_exc
    raise RuntimeError(f"Falha ao consultar TopDeck: {path}")


def unix_to_date(ts: int | None) -> str | None:
    if ts is None:
        return None
    ts_int = int(ts)
    if ts_int > 10**12:  # se vier em ms
        ts_int //= 1000
    dt = datetime.fromtimestamp(ts_int, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def month_key(ts: int | None) -> str | None:
    if ts is None:
        return None
    ts_int = int(ts)
    if ts_int > 10**12:
        ts_int //= 1000
    dt = datetime.fromtimestamp(ts_int, tz=timezone.utc)
    return dt.strftime("%Y-%m")


def quarter_key_from_month(month: str | None) -> str | None:
    if not month:
        return None
    try:
        year_str, month_str = month.split("-", 1)
        year = int(year_str)
        m = int(month_str)
        if m < 1 or m > 12:
            return None
    except Exception:
        return None
    q = (m - 1) // 3 + 1
    return f"{year}-Q{q}"


def points_for_event_position(rank: int, player_count: int) -> int:
    """Calcula pontuacao por colocacao do evento (regras 2026)."""
    if rank == 1:
        return 3
    if player_count <= 12:
        return 2 if rank <= 4 else 1
    if player_count <= 24:
        return 2 if rank <= 8 else 1
    if player_count <= 36:
        return 2 if rank <= 12 else 1
    return 2 if rank <= 16 else 1


def group_apply_preserve_keys(
    df: pd.DataFrame,
    by: str | list[str],
    func,
    *,
    sort: bool = False,
    dropna: bool = True,
) -> pd.DataFrame:
    """
    Compatibilidade com pandas 3:
    DataFrameGroupBy.apply pode remover as colunas de agrupamento do resultado.
    Este helper reaplica as chaves ao concatenar os grupos processados.
    """
    group_cols = [by] if isinstance(by, str) else list(by)
    parts = []

    for keys, group in df.groupby(group_cols, sort=sort, dropna=dropna):
        if not isinstance(keys, tuple):
            keys = (keys,)

        result = func(group).copy()
        for col, key in zip(group_cols, keys):
            if col not in result.columns:
                result[col] = key
        parts.append(result)

    if not parts:
        return df.iloc[0:0].copy()
    return pd.concat(parts, ignore_index=True)


def fix_moxfield(url: str | None) -> str | None:
    if not url:
        return None
    if "moxfield.com/decks/" in url:
        if url.startswith("https://moxfield.com/decks/"):
            return url
        m = re.search(r"q=(https://moxfield\.com/decks/[^&]+)", url)
        if m:
            return m.group(1)
    return url


def clean_player_name(name: object) -> str | None:
    if name is None:
        return None
    try:
        if pd.isna(name):
            return None
    except Exception:
        pass

    text = re.sub(r"\s+", " ", str(name)).strip()
    if text and not any(ch.isalnum() for ch in text):
        return None
    return text or None


def normalize_player_name(name: object) -> str | None:
    text = clean_player_name(name)
    if not text:
        return None
    return text.lower()


def normalize_lookup_key(value: object) -> str | None:
    text = clean_player_name(value)
    if not text:
        return None

    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def is_placeholder_deck(value: object) -> bool:
    key = normalize_lookup_key(value)
    return key in {
        "sem deck",
        "sem decks",
        "s deck",
        "no deck",
        "no decks",
        "deck missing",
        "missing deck",
    }


def load_player_aliases(path: str = "player_aliases.json") -> dict[str, str]:
    if not os.path.exists(path):
        return {}

    with open(path, "r", encoding="utf-8-sig") as f:
        raw_aliases = json.load(f)

    if not isinstance(raw_aliases, dict):
        raise SystemExit(f"{path}: esperado um objeto JSON com aliases de jogadores.")

    aliases = {}
    for raw_name, alias_name in raw_aliases.items():
        key = clean_player_name(raw_name)
        value = clean_player_name(alias_name)
        if key and value:
            aliases[key] = value
    return aliases


def load_deck_aliases(path: str = DECK_ALIASES_PATH) -> dict[str, str]:
    global _DECK_ALIASES_CACHE
    if _DECK_ALIASES_CACHE is not None:
        return _DECK_ALIASES_CACHE
    if not os.path.exists(path):
        _DECK_ALIASES_CACHE = {}
        return _DECK_ALIASES_CACHE

    with open(path, "r", encoding="utf-8-sig") as f:
        raw_aliases = json.load(f)

    if not isinstance(raw_aliases, dict):
        raise SystemExit(f"{path}: esperado um objeto JSON com aliases de decks.")

    aliases = {}
    for raw_name, canonical_name in raw_aliases.items():
        key = normalize_lookup_key(raw_name)
        value = clean_player_name(canonical_name)
        if key and value:
            aliases[key] = value
    _DECK_ALIASES_CACHE = aliases
    return aliases


def resolve_player_alias(name: object, aliases: dict[str, str]) -> str | None:
    clean_name = clean_player_name(name)
    if not clean_name:
        return None
    exact = aliases.get(clean_name)
    if exact:
        return exact

    norm_name = normalize_player_name(clean_name)
    for alias_name, canonical_name in aliases.items():
        if normalize_player_name(alias_name) == norm_name:
            return canonical_name

    return clean_name


def is_two_by_two_league(league: str | None) -> bool:
    return normalize_league(league) in TWO_BY_TWO_LEAGUES


def normalize_team_key(value: object) -> str | None:
    return normalize_lookup_key(value)


def two_by_two_player_pair_key(player_1: object, player_2: object) -> str | None:
    parts = sorted(
        [key for key in [normalize_lookup_key(player_1), normalize_lookup_key(player_2)] if key]
    )
    if len(parts) != 2:
        return None
    return " & ".join(parts)


def canonical_two_by_two_team_name(player_1: object, player_2: object) -> str | None:
    players = [clean_player_name(player_1), clean_player_name(player_2)]
    players = [player for player in players if player]
    if len(players) != 2:
        return None
    return " & ".join(sorted(players, key=lambda value: normalize_lookup_key(value) or ""))


def split_two_by_two_values(value: object) -> list[str]:
    text = clean_player_name(value)
    if not text:
        return []
    parts = re.split(r"\s*(?:&|\+|\be\b|\band\b|,|/)\s*", text, flags=re.IGNORECASE)
    return [p.strip() for p in parts if p and p.strip()]


def parse_two_by_two_raw_team(raw_team: object) -> dict:
    text = clean_player_name(raw_team)
    if not text:
        return {"team_name": None, "players": [], "decks": []}

    team_part = text
    deck_part = None
    if "//" in text:
        team_part, deck_part = re.split(r"\s*//\s*", text, maxsplit=1)

    players = split_two_by_two_values(team_part)[:2]
    decks = split_two_by_two_values(deck_part)[:2] if deck_part else []
    return {
        "team_name": clean_player_name(team_part),
        "players": players,
        "decks": decks,
    }


TEAM_MAP_2X2_COLUMNS = [
    "tid",
    "raw_team",
    "team_name",
    "player_1",
    "player_2",
    "deck_1",
    "deck_2",
    "deck_url_1",
    "deck_url_2",
    "notes",
]


def load_team_map_2x2(path: str = TEAM_MAP_2X2_PATH) -> dict[tuple[str, str], dict]:
    if not os.path.exists(path):
        return {}

    df = pd.read_csv(path, encoding="utf-8-sig")
    if df.empty:
        return {}

    for col in TEAM_MAP_2X2_COLUMNS:
        if col not in df.columns:
            df[col] = None

    mapping = {}
    for _, row in df.iterrows():
        tid = clean_player_name(row.get("tid"))
        raw_team = clean_player_name(row.get("raw_team"))
        raw_key = normalize_team_key(raw_team)
        if not tid or not raw_key:
            continue

        item = {}
        for col in TEAM_MAP_2X2_COLUMNS:
            item[col] = clean_player_name(row.get(col))
        keys = [raw_key]
        parsed = parse_two_by_two_raw_team(raw_team)
        team_key = normalize_team_key(item.get("team_name")) or normalize_team_key(parsed.get("team_name"))
        pair_key = two_by_two_player_pair_key(item.get("player_1"), item.get("player_2"))
        parsed_pair_key = None
        if len(parsed["players"]) >= 2:
            parsed_pair_key = two_by_two_player_pair_key(parsed["players"][0], parsed["players"][1])

        for key in [team_key, pair_key, parsed_pair_key]:
            if key:
                keys.append(key)

        for key in dict.fromkeys(keys):
            mapping[(tid, key)] = item
    return mapping


def resolve_manual_deck(deck_label: object, deck_url: object, deck_map: dict) -> dict:
    url = canonicalize_url(deck_url)
    if is_placeholder_deck(deck_label) and not url:
        return {
            "deck_key": None,
            "deck_name_pt": None,
            "deck_name_en": None,
            "colecao": None,
            "deck_url": None,
        }

    info = deck_map.get(url) if url and deck_map else None
    label = clean_player_name(deck_label)

    if not info and label and deck_map:
        info = find_deck_by_name(label, deck_map)
        if info:
            url = info.get("deck_url") or url

    if info:
        deck_name_pt = info.get("deck_name_pt") or label
        deck_name_en = info.get("deck_name_en")
        colecao = info.get("colecao")
    else:
        deck_name_pt = label
        deck_name_en = None
        colecao = None

    deck_key = clean_player_name(deck_name_pt) or url
    return {
        "deck_key": deck_key,
        "deck_name_pt": deck_name_pt,
        "deck_name_en": deck_name_en,
        "colecao": colecao,
        "deck_url": url,
    }


def find_deck_by_name(deck_label: object, deck_map: dict) -> dict | None:
    key = normalize_lookup_key(deck_label)
    if not key or not deck_map:
        return None

    by_name = {}
    for url, info in deck_map.items():
        for value in [info.get("deck_name_pt"), info.get("deck_name_en")]:
            value_key = normalize_lookup_key(value)
            if value_key:
                by_name[value_key] = {**info, "deck_url": url}

    exact = by_name.get(key)
    if exact:
        return exact

    alias_target = load_deck_aliases().get(key)
    alias_key = normalize_lookup_key(alias_target)
    if alias_key:
        alias_exact = by_name.get(alias_key)
        if alias_exact:
            return alias_exact

    matches = get_close_matches(key, by_name.keys(), n=1, cutoff=0.92)
    if matches:
        return by_name[matches[0]]
    return None


def resolve_two_by_two_team_info(
    tid: object,
    raw_team: object,
    team_map: dict[tuple[str, str], dict],
    player_aliases: dict[str, str],
    deck_map: dict,
) -> dict:
    tid_text = clean_player_name(tid)
    raw_team_text = clean_player_name(raw_team)
    parsed = parse_two_by_two_raw_team(raw_team_text)
    manual = None
    raw_key = normalize_team_key(raw_team_text)
    if tid_text and raw_key:
        candidate_keys = [raw_key, normalize_team_key(parsed.get("team_name"))]
        if len(parsed["players"]) >= 2:
            candidate_keys.append(two_by_two_player_pair_key(parsed["players"][0], parsed["players"][1]))
        for key in dict.fromkeys([key for key in candidate_keys if key]):
            manual = team_map.get((tid_text, key))
            if manual:
                break

    player_1 = clean_player_name(manual.get("player_1") if manual else None) or (
        parsed["players"][0] if len(parsed["players"]) >= 1 else None
    )
    player_2 = clean_player_name(manual.get("player_2") if manual else None) or (
        parsed["players"][1] if len(parsed["players"]) >= 2 else None
    )
    player_1 = resolve_player_alias(player_1, player_aliases)
    player_2 = resolve_player_alias(player_2, player_aliases)

    deck_1_label = clean_player_name(manual.get("deck_1") if manual else None) or (
        parsed["decks"][0] if len(parsed["decks"]) >= 1 else None
    )
    deck_2_label = clean_player_name(manual.get("deck_2") if manual else None) or (
        parsed["decks"][1] if len(parsed["decks"]) >= 2 else None
    )
    deck_1 = resolve_manual_deck(deck_1_label, manual.get("deck_url_1") if manual else None, deck_map)
    deck_2 = resolve_manual_deck(deck_2_label, manual.get("deck_url_2") if manual else None, deck_map)

    pair_key = two_by_two_player_pair_key(player_1, player_2)
    if pair_key:
        team_key = pair_key
    else:
        team_key = normalize_team_key(clean_player_name(manual.get("team_name") if manual else None) or parsed.get("team_name") or raw_team_text)

    canonical_team_name = canonical_two_by_two_team_name(player_1, player_2)
    team_name = clean_player_name(manual.get("team_name") if manual else None) or canonical_team_name or parsed.get("team_name") or raw_team_text

    map_status = "manual" if manual else ("parsed" if player_1 and player_2 else "missing")
    return {
        "tid": tid_text,
        "raw_team": raw_team_text,
        "team_name": team_name,
        "team_key": team_key,
        "player_1": player_1,
        "player_2": player_2,
        "deck_1": deck_1.get("deck_key"),
        "deck_2": deck_2.get("deck_key"),
        "deck_url_1": deck_1.get("deck_url"),
        "deck_url_2": deck_2.get("deck_url"),
        "deck_name_pt_1": deck_1.get("deck_name_pt"),
        "deck_name_pt_2": deck_2.get("deck_name_pt"),
        "deck_name_en_1": deck_1.get("deck_name_en"),
        "deck_name_en_2": deck_2.get("deck_name_en"),
        "colecao_1": deck_1.get("colecao"),
        "colecao_2": deck_2.get("colecao"),
        "map_status": map_status,
        "missing_players": int(not (player_1 and player_2)),
        "missing_decks": int(not (deck_1.get("deck_url") and deck_2.get("deck_url"))),
    }


def looks_like_url(u: str | None) -> bool:
    if not isinstance(u, str):
        return False
    return bool(re.match(r"^https?://", u.strip()))


def canonicalize_url(u: str | None) -> str | None:
    """Normaliza URL (remove query/fragment, forca https, remove www e / final)."""
    if not u or not isinstance(u, str):
        return None
    u = u.strip()

    m = re.search(r"q=(https?://[^&]+)", u)
    if m:
        u = unquote(m.group(1))

    if not re.match(r"^https?://", u):
        return None

    u = u.replace("http://", "https://")
    p = urlparse(u)
    netloc = p.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    p = p._replace(scheme="https", netloc=netloc, query="", fragment="")
    clean = urlunparse(p).rstrip("/")
    return clean


def load_deck_map_csv(path: str = "deck_map.csv") -> dict:
    """
    Espera colunas:
    #,Colecao,Nome PT-BR,Nome ENG,Link
    Retorna dict: link_normalizado -> infos
    """
    if not os.path.exists(path):
        return {}

    dfm = pd.read_csv(path, encoding="utf-8")
    if dfm.empty:
        return {}

    required = ["Link", "Nome PT-BR", "Nome ENG", "Coleção"]
    for c in required:
        if c not in dfm.columns:
            raise SystemExit(f"deck_map.csv: coluna obrigatoria faltando: {c}")

    mapping = {}
    for _, r in dfm.iterrows():
        link = r.get("Link")
        if pd.isna(link):
            continue

        link_norm = canonicalize_url(str(link))
        if not link_norm:
            continue

        mapping[link_norm] = {
            "deck_name_pt": None if pd.isna(r.get("Nome PT-BR")) else str(r.get("Nome PT-BR")),
            "deck_name_en": None if pd.isna(r.get("Nome ENG")) else str(r.get("Nome ENG")),
            "colecao": None if pd.isna(r.get("Coleção")) else str(r.get("Coleção")),
        }

    return mapping


def extract_decklist(obj: dict) -> str | None:
    if not isinstance(obj, dict):
        return None

    dl = obj.get("decklist")
    if dl:
        return fix_moxfield(dl)

    meta = obj.get("metadata")
    if isinstance(meta, dict):
        imported = meta.get("importedFrom")
        if imported:
            return fix_moxfield(imported)

    deckobj = obj.get("deckObj")
    if isinstance(deckobj, dict):
        meta2 = deckobj.get("metadata")
        if isinstance(meta2, dict):
            imported = meta2.get("importedFrom")
            if imported:
                return fix_moxfield(imported)

    return None


def extract_deck_url(obj: dict) -> str | None:
    if not isinstance(obj, dict):
        return None

    deckobj = obj.get("deckObj")
    if isinstance(deckobj, dict):
        meta = deckobj.get("metadata")
        if isinstance(meta, dict):
            imported = meta.get("importedFrom")
            if imported:
                return fix_moxfield(imported)

    meta = obj.get("metadata")
    if isinstance(meta, dict):
        imported = meta.get("importedFrom")
        if imported:
            return fix_moxfield(imported)

    dl = obj.get("decklist")
    if isinstance(dl, str) and looks_like_url(dl):
        return fix_moxfield(dl.strip())

    return None


def extract_commanders(standing: dict) -> str | None:
    deckobj = standing.get("deckObj")
    if not isinstance(deckobj, dict):
        return None
    cmds = deckobj.get("Commanders")
    if isinstance(cmds, dict) and cmds:
        return " / ".join(sorted(cmds.keys()))
    return None


def fetch_tournament_query(tid: str) -> dict:
    """
    Faz POST /api/v2/tournaments (mesmo conceito do Power BI),
    pedindo rounds+tables+players com decklist.
    """
    payload = {
        "columns": ["decklist", "wins", "draws", "losses"],
        "rounds": True,
        "tables": ["table", "players", "winner"],
        "players": ["name", "id", "decklist"],
        "TID": [tid],
        "game": "Magic: The Gathering",
        "format": "EDH",
    }

    r = topdeck_request("POST", "/v2/tournaments", json_payload=payload, timeout=30)
    data = r.json()

    if isinstance(data, list):
        if len(data) == 0:
            raise RuntimeError(f"POST /tournaments retornou lista vazia para: {tid}")
        data = data[0]

    standings = data.get("standings") if isinstance(data, dict) else None
    needs_details = True
    if isinstance(standings, list) and standings:
        sample = standings[0]
        if isinstance(sample, dict) and ("name" in sample or "id" in sample):
            if "opponentWinRate" in sample or "opponentSuccessRate" in sample:
                needs_details = False
    if needs_details:
        try:
            details = fetch_tournament_details(tid)
            if isinstance(details, dict):
                if isinstance(details.get("standings"), list) and details["standings"]:
                    data["standings"] = details["standings"]
                if isinstance(details.get("data"), dict):
                    data["data"] = details["data"]
                if not data.get("tournamentName"):
                    data["tournamentName"] = (
                        (details.get("data") or {}).get("name") if isinstance(details.get("data"), dict) else None
                    )
                if not data.get("startDate"):
                    data["startDate"] = (
                        (details.get("data") or {}).get("startDate") if isinstance(details.get("data"), dict) else None
                    )
        except Exception as exc:
            print(f"Aviso: falha ao buscar standings completos para {tid}: {exc}")

    return data


def fetch_tournament_details(tid: str) -> dict:
    r = topdeck_request("GET", f"/v2/tournaments/{tid}/", timeout=30)
    return r.json()


def fetch_rounds(tid: str) -> list:
    r = topdeck_request("GET", f"/v2/tournaments/{tid}/rounds", timeout=30)
    data = r.json()
    if isinstance(data, dict) and "rounds" in data:
        return data["rounds"]
    return data if isinstance(data, list) else []


def normalize_rounds(raw_rounds: list) -> list:
    """Se 'round' vier zoado, renumera por ordem (1..N)."""
    if not raw_rounds:
        return []

    round_vals = [r.get("round") for r in raw_rounds]
    all_int = all(isinstance(x, int) for x in round_vals)

    if all_int:
        uniq = sorted(set(round_vals))
        if len(uniq) == len(raw_rounds) and uniq == list(range(1, len(raw_rounds) + 1)):
            return sorted(raw_rounds, key=lambda r: r.get("round", 10**9))

    normalized = []
    for idx, r in enumerate(raw_rounds, start=1):
        r2 = dict(r)
        r2["round_norm"] = idx
        normalized.append(r2)
    return normalized


def compute_event_points(standings_df: pd.DataFrame) -> pd.DataFrame:
    if standings_df.empty:
        return standings_df

    df = standings_df.copy()
    df["player_name"] = df["player_name"].map(clean_player_name)
    df = df.dropna(subset=["player_name"])
    if df.empty:
        return standings_df.iloc[0:0].copy()

    df["points_val"] = pd.to_numeric(df.get("points"), errors="coerce").fillna(-10**9)
    df["win_rate_val"] = pd.to_numeric(df.get("win_rate"), errors="coerce").fillna(-10**9)
    df["opp_win_rate_val"] = pd.to_numeric(df.get("opp_win_rate"), errors="coerce").fillna(-10**9)

    def assign(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_values(
            by=["points_val", "win_rate_val", "opp_win_rate_val"],
            ascending=[False, False, False],
            kind="mergesort",
        ).copy()

        tie_keys = list(zip(group["points_val"], group["win_rate_val"], group["opp_win_rate_val"]))
        ranks = []
        current_rank = 0
        last_key = None
        for key in tie_keys:
            if key != last_key:
                current_rank += 1
                last_key = key
            ranks.append(current_rank)

        player_count = group["player_name"].nunique()

        group["event_rank"] = ranks
        group["event_points"] = [points_for_event_position(r, player_count) for r in ranks]
        group["event_players"] = player_count
        return group

    return group_apply_preserve_keys(df, "tid", assign)


def compute_event_points_from_stats(stats_df: pd.DataFrame) -> pd.DataFrame:
    if stats_df.empty:
        return stats_df

    df = stats_df.copy()

    def assign(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_values(
            by=["points_match", "win_rate", "opp_win_rate"],
            ascending=[False, False, False],
            kind="mergesort",
        ).copy()

        tie_keys = list(zip(group["points_match"], group["win_rate"], group["opp_win_rate"]))
        ranks = []
        current_rank = 0
        last_key = None
        for key in tie_keys:
            if key != last_key:
                current_rank += 1
                last_key = key
            ranks.append(current_rank)

        player_count = group["player_name"].nunique()

        group["event_rank"] = ranks
        group["event_points"] = [points_for_event_position(r, player_count) for r in ranks]
        group["event_players"] = player_count
        return group

    return group_apply_preserve_keys(df, "tid", assign)


def compute_event_scores_from_matches(matches_df: pd.DataFrame) -> pd.DataFrame:
    empty_cols = [
        "tid",
        "player_name",
        "tournament_name",
        "start_date",
        "month",
        "matches",
        "wins",
        "draws",
        "losses",
        "points_match",
        "win_rate",
        "opp_win_rate",
        "event_rank",
        "event_points",
        "event_players",
    ]

    if matches_df.empty:
        return pd.DataFrame(columns=empty_cols)

    df = matches_df.copy()
    df["player_name"] = df["player_name"].map(clean_player_name)
    df = df.dropna(subset=["tid", "player_name"])
    if df.empty:
        return pd.DataFrame(columns=empty_cols)

    df["is_winner"] = pd.to_numeric(df.get("is_winner"), errors="coerce").fillna(0).astype(int)

    status_lower = df.get("status", pd.Series([""] * len(df))).astype(str).str.lower()
    winner_id = df.get("winner_id", pd.Series([""] * len(df))).astype(str).str.lower()
    winner_name = df.get("winner_name", pd.Series([""] * len(df))).astype(str).str.lower()

    df["is_draw"] = (
        winner_id.eq("draw")
        | winner_name.eq("draw")
        | status_lower.str.contains("draw", na=False)
        | status_lower.str.contains("empate", na=False)
        | status_lower.str.contains(r"\bid\b", regex=True, na=False)
    ).astype(int)

    df["points_match"] = df["is_winner"] * 3 + df["is_draw"]

    agg = df.groupby(["tid", "player_name"], as_index=False).agg(
        tournament_name=("tournament_name", lambda x: next((v for v in x if pd.notna(v)), None)),
        start_date=("start_date", lambda x: next((v for v in x if pd.notna(v)), None)),
        month=("month", lambda x: next((v for v in x if pd.notna(v)), None)),
        matches=("player_name", "size"),
        wins=("is_winner", "sum"),
        draws=("is_draw", "sum"),
        points_match=("points_match", "sum"),
    )

    agg["losses"] = agg["matches"] - agg["wins"] - agg["draws"]
    agg["win_rate"] = agg["wins"] / agg["matches"]

    opp_chunks = []
    for tid, group in df.groupby("tid"):
        stats = agg[agg["tid"] == tid].copy()
        win_rate_map = dict(zip(stats["player_name"], stats["win_rate"]))
        opp_sum = {name: 0.0 for name in win_rate_map}
        opp_count = {name: 0 for name in win_rate_map}

        for _, tbl in group.groupby(["round", "table"], dropna=False):
            names = [n for n in tbl["player_name"].tolist() if isinstance(n, str)]
            for name in names:
                for opp in names:
                    if opp == name:
                        continue
                    opp_sum[name] = opp_sum.get(name, 0.0) + win_rate_map.get(opp, 0.0)
                    opp_count[name] = opp_count.get(name, 0) + 1

        stats["opp_win_rate"] = stats["player_name"].map(
            lambda n: opp_sum.get(n, 0.0) / opp_count.get(n, 1) if opp_count.get(n, 0) else 0.0
        )
        opp_chunks.append(stats[["tid", "player_name", "opp_win_rate"]])

    if opp_chunks:
        opp_df = pd.concat(opp_chunks, ignore_index=True)
        agg = agg.merge(opp_df, on=["tid", "player_name"], how="left")
    else:
        agg["opp_win_rate"] = 0.0

    agg["opp_win_rate"] = agg["opp_win_rate"].fillna(0.0)

    scored = compute_event_points_from_stats(agg)
    return scored.reindex(columns=empty_cols, fill_value=None)


def compute_fourth_tiebreak_from_matches(matches_df: pd.DataFrame, standings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula 4o criterio de desempate:
    media do winRate dos oponentes unicos enfrentados no evento.
    """
    cols = ["tid", "player_name_norm", "opp2_win_rate_val"]
    if matches_df.empty or standings_df.empty:
        return pd.DataFrame(columns=cols)

    st = standings_df.dropna(subset=["tid", "player_name"]).copy()
    if st.empty:
        return pd.DataFrame(columns=cols)

    st["tid"] = st["tid"].astype("string")
    st["player_name_norm"] = st["player_name"].map(normalize_player_name)
    st["win_rate_val"] = pd.to_numeric(st.get("win_rate"), errors="coerce")
    st = st.dropna(subset=["player_name_norm", "win_rate_val"])
    if st.empty:
        return pd.DataFrame(columns=cols)

    win_map = {
        (str(row.tid), row.player_name_norm): float(row.win_rate_val)
        for row in st.itertuples(index=False)
    }

    m = matches_df.dropna(subset=["tid", "player_name"]).copy()
    if m.empty:
        return pd.DataFrame(columns=cols)
    m["tid"] = m["tid"].astype("string")
    m["player_name_norm"] = m["player_name"].map(normalize_player_name)
    m = m.dropna(subset=["player_name_norm"])
    if m.empty:
        return pd.DataFrame(columns=cols)

    faced = {}
    for (tid, round_no, table_no), table_df in m.groupby(["tid", "round", "table"], dropna=False):
        names = [
            n for n in table_df["player_name_norm"].dropna().tolist() if isinstance(n, str) and n.strip()
        ]
        if len(names) < 2:
            continue
        unique_names = list(dict.fromkeys(names))
        for player_name in unique_names:
            key = (str(tid), player_name)
            s = faced.setdefault(key, set())
            for opp_name in unique_names:
                if opp_name != player_name:
                    s.add(opp_name)

    out = []
    for (tid, player_name_norm), opponents in faced.items():
        opp_rates = [win_map.get((tid, opp)) for opp in opponents]
        opp_rates = [v for v in opp_rates if v is not None and pd.notna(v)]
        if not opp_rates:
            continue
        out.append(
            {
                "tid": tid,
                "player_name_norm": player_name_norm,
                "opp2_win_rate_val": float(sum(opp_rates) / len(opp_rates)),
            }
        )

    if not out:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(out, columns=cols)


def apply_tied_first_place_points(df: pd.DataFrame, tol: float = 1e-9) -> pd.DataFrame:
    """
    Se houver empate tecnico no topo (points + win_rate + opp_win_rate),
    todos os empatados recebem 3 pontos de liga.
    """
    if df.empty:
        return df

    has_4th = "opp2_win_rate_val" in df.columns

    for tid, idx in df.groupby("tid").groups.items():
        group = df.loc[idx]
        valid = group[["points_val", "win_rate_val", "opp_win_rate_val"]].notna().all(axis=1)
        valid_group = group[valid]
        if len(valid_group) < 2:
            continue

        points_cmp = valid_group["points_val"]
        win_cmp = valid_group["win_rate_val"]
        opp_cmp = valid_group["opp_win_rate_val"]

        top_points = points_cmp.max()
        top_mask = points_cmp >= (top_points - tol)
        if not top_mask.any():
            continue

        top_win = win_cmp[top_mask].max()
        top_mask = top_mask & (win_cmp >= (top_win - tol))
        if not top_mask.any():
            continue

        top_opp = opp_cmp[top_mask].max()
        top_mask = top_mask & (opp_cmp >= (top_opp - tol))
        tied_idx = valid_group.index[top_mask]

        if len(tied_idx) < 2:
            continue

        if has_4th:
            tied_group = valid_group.loc[tied_idx]
            fourth_vals = pd.to_numeric(tied_group.get("opp2_win_rate_val"), errors="coerce")
            finite_mask = fourth_vals.notna()
            if finite_mask.any():
                fourth_max = fourth_vals[finite_mask].max()
                best4_idx = tied_group.index[fourth_vals >= (fourth_max - tol)]
                if len(best4_idx) == 1:
                    continue
                tied_idx = best4_idx

        if len(tied_idx) >= 2:
            df.loc[tied_idx, "event_points_official"] = 3

    return df


def build_official_event_rank_df(
    standings_df: pd.DataFrame,
    matches_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    cols = [
        "tid",
        "player_name_norm",
        "event_rank_official",
        "event_points_official",
        "event_players_official",
        "points_official",
    ]
    if standings_df.empty:
        return pd.DataFrame(columns=cols)

    required = {"tid", "player_name", "standing"}
    if not required.issubset(standings_df.columns):
        return pd.DataFrame(columns=cols)

    df = standings_df.dropna(subset=["tid", "player_name"]).copy()
    if df.empty:
        return pd.DataFrame(columns=cols)

    df["event_rank_official"] = pd.to_numeric(df.get("standing"), errors="coerce")
    df["points_official"] = pd.to_numeric(df.get("points"), errors="coerce")
    df = df.dropna(subset=["event_rank_official"])
    if df.empty:
        return pd.DataFrame(columns=cols)

    df["event_rank_official"] = df["event_rank_official"].astype(int)
    df["tid"] = df["tid"].astype("string")
    df["player_name_norm"] = df["player_name"].map(normalize_player_name)
    df = df.dropna(subset=["player_name_norm"])
    if df.empty:
        return pd.DataFrame(columns=cols)

    df = df.sort_values(
        ["tid", "event_rank_official", "player_name_norm"],
        ascending=[True, True, True],
        kind="mergesort",
    )
    df = df.drop_duplicates(subset=["tid", "player_name_norm"], keep="first")
    df["event_players_official"] = df.groupby("tid")["player_name_norm"].transform("nunique")
    df["event_points_official"] = df.apply(
        lambda row: points_for_event_position(int(row["event_rank_official"]), int(row["event_players_official"])),
        axis=1,
    )
    df["points_val"] = pd.to_numeric(df.get("points"), errors="coerce")
    df["win_rate_val"] = pd.to_numeric(df.get("win_rate"), errors="coerce")
    df["opp_win_rate_val"] = pd.to_numeric(df.get("opp_win_rate"), errors="coerce")
    df["opp2_win_rate_val"] = pd.NA
    if matches_df is not None and not matches_df.empty:
        fourth_df = compute_fourth_tiebreak_from_matches(matches_df, standings_df)
        if not fourth_df.empty:
            df = df.merge(fourth_df, on=["tid", "player_name_norm"], how="left", suffixes=("", "_m"))
            if "opp2_win_rate_val_m" in df.columns:
                df["opp2_win_rate_val"] = df["opp2_win_rate_val_m"]
                df = df.drop(columns=["opp2_win_rate_val_m"], errors="ignore")
    df = apply_tied_first_place_points(df)
    return df.reindex(columns=cols)


def apply_official_event_rank(
    event_scores_df: pd.DataFrame,
    standings_df: pd.DataFrame,
    matches_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if event_scores_df.empty:
        return event_scores_df

    official_df = build_official_event_rank_df(standings_df, matches_df=matches_df)
    if official_df.empty:
        return event_scores_df

    scores = event_scores_df.copy()
    scores["tid"] = scores["tid"].astype("string")
    scores["player_name_norm"] = scores["player_name"].map(normalize_player_name)

    scores = scores.merge(
        official_df,
        on=["tid", "player_name_norm"],
        how="left",
    )

    scores["event_rank"] = pd.to_numeric(scores.get("event_rank"), errors="coerce")
    scores["event_points"] = pd.to_numeric(scores.get("event_points"), errors="coerce")
    scores["event_players"] = pd.to_numeric(scores.get("event_players"), errors="coerce")
    scores["points_match"] = pd.to_numeric(scores.get("points_match"), errors="coerce")

    has_official = scores["event_rank_official"].notna()
    scores.loc[has_official, "event_rank"] = scores.loc[has_official, "event_rank_official"]
    scores.loc[has_official, "event_points"] = scores.loc[has_official, "event_points_official"]
    scores.loc[has_official, "event_players"] = scores.loc[has_official, "event_players_official"]
    has_official_points = scores["points_official"].notna()
    scores.loc[has_official_points, "points_match"] = scores.loc[has_official_points, "points_official"]

    return scores.drop(
        columns=[
            "player_name_norm",
            "event_rank_official",
            "event_points_official",
            "event_players_official",
            "points_official",
        ],
        errors="ignore",
    )


def apply_official_rank_to_standings(
    standings_df: pd.DataFrame,
    matches_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if standings_df.empty:
        return standings_df

    official_df = build_official_event_rank_df(standings_df, matches_df=matches_df)
    if official_df.empty:
        fallback = compute_event_points(standings_df)
        return fallback.drop(columns=["points_val", "win_rate_val", "opp_win_rate_val"], errors="ignore")

    df = standings_df.copy()
    df["tid"] = df["tid"].astype("string")
    df["player_name_norm"] = df["player_name"].map(normalize_player_name)
    df = df.merge(official_df, on=["tid", "player_name_norm"], how="left")

    df["event_rank"] = pd.to_numeric(df.get("event_rank"), errors="coerce")
    df["event_points"] = pd.to_numeric(df.get("event_points"), errors="coerce")
    df["event_players"] = pd.to_numeric(df.get("event_players"), errors="coerce")

    has_official = df["event_rank_official"].notna()
    df.loc[has_official, "event_rank"] = df.loc[has_official, "event_rank_official"]
    df.loc[has_official, "event_points"] = df.loc[has_official, "event_points_official"]
    df.loc[has_official, "event_players"] = df.loc[has_official, "event_players_official"]

    df = df.drop(
        columns=[
            "player_name_norm",
            "event_rank_official",
            "event_points_official",
            "event_players_official",
        ],
        errors="ignore",
    )
    return df


def is_draw_result(status: object, winner_id: object = None, winner_name: object = None) -> bool:
    winner_id_text = str(winner_id).strip().lower() if pd.notna(winner_id) else ""
    winner_name_text = str(winner_name).strip().lower() if pd.notna(winner_name) else ""
    status_text = str(status).strip().lower() if pd.notna(status) else ""
    return (
        winner_id_text == "draw"
        or winner_name_text == "draw"
        or "draw" in status_text
        or "empate" in status_text
        or bool(re.search(r"\bid\b", status_text))
    )


def resolve_deck_label(row: pd.Series) -> str | None:
    for key in ["deck_name_pt", "deck_name_en", "deck_url"]:
        value = row.get(key)
        if pd.notna(value):
            text = str(value).strip()
            if text:
                return text
    return None


def build_player_matchups(matches_df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "tid",
        "tournament_name",
        "start_date",
        "event_day",
        "month",
        "round",
        "table",
        "player_id",
        "player_name",
        "player_deck",
        "opponent_id",
        "opponent_name",
        "opponent_deck",
        "result",
    ]

    if matches_df.empty:
        return pd.DataFrame(columns=cols)

    df = matches_df.copy()
    df["player_name"] = df["player_name"].map(clean_player_name)
    df = df.dropna(subset=["tid", "player_name"])
    if df.empty:
        return pd.DataFrame(columns=cols)

    required = [
        "tournament_name",
        "start_date",
        "month",
        "round",
        "table",
        "player_id",
        "player_name",
        "is_winner",
        "status",
        "winner_id",
        "winner_name",
        "deck_name_pt",
        "deck_name_en",
        "deck_url",
    ]
    for col in required:
        if col not in df.columns:
            df[col] = None

    df["player_deck"] = df.apply(resolve_deck_label, axis=1)

    out_rows = []
    for _, table_df in df.groupby(["tid", "round", "table"], dropna=False, sort=False):
        participants = table_df.reset_index(drop=True)
        for i, player in participants.iterrows():
            player_name = player.get("player_name")
            if not isinstance(player_name, str) or not player_name.strip():
                continue
            player_name = clean_player_name(player_name)
            if not player_name:
                continue

            player_id = player.get("player_id")
            norm_player = normalize_player_name(player_name)

            if is_draw_result(player.get("status"), player.get("winner_id"), player.get("winner_name")):
                result = "draw"
            else:
                try:
                    is_winner = int(player.get("is_winner"))
                except Exception:
                    is_winner = 0
                result = "win" if is_winner == 1 else "lose"

            for j, opp in participants.iterrows():
                if i == j:
                    continue

                opp_name = opp.get("player_name")
                if not isinstance(opp_name, str) or not opp_name.strip():
                    continue
                opp_name = clean_player_name(opp_name)
                if not opp_name:
                    continue

                opp_id = opp.get("player_id")
                same_player = False
                if pd.notna(player_id) and pd.notna(opp_id):
                    same_player = str(player_id) == str(opp_id)
                elif norm_player:
                    same_player = norm_player == normalize_player_name(opp_name)
                if same_player:
                    continue

                out_rows.append(
                    {
                        "tid": player.get("tid"),
                        "tournament_name": player.get("tournament_name"),
                        "start_date": player.get("start_date"),
                        "event_day": player.get("start_date"),
                        "month": player.get("month"),
                        "round": player.get("round"),
                        "table": player.get("table"),
                        "player_id": player_id,
                        "player_name": player_name,
                        "player_deck": player.get("player_deck"),
                        "opponent_id": opp_id,
                        "opponent_name": opp_name,
                        "opponent_deck": opp.get("player_deck"),
                        "result": result,
                    }
                )

    if not out_rows:
        return pd.DataFrame(columns=cols)

    player_df = pd.DataFrame(out_rows, columns=cols)
    player_df = player_df.sort_values(
        ["start_date", "tid", "round", "table", "player_name", "opponent_name"],
        ascending=[True, True, True, True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)
    return player_df


def enrich_two_by_two_team_frame(
    frame: pd.DataFrame,
    team_map: dict[tuple[str, str], dict],
    player_aliases: dict[str, str],
    deck_map: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    info_cols = [
        "raw_team",
        "team_name",
        "team_key",
        "player_1",
        "player_2",
        "deck_1",
        "deck_2",
        "deck_url_1",
        "deck_url_2",
        "deck_name_pt_1",
        "deck_name_pt_2",
        "deck_name_en_1",
        "deck_name_en_2",
        "colecao_1",
        "colecao_2",
        "map_status",
        "missing_players",
        "missing_decks",
    ]
    if frame.empty:
        return frame.copy(), pd.DataFrame(columns=["tid", *info_cols])

    out = frame.copy()
    infos = []
    for row in out.itertuples(index=False):
        tid = getattr(row, "tid", None)
        raw_team = getattr(row, "player_name", None)
        infos.append(resolve_two_by_two_team_info(tid, raw_team, team_map, player_aliases, deck_map))

    info_df = pd.DataFrame(infos)
    for col in info_cols:
        out[col] = info_df[col].values if col in info_df.columns else None

    out = out[out["raw_team"].map(clean_player_name).notna()].copy()
    if out.empty:
        return out, pd.DataFrame(columns=["tid", *info_cols])

    if "player_name" in out.columns:
        out["player_name"] = out["team_name"]

    team_map_df = (
        out[
            [
                "tid",
                "tournament_name",
                "start_date",
                "month",
                *info_cols,
            ]
        ]
        .drop_duplicates(subset=["tid", "raw_team"])
        .sort_values(["tid", "team_name"], kind="mergesort")
        .reset_index(drop=True)
    )
    return out, team_map_df


def normalize_two_by_two_winner_names(matches_df: pd.DataFrame, team_map_df: pd.DataFrame) -> pd.DataFrame:
    if matches_df.empty or team_map_df.empty or "winner_name" not in matches_df.columns:
        return matches_df

    out = matches_df.copy()
    lookup = {}
    for row in team_map_df.itertuples(index=False):
        tid = getattr(row, "tid", None)
        for value in [getattr(row, "raw_team", None), getattr(row, "team_name", None)]:
            key = (clean_player_name(tid), normalize_team_key(value))
            if key[0] and key[1]:
                lookup[key] = getattr(row, "team_name", None)

    def resolve(row):
        winner = row.get("winner_name")
        if not isinstance(winner, str) or not winner.strip():
            return winner
        if is_draw_result(row.get("status"), row.get("winner_id"), winner):
            return clean_player_name(winner)
        key = (clean_player_name(row.get("tid")), normalize_team_key(winner))
        return lookup.get(key, clean_player_name(winner))

    out["winner_name"] = out.apply(resolve, axis=1)
    return out


def expand_two_by_two_matches_to_players(team_matches_df: pd.DataFrame) -> pd.DataFrame:
    if team_matches_df.empty:
        return team_matches_df.copy()

    rows = []
    for row in team_matches_df.to_dict(orient="records"):
        slots = [
            {
                "slot": 1,
                "player_name": row.get("player_1"),
                "teammate_name": row.get("player_2"),
                "deck_key": row.get("deck_1"),
                "deck_url": row.get("deck_url_1"),
                "deck_name_pt": row.get("deck_name_pt_1"),
                "deck_name_en": row.get("deck_name_en_1"),
                "colecao": row.get("colecao_1"),
            },
            {
                "slot": 2,
                "player_name": row.get("player_2"),
                "teammate_name": row.get("player_1"),
                "deck_key": row.get("deck_2"),
                "deck_url": row.get("deck_url_2"),
                "deck_name_pt": row.get("deck_name_pt_2"),
                "deck_name_en": row.get("deck_name_en_2"),
                "colecao": row.get("colecao_2"),
            },
        ]
        for slot in slots:
            player_name = clean_player_name(slot["player_name"])
            if not player_name:
                continue

            out = dict(row)
            out["team_player_slot"] = slot["slot"]
            out["team_name"] = row.get("team_name")
            out["team_key"] = row.get("team_key")
            out["raw_team"] = row.get("raw_team")
            out["teammate_name"] = clean_player_name(slot["teammate_name"])
            out["player_name"] = player_name
            out["player_id"] = f'{row.get("player_id") or row.get("team_key") or row.get("team_name")}#{slot["slot"]}'
            out["deck_key"] = slot["deck_key"]
            out["deck_url"] = slot["deck_url"]
            out["deck_name_pt"] = slot["deck_name_pt"] or slot["deck_key"]
            out["deck_name_en"] = slot["deck_name_en"]
            out["colecao"] = slot["colecao"]
            rows.append(out)

    return pd.DataFrame(rows)


def expand_two_by_two_scores_to_players(team_scores_df: pd.DataFrame, team_map_df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "tid",
        "player_name",
        "team_name",
        "team_key",
        "tournament_name",
        "start_date",
        "month",
        "matches",
        "wins",
        "draws",
        "losses",
        "points_match",
        "win_rate",
        "opp_win_rate",
        "event_rank",
        "event_points",
        "event_players",
        "event_teams",
    ]
    if team_scores_df.empty:
        return pd.DataFrame(columns=cols)

    meta_cols = ["tid", "team_name", "team_key", "player_1", "player_2"]
    meta = team_map_df.reindex(columns=meta_cols).drop_duplicates(subset=["tid", "team_name"])
    merged = team_scores_df.merge(
        meta,
        left_on=["tid", "player_name"],
        right_on=["tid", "team_name"],
        how="left",
    )

    rows = []
    for row in merged.to_dict(orient="records"):
        for player_col in ["player_1", "player_2"]:
            player_name = clean_player_name(row.get(player_col))
            if not player_name:
                continue
            out = {k: row.get(k) for k in team_scores_df.columns}
            out["team_name"] = row.get("team_name") or row.get("player_name")
            out["team_key"] = row.get("team_key")
            out["player_name"] = player_name
            out["event_teams"] = row.get("event_players")
            rows.append(out)

    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(rows).reindex(columns=cols)


def compute_deck_stats_from_matches(matches_df: pd.DataFrame) -> pd.DataFrame:
    deck_stats_cols = [
        "deck_key",
        "partidas_jogadas",
        "vitorias",
        "empates",
        "jogadores_unicos",
        "eventos",
        "derrotas",
        "win_rate",
        "deck_name_pt",
        "deck_name_en",
        "colecao",
        "deck_url",
    ]
    deck_stats_df = pd.DataFrame(columns=deck_stats_cols)
    if matches_df.empty:
        return deck_stats_df

    played = matches_df.copy()
    if "deck_key" not in played.columns:
        if "deck_name_pt" in played.columns:
            played["deck_key"] = played["deck_name_pt"]
        else:
            played["deck_key"] = None
        if "deck_url" in played.columns:
            played["deck_key"] = played["deck_key"].fillna(played["deck_url"])

    played["deck_key"] = played["deck_key"].astype("string").str.strip()
    played["deck_key"] = played["deck_key"].mask(played["deck_key"].eq(""))
    played["deck_key"] = played["deck_key"].mask(played["deck_key"].map(is_placeholder_deck))
    played = played.dropna(subset=["deck_key", "player_name"]).copy()
    if played.empty:
        return deck_stats_df

    winner_id = played.get("winner_id", pd.Series([""] * len(played))).astype(str).str.lower()
    winner_name = played.get("winner_name", pd.Series([""] * len(played))).astype(str).str.lower()
    status_lower = played.get("status", pd.Series([""] * len(played))).astype(str).str.lower()
    played["is_draw"] = (
        winner_id.eq("draw")
        | winner_name.eq("draw")
        | status_lower.str.contains("draw", na=False)
        | status_lower.str.contains("empate", na=False)
        | status_lower.str.contains(r"\bid\b", regex=True, na=False)
    ).astype(int)

    played["wins"] = played["is_winner"].fillna(0).astype(int)
    played["draws"] = played["is_draw"].fillna(0).astype(int)

    agg = played.groupby("deck_key").agg(
        partidas_jogadas=("deck_key", "size"),
        vitorias=("wins", "sum"),
        empates=("draws", "sum"),
        jogadores_unicos=("player_name", "nunique"),
        eventos=("tid", "nunique"),
    ).reset_index()

    agg["derrotas"] = agg["partidas_jogadas"] - agg["vitorias"] - agg["empates"]
    agg["win_rate"] = (agg["vitorias"] / agg["partidas_jogadas"]).fillna(0)

    for c in ["deck_name_pt", "deck_name_en", "colecao", "deck_url"]:
        if c not in played.columns:
            played[c] = None

    meta = played.groupby("deck_key").agg(
        deck_name_pt=("deck_name_pt", lambda x: next((v for v in x if pd.notna(v)), None)),
        deck_name_en=("deck_name_en", lambda x: next((v for v in x if pd.notna(v)), None)),
        colecao=("colecao", lambda x: next((v for v in x if pd.notna(v)), None)),
        deck_url=("deck_url", lambda x: next((v for v in x if isinstance(v, str)), None)),
    ).reset_index()

    deck_stats_df = agg.merge(meta, on="deck_key", how="left").sort_values(
        "partidas_jogadas", ascending=False
    )
    return deck_stats_df.reindex(columns=deck_stats_cols)


def expand_absences_with_zero_rows(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = {"quarter", "player_name", "tid"}
    if df.empty or not required_cols.issubset(df.columns):
        return df

    valid = df.dropna(subset=list(required_cols)).copy()
    if valid.empty:
        return df

    player_quarters = valid[["quarter", "player_name"]].drop_duplicates()
    meta_cols = [
        col
        for col in ["month", "start_date", "start_date_val", "tournament_name", "event_players"]
        if col in valid.columns
    ]

    if "start_date_val" in valid.columns:
        event_meta = (
            valid[["quarter", "tid", *meta_cols]]
            .sort_values(["quarter", "start_date_val", "tid"], ascending=[True, True, True], kind="mergesort")
            .drop_duplicates(subset=["quarter", "tid"], keep="first")
        )
    else:
        event_meta = (
            valid[["quarter", "tid", *meta_cols]]
            .sort_values(["quarter", "tid"], ascending=[True, True], kind="mergesort")
            .drop_duplicates(subset=["quarter", "tid"], keep="first")
        )

    full_grid = player_quarters.merge(event_meta, on="quarter", how="inner")
    if full_grid.empty:
        return df

    present = valid[["quarter", "player_name", "tid"]].drop_duplicates().assign(_present=True)
    missing = full_grid.merge(present, on=["quarter", "player_name", "tid"], how="left")
    missing = missing[missing["_present"].isna()].drop(columns=["_present"])
    if missing.empty:
        return df

    missing_rows = pd.DataFrame(index=missing.index, columns=df.columns)
    for col in missing.columns:
        if col in missing_rows.columns:
            missing_rows[col] = missing[col].values

    for col, default in [
        ("event_points", 0),
        ("points_match", 0),
        ("win_rate", 0.0),
        ("opp_win_rate", 0.0),
    ]:
        if col in missing_rows.columns:
            missing_rows[col] = default

    return pd.concat([df, missing_rows], ignore_index=True)


def compute_quarterly_points(
    events_df: pd.DataFrame,
    discard_worst_results: bool = True,
    preserve_best_month: bool = True,
    include_absences_as_zero: bool = False,
    discard_count: int = 3,
) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame(columns=["player_name", "league_points"])

    df = events_df.dropna(subset=["month", "player_name"]).copy()
    df["player_name"] = df["player_name"].map(clean_player_name)
    df = df.dropna(subset=["player_name"])
    if df.empty:
        return pd.DataFrame(columns=["player_name", "league_points"])

    df["quarter"] = df["month"].map(quarter_key_from_month)
    df = df.dropna(subset=["quarter"])
    if df.empty:
        return pd.DataFrame(columns=["player_name", "league_points"])

    df["event_points"] = pd.to_numeric(df.get("event_points"), errors="coerce").fillna(0)
    df["points_match"] = pd.to_numeric(df.get("points_match"), errors="coerce").fillna(0)
    df["win_rate"] = pd.to_numeric(df.get("win_rate"), errors="coerce").fillna(0.0)
    df["opp_win_rate"] = pd.to_numeric(df.get("opp_win_rate"), errors="coerce").fillna(0.0)
    df["start_date_val"] = pd.to_datetime(df.get("start_date"), errors="coerce")

    if include_absences_as_zero:
        df = expand_absences_with_zero_rows(df)

    sort_cols = [
        "event_points",
        "points_match",
        "win_rate",
        "opp_win_rate",
        "start_date_val",
    ]
    sort_asc = [True, True, True, True, True]

    if not discard_worst_results:
        league = (
            df.groupby("player_name", as_index=False)["event_points"]
            .sum()
            .rename(columns={"event_points": "league_points"})
        )
        return league

    if preserve_best_month:
        best_month = (
            df.sort_values(sort_cols, ascending=[False, False, False, False, False], kind="mergesort")
            .groupby(["player_name", "quarter", "month"], as_index=False)
            .head(1)
        )

        df = df.merge(
            best_month[["tid", "player_name"]].assign(preserved=True),
            on=["tid", "player_name"],
            how="left",
        )
    else:
        df["preserved"] = False

    df["preserved"] = df["preserved"].fillna(False).astype(bool)

    def apply_discards(group: pd.DataFrame) -> pd.DataFrame:
        group = group.copy()
        if len(group) < 4:
            group["kept"] = True
            return group

        group["kept"] = True
        candidates = group[~group["preserved"]].copy()
        if not candidates.empty:
            candidates = candidates.sort_values(sort_cols, ascending=sort_asc, kind="mergesort")
            drop_n = max(0, int(discard_count))
            if drop_n > 0:
                to_drop = candidates.head(drop_n).index
                group.loc[to_drop, "kept"] = False
        return group

    df = group_apply_preserve_keys(df, ["player_name", "quarter"], apply_discards)
    kept = df[df["kept"]]
    league = (
        kept.groupby("player_name", as_index=False)["event_points"]
        .sum()
        .rename(columns={"event_points": "league_points"})
    )
    return league


def clean_nan(obj):
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass
    return obj



def normalize_league(value: str | None) -> str:
    if value is None:
        return DEFAULT_LEAGUE
    if isinstance(value, str):
        league = value.strip().lower()
    else:
        league = str(value).strip().lower()
    league = LEAGUE_ALIASES.get(league, league)
    return league if league in VALID_LEAGUES else DEFAULT_LEAGUE


def split_events_by_league(events: list[dict]) -> dict[str, list[dict]]:
    grouped = {league: [] for league in LEAGUE_ORDER}
    for e in events:
        league = normalize_league(e.get("league"))
        grouped.setdefault(league, []).append(e)
    return grouped


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    val = str(raw).strip().lower()
    return val in {"1", "true", "yes", "on"}


def main():
    run_started_at = datetime.now().astimezone()
    site_meta = {
        "last_updated": run_started_at.strftime("%Y-%m-%d"),
        "last_updated_at": run_started_at.isoformat(timespec="seconds"),
    }
    offline_mode = env_flag("TOPDECK_OFFLINE", default=False)
    if not API_KEY and not offline_mode:
        raise SystemExit("Defina TOPDECK_API_KEY no terminal (nao coloque a key no arquivo).")
    if offline_mode:
        print("Modo offline ativo (TOPDECK_OFFLINE=1): usando apenas RAW local.")

    with open("events.json", "r", encoding="utf-8-sig") as f:
        events = json.load(f)

    aliases = load_player_aliases("player_aliases.json")

    deck_map = load_deck_map_csv("deck_map.csv")
    team_map_2x2 = load_team_map_2x2(TEAM_MAP_2X2_PATH)
    team_map_2x2_rows = 0
    if os.path.exists(TEAM_MAP_2X2_PATH):
        team_map_2x2_rows = len(pd.read_csv(TEAM_MAP_2X2_PATH, encoding="utf-8-sig"))
    print(
        f"Aliases carregados: {len(aliases)} | Deck map carregado: {len(deck_map)} | "
        f"Team map 2x2 carregado: {team_map_2x2_rows} linha(s), {len(team_map_2x2)} chave(s)"
    )
    grouped = split_events_by_league(events)
    summaries = []

    legacy_raw_dir = os.path.join("data", "raw")
    if os.path.isdir(legacy_raw_dir):
        legacy_removed = 0
        for name in os.listdir(legacy_raw_dir):
            path = os.path.join(legacy_raw_dir, name)
            if os.path.isfile(path) and name.endswith(".json"):
                os.remove(path)
                legacy_removed += 1
        if legacy_removed:
            print(f"{legacy_raw_dir} legado limpo: {legacy_removed} arquivo(s) removido(s)")
    for league_type in LEAGUE_ORDER:
        events = grouped.get(league_type, [])
        is_2x2 = league_type in TWO_BY_TWO_LEAGUES
        event_tids = {str(e.get("tid")) for e in events if e.get("tid")}
        raw_dir = os.path.join("data", "raw", league_type)
        out_dir = os.path.join("data", league_type)
        print(f"==== Processando liga {league_type} ({len(event_tids)} evento(s)) ====")
        os.makedirs(raw_dir, exist_ok=True)
        os.makedirs(out_dir, exist_ok=True)
        removed = 0
        for name in os.listdir(raw_dir):
            if not name.endswith(".json"):
                continue
            raw_path = os.path.join(raw_dir, name)
            tid_from_file = name[:-5]
            if os.path.isfile(raw_path) and tid_from_file not in event_tids:
                os.remove(raw_path)
                removed += 1
        if removed:
            print(f"{raw_dir} limpo: {removed} arquivo(s) stale removido(s)")

        rows = []
        match_rows = []
        table_rows = []
        failed_tids = []

        deck_by_pid = {}
        deck_by_name = {}
        player_name_by_pid = {}

        for e in events:
            tid = str(e["tid"])
            raw_path = os.path.join(raw_dir, f"{tid}.json")
            print(f"\n=== Processando: {tid} ===")
            data = None

            if offline_mode:
                if os.path.exists(raw_path):
                    with open(raw_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    print(f"Modo offline: RAW local carregado para {tid}.")
                else:
                    print(f"Erro: RAW local ausente para {tid}. Pulando evento.")
                    failed_tids.append(tid)
                    continue
            else:
                try:
                    data = fetch_tournament_query(tid)
                    with open(raw_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                except Exception as exc:
                    if os.path.exists(raw_path):
                        print(f"Aviso: falha ao baixar {tid}: {exc}. Usando raw local.")
                        with open(raw_path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                    else:
                        print(f"Erro: falha ao baixar {tid}: {exc}. Pulando evento.")
                        failed_tids.append(tid)
                        with open(raw_path, "w", encoding="utf-8") as f:
                            json.dump(
                                {"tid": tid, "error": str(exc), "fetched": False},
                                f,
                                ensure_ascii=False,
                                indent=2,
                            )
                        continue

            if isinstance(data, dict) and data.get("fetched") is False and data.get("error"):
                print(f"Erro: RAW local de erro para {tid}: {data.get('error')}. Pulando evento.")
                failed_tids.append(tid)
                continue

            tdata = data.get("data", {}) if isinstance(data.get("data", {}), dict) else {}
            tournament_name = data.get("tournamentName") or tdata.get("name") or tid
            start_ts = data.get("startDate") or tdata.get("startDate")

            start_date = unix_to_date(start_ts)
            mkey = month_key(start_ts)

            for s in data.get("standings", []):
                name = resolve_player_alias(s.get("name"), aliases)
                win_rate = s.get("winRate", s.get("successRate"))
                opp_wr = s.get("opponentWinRate", s.get("opponentSuccessRate"))
                pid = s.get("id")
                pid = str(pid) if pid is not None else None
                if pid and name:
                    player_name_by_pid[(tid, pid)] = name

                deck_url = canonicalize_url(extract_deck_url(s))
                name_key = normalize_player_name(name)
                if deck_url:
                    if pid:
                        deck_by_pid[(tid, pid)] = deck_url
                    if name_key:
                        deck_by_name[(tid, name_key)] = deck_url

                rows.append(
                    {
                        "tid": tid,
                        "tournament_name": tournament_name,
                        "start_date": start_date,
                        "month": mkey,
                        "player_name": name,
                        "player_id": pid,
                        "standing": s.get("standing"),
                        "points": s.get("points"),
                        "win_rate": win_rate,
                        "opp_win_rate": opp_wr,
                        "decklist": extract_decklist(s),
                        "commanders": extract_commanders(s),
                    }
                )

            raw_rounds = data.get("rounds")
            if raw_rounds is None and not offline_mode:
                raw_rounds = fetch_rounds(tid)
            if raw_rounds is None:
                raw_rounds = []

            norm_rounds = normalize_rounds(raw_rounds)

            seen_tables = set()
            tables_added = 0
            matches_added = 0

            for rnd in norm_rounds or []:
                round_val = rnd.get("round")
                round_no = round_val if isinstance(round_val, int) else rnd.get("round_norm")

                for tbl in rnd.get("tables", []):
                    table_no = tbl.get("table")
                    status = tbl.get("status")

                    key = (tid, round_no, str(table_no))
                    if key in seen_tables:
                        continue
                    seen_tables.add(key)

                    table_rows.append(
                        {
                            "tid": tid,
                            "tournament_name": tournament_name,
                            "start_date": start_date,
                            "round": round_no,
                            "table": table_no,
                            "status": status,
                        }
                    )
                    tables_added += 1

                    winner_id = tbl.get("winner_id")
                    winner_name = tbl.get("winner")
                    if isinstance(winner_name, dict):
                        winner_id = winner_id or winner_name.get("id")
                        winner_name = winner_name.get("name")
                    winner_id = str(winner_id) if winner_id is not None else None
                    winner_name = (
                        player_name_by_pid.get((tid, winner_id))
                        if winner_id
                        else None
                    ) or resolve_player_alias(winner_name, aliases)

                    for p in tbl.get("players", []):
                        pname = resolve_player_alias(p.get("name"), aliases)
                        pid = p.get("id")
                        pid = str(pid) if pid is not None else None
                        if pid:
                            pname = player_name_by_pid.get((tid, pid), pname)

                        deck_raw = extract_decklist(p)
                        deck_url = canonicalize_url(extract_deck_url(p))
                        if not deck_url:
                            if pid:
                                deck_url = deck_by_pid.get((tid, pid))
                            if not deck_url:
                                name_key = normalize_player_name(pname)
                                if name_key:
                                    deck_url = deck_by_name.get((tid, name_key))

                        match_rows.append(
                            {
                                "tid": tid,
                                "tournament_name": tournament_name,
                                "start_date": start_date,
                                "month": mkey,
                                "round": round_no,
                                "table": table_no,
                                "status": status,
                                "winner_id": winner_id,
                                "winner_name": winner_name,
                                "player_id": pid,
                                "player_name": pname,
                                "is_winner": 1
                                if (
                                    (winner_id is not None and pid is not None and pid == winner_id)
                                    or (
                                        isinstance(winner_name, str)
                                        and normalize_player_name(winner_name)
                                        and normalize_player_name(winner_name)
                                        == normalize_player_name(pname)
                                    )
                                )
                                else 0,
                                "deck_url": deck_url,
                                "deck_raw": deck_raw,
                            }
                        )
                        matches_added += 1

            print(
                f"Rounds recebidos: {len(norm_rounds)} | Mesas: {tables_added} | Linhas matches: {matches_added}"
            )

        standings_cols = [
            "tid",
            "tournament_name",
            "start_date",
            "month",
            "player_name",
            "player_id",
            "standing",
            "points",
            "win_rate",
            "opp_win_rate",
            "decklist",
            "commanders",
        ]
        tables_cols = ["tid", "tournament_name", "start_date", "round", "table", "status"]
        matches_cols = [
            "tid",
            "tournament_name",
            "start_date",
            "month",
            "round",
            "table",
            "status",
            "winner_id",
            "winner_name",
            "player_id",
            "player_name",
            "is_winner",
            "deck_url",
            "deck_raw",
        ]

        df = pd.DataFrame(rows).reindex(columns=standings_cols)
        tables_df = pd.DataFrame(table_rows).reindex(columns=tables_cols)
        matches_df = pd.DataFrame(match_rows).reindex(columns=matches_cols)

        for frame, columns in (
            (df, ["player_name"]),
            (matches_df, ["winner_name", "player_name"]),
        ):
            if frame.empty:
                continue
            for col in columns:
                if col in frame.columns:
                    frame[col] = frame[col].map(clean_player_name)

        if event_tids:
            df = df[df["tid"].isin(event_tids)]
            tables_df = tables_df[tables_df["tid"].isin(event_tids)]
            matches_df = matches_df[matches_df["tid"].isin(event_tids)]

        if is_2x2:
            team_df, team_map_from_standings = enrich_two_by_two_team_frame(
                df, team_map_2x2, aliases, deck_map
            )
            team_matches_df, team_map_from_matches = enrich_two_by_two_team_frame(
                matches_df, team_map_2x2, aliases, deck_map
            )
            team_map_df = (
                pd.concat([team_map_from_standings, team_map_from_matches], ignore_index=True)
                if not team_map_from_standings.empty or not team_map_from_matches.empty
                else pd.DataFrame(columns=team_map_from_standings.columns)
            )
            if not team_map_df.empty:
                team_map_df = (
                    team_map_df.drop_duplicates(subset=["tid", "raw_team"])
                    .sort_values(["tid", "team_name"], kind="mergesort")
                    .reset_index(drop=True)
                )

            team_matches_df = normalize_two_by_two_winner_names(team_matches_df, team_map_df)
            player_matches_df = expand_two_by_two_matches_to_players(team_matches_df)

            team_event_scores_df = compute_event_scores_from_matches(team_matches_df)
            team_event_scores_df = apply_official_event_rank(
                team_event_scores_df,
                team_df,
                matches_df=team_matches_df,
            )
            player_event_scores_df = expand_two_by_two_scores_to_players(team_event_scores_df, team_map_df)

            team_df = apply_official_rank_to_standings(team_df, matches_df=team_matches_df)

            df2 = player_event_scores_df.dropna(subset=["month", "player_name"]).copy()
            df2["event_points"] = pd.to_numeric(df2.get("event_points"), errors="coerce").fillna(0)
            df2["points_match"] = pd.to_numeric(df2.get("points_match"), errors="coerce").fillna(0)
            df2["win_rate"] = pd.to_numeric(df2.get("win_rate"), errors="coerce").fillna(0.0)
            df2["opp_win_rate"] = pd.to_numeric(df2.get("opp_win_rate"), errors="coerce").fillna(0.0)
            df2["start_date_val"] = pd.to_datetime(df2.get("start_date"), errors="coerce")
            df2 = df2.sort_values(
                by=[
                    "month",
                    "player_name",
                    "event_points",
                    "points_match",
                    "win_rate",
                    "opp_win_rate",
                    "start_date_val",
                ],
                ascending=[True, True, False, False, False, False, False],
            )

            monthly_best = df2.groupby(["month", "player_name"], as_index=False).head(1)
            if "event_points" not in monthly_best.columns:
                monthly_best["event_points"] = 0
            monthly_best = monthly_best.drop(columns=["start_date_val"], errors="ignore")

            league = compute_quarterly_points(
                df2,
                discard_worst_results=True,
                preserve_best_month=False,
                include_absences_as_zero=True,
                discard_count=discard_count_for_league(league_type),
            )
            if not df2.empty:
                event_counts = df2.groupby("player_name", as_index=False)["tid"].nunique().rename(
                    columns={"tid": "eventos"}
                )
                league = league.merge(event_counts, on="player_name", how="left")
            else:
                league["eventos"] = 0

            deck_principal = {}
            if not player_matches_df.empty and "deck_key" in player_matches_df.columns:
                tmp = player_matches_df.dropna(subset=["player_name", "deck_key"]).copy()
                tmp["deck_key"] = tmp["deck_key"].astype("string").str.strip()
                tmp = tmp[tmp["deck_key"].notna() & tmp["deck_key"].ne("")]
                tmp = tmp[~tmp["deck_key"].map(is_placeholder_deck)]
                if not tmp.empty:
                    counts = (
                        tmp.groupby(["player_name", "deck_key"])
                        .size()
                        .reset_index(name="games")
                        .sort_values(["player_name", "games", "deck_key"], ascending=[True, False, True])
                    )
                    deck_principal = (
                        counts.groupby("player_name").head(1).set_index("player_name")["deck_key"].to_dict()
                    )

            league["deck_principal"] = league["player_name"].map(lambda n: deck_principal.get(n))
            league = league.sort_values(
                ["league_points", "eventos", "player_name"],
                ascending=[False, False, True],
            )

            team_df2 = team_event_scores_df.dropna(subset=["month", "player_name"]).copy()
            team_identity = pd.DataFrame(columns=["tid", "team_name", "team_key", "display_team_name"])
            if not team_map_df.empty:
                team_identity = team_map_df.reindex(
                    columns=["tid", "team_name", "team_key", "player_1", "player_2"]
                ).copy()
                team_identity["display_team_name"] = team_identity.apply(
                    lambda row: canonical_two_by_two_team_name(row.get("player_1"), row.get("player_2"))
                    or row.get("team_name"),
                    axis=1,
                )
                team_identity = team_identity.drop_duplicates(subset=["tid", "team_name"])
            if not team_df2.empty and not team_identity.empty:
                team_df2 = team_df2.merge(
                    team_identity[["tid", "team_name", "team_key", "display_team_name"]],
                    left_on=["tid", "player_name"],
                    right_on=["tid", "team_name"],
                    how="left",
                )
            if "team_key" not in team_df2.columns:
                team_df2["team_key"] = None
            team_df2["team_key"] = team_df2["team_key"].fillna(team_df2["player_name"].map(normalize_team_key))
            if "display_team_name" not in team_df2.columns:
                team_df2["display_team_name"] = None
            team_df2["display_team_name"] = team_df2["display_team_name"].fillna(team_df2["player_name"])
            team_display = (
                team_df2.dropna(subset=["team_key", "display_team_name"])
                .groupby("team_key")["display_team_name"]
                .agg(lambda values: values.value_counts().index[0])
                .reset_index()
                .rename(columns={"display_team_name": "team_name"})
            )
            team_df2["player_name"] = team_df2["team_key"]
            team_df2["event_points"] = pd.to_numeric(team_df2.get("event_points"), errors="coerce").fillna(0)
            team_df2["points_match"] = pd.to_numeric(team_df2.get("points_match"), errors="coerce").fillna(0)
            team_df2["win_rate"] = pd.to_numeric(team_df2.get("win_rate"), errors="coerce").fillna(0.0)
            team_df2["opp_win_rate"] = pd.to_numeric(team_df2.get("opp_win_rate"), errors="coerce").fillna(0.0)
            team_league = compute_quarterly_points(
                team_df2,
                discard_worst_results=True,
                preserve_best_month=False,
                include_absences_as_zero=True,
                discard_count=discard_count_for_league(league_type),
            ).rename(columns={"player_name": "team_key"})
            if not team_df2.empty:
                team_counts = team_df2.groupby("player_name", as_index=False)["tid"].nunique().rename(
                    columns={"player_name": "team_key", "tid": "eventos"}
                )
                team_league = team_league.merge(team_counts, on="team_key", how="left")
            else:
                team_league["eventos"] = 0
            if not team_display.empty:
                team_league = team_league.merge(team_display, on="team_key", how="left")
            else:
                team_league["team_name"] = team_league["team_key"]
            team_league["team_name"] = team_league["team_name"].fillna(team_league["team_key"])
            team_league = team_league.sort_values(
                ["league_points", "eventos", "team_name"],
                ascending=[False, False, True],
            )
            team_league = team_league.reindex(columns=["team_name", "team_key", "league_points", "eventos"])

            unmapped_teams_df = team_map_df[
                (pd.to_numeric(team_map_df.get("missing_players"), errors="coerce").fillna(1) > 0)
                | (pd.to_numeric(team_map_df.get("missing_decks"), errors="coerce").fillna(1) > 0)
            ].copy()
            unmapped_teams_df.to_csv(os.path.join(out_dir, "unmapped_teams.csv"), index=False, encoding="utf-8")

            unmapped_decks_cols = [
                "tid",
                "tournament_name",
                "start_date",
                "team_name",
                "raw_team",
                "player_name",
                "slot",
                "deck",
            ]
            missing_deck_rows = []
            for row in team_map_df.to_dict(orient="records"):
                for slot in [1, 2]:
                    if clean_player_name(row.get(f"deck_url_{slot}")):
                        continue
                    missing_deck_rows.append(
                        {
                            "tid": row.get("tid"),
                            "tournament_name": row.get("tournament_name"),
                            "start_date": row.get("start_date"),
                            "team_name": row.get("team_name"),
                            "raw_team": row.get("raw_team"),
                            "player_name": row.get(f"player_{slot}"),
                            "slot": slot,
                            "deck": row.get(f"deck_{slot}"),
                        }
                    )
            pd.DataFrame(missing_deck_rows, columns=unmapped_decks_cols).to_csv(
                os.path.join(out_dir, "unmapped_decks.csv"),
                index=False,
                encoding="utf-8",
            )

            player_df = build_player_matchups(player_matches_df)
            deck_stats_df = compute_deck_stats_from_matches(player_matches_df)

            team_df.to_csv(os.path.join(out_dir, "standings.csv"), index=False, encoding="utf-8")
            team_df.to_csv(os.path.join(out_dir, "team_standings.csv"), index=False, encoding="utf-8")
            tables_df.to_csv(os.path.join(out_dir, "tables.csv"), index=False, encoding="utf-8")
            player_matches_df.to_csv(os.path.join(out_dir, "matches.csv"), index=False, encoding="utf-8")
            team_matches_df.to_csv(os.path.join(out_dir, "team_matches.csv"), index=False, encoding="utf-8")
            player_event_scores_df.to_csv(os.path.join(out_dir, "event_summary.csv"), index=False, encoding="utf-8")
            player_event_scores_df.to_csv(
                os.path.join(out_dir, "player_event_summary.csv"), index=False, encoding="utf-8"
            )
            team_event_scores_df.to_csv(
                os.path.join(out_dir, "team_event_summary.csv"), index=False, encoding="utf-8"
            )
            player_df.to_csv(os.path.join(out_dir, "player.csv"), index=False, encoding="utf-8")
            monthly_best.to_csv(os.path.join(out_dir, "monthly_best.csv"), index=False, encoding="utf-8")
            league.to_csv(os.path.join(out_dir, "league_table.csv"), index=False, encoding="utf-8")
            league.to_csv(os.path.join(out_dir, "player_league_table.csv"), index=False, encoding="utf-8")
            team_league.to_csv(os.path.join(out_dir, "team_league_table.csv"), index=False, encoding="utf-8")
            team_map_df.to_csv(os.path.join(out_dir, "team_player_map.csv"), index=False, encoding="utf-8")
            deck_stats_df.to_csv(os.path.join(out_dir, "deck_stats.csv"), index=False, encoding="utf-8")

            site = {
                **site_meta,
                "league_format": "2x2",
                "rounds_per_event": 3,
                "standings": team_df.to_dict(orient="records"),
                "tables": tables_df.to_dict(orient="records"),
                "matches": player_matches_df.to_dict(orient="records"),
                "team_matches": team_matches_df.to_dict(orient="records"),
                "event_scores": player_event_scores_df.to_dict(orient="records"),
                "team_event_scores": team_event_scores_df.to_dict(orient="records"),
                "monthly_best": monthly_best.to_dict(orient="records"),
                "league_table": league.to_dict(orient="records"),
                "player_league_table": league.to_dict(orient="records"),
                "team_league_table": team_league.to_dict(orient="records"),
                "team_player_map": team_map_df.to_dict(orient="records"),
                "deck_stats": deck_stats_df.to_dict(orient="records"),
                "player_aliases": aliases,
            }
            with open(os.path.join(out_dir, "site.json"), "w", encoding="utf-8") as f:
                json.dump(clean_nan(site), f, ensure_ascii=False, allow_nan=False)

            print(
                f"\nOK ({league_type})! Gerado 2x2: standings.csv, team_matches.csv, matches.csv, "
                f"player_event_summary.csv, team_event_summary.csv, league_table.csv, "
                f"team_league_table.csv, deck_stats.csv, site.json em {out_dir}"
            )
            if not unmapped_teams_df.empty:
                print(f"Aviso 2x2: {len(unmapped_teams_df)} dupla(s) precisam revisao em {out_dir}/unmapped_teams.csv")
            if failed_tids:
                print(f"Aviso: {len(failed_tids)} evento(s) falharam nesta liga: {', '.join(failed_tids)}")
            summaries.append({"league": league_type, "events": len(event_tids), "out_dir": out_dir})
            continue

        if not matches_df.empty:
            def fill_deck_url(row):
                tid_val = row.get("tid")
                if pd.notna(tid_val):
                    tid_val = str(tid_val)

                pid_val = row.get("player_id")
                if pd.notna(pid_val):
                    pid_val = str(pid_val)

                if tid_val and pid_val:
                    hit = deck_by_pid.get((tid_val, pid_val))
                    if hit:
                        return hit

                name_key = normalize_player_name(row.get("player_name"))
                if tid_val and name_key:
                    hit = deck_by_name.get((tid_val, name_key))
                    if hit:
                        return hit

                if isinstance(row.get("deck_url"), str) and row["deck_url"]:
                    return row["deck_url"]

                return None

            matches_df["deck_url"] = matches_df.apply(fill_deck_url, axis=1)
            matches_df["deck_url"] = matches_df["deck_url"].map(canonicalize_url)

        if deck_map and not matches_df.empty:
            def lookup(u, key):
                if not isinstance(u, str):
                    return None
                info = deck_map.get(u)
                return info.get(key) if info else None

            matches_df["deck_name_pt"] = matches_df["deck_url"].map(lambda u: lookup(u, "deck_name_pt"))
            matches_df["deck_name_en"] = matches_df["deck_url"].map(lambda u: lookup(u, "deck_name_en"))
            matches_df["colecao"] = matches_df["deck_url"].map(lambda u: lookup(u, "colecao"))

        if not matches_df.empty:
            total_matches = len(matches_df)
            filled = matches_df["deck_url"].notna().sum()
            print(f"Matches com deck_url preenchido: {filled} / {total_matches}")

            examples = matches_df["deck_url"].dropna().unique()[:5]
            print(f"Exemplos deck_url: {', '.join(examples) if len(examples) else '-'}")

            mapped = matches_df["deck_name_pt"].notna().sum() if "deck_name_pt" in matches_df.columns else 0
            print(f"Matches com deck mapeado: {mapped} / {total_matches}")

            deck_map_keys = set(deck_map.keys()) if deck_map else set()
            unmapped_cols = ["tid", "tournament_name", "start_date", "player_name", "deck_url"]

            deck_url_series = matches_df["deck_url"].astype("string").str.strip()
            missing_url_mask = deck_url_series.isna() | deck_url_series.eq("")
            unmapped_url_mask = (~missing_url_mask) & (~deck_url_series.isin(deck_map_keys))

            unmapped_df = matches_df[missing_url_mask | unmapped_url_mask].copy()
            if not unmapped_df.empty:
                unmapped_df = (
                    unmapped_df.reindex(columns=unmapped_cols)
                    .drop_duplicates()
                    .sort_values(["tid", "player_name", "deck_url"])
                )
            else:
                unmapped_df = pd.DataFrame(columns=unmapped_cols)
            unmapped_df.to_csv(os.path.join(out_dir, "unmapped_decks.csv"), index=False, encoding="utf-8")
            print(
                f"Decks com problema de cadastro: {len(unmapped_df)} "
                f"(sem URL: {int(missing_url_mask.sum())} | URL sem mapping: {int(unmapped_url_mask.sum())}) "
                f"(salvo em {out_dir}/unmapped_decks.csv)"
            )
        else:
            pd.DataFrame(
                columns=["tid", "tournament_name", "start_date", "player_name", "deck_url"]
            ).to_csv(os.path.join(out_dir, "unmapped_decks.csv"), index=False, encoding="utf-8")
            print(f"Matches vazios: {out_dir}/unmapped_decks.csv gerado vazio.")

        event_scores_df = compute_event_scores_from_matches(matches_df)
        event_scores_df = apply_official_event_rank(event_scores_df, df, matches_df=matches_df)
        event_scores_df.to_csv(os.path.join(out_dir, "event_summary.csv"), index=False, encoding="utf-8")
        player_df = build_player_matchups(matches_df)
        player_df.to_csv(os.path.join(out_dir, "player.csv"), index=False, encoding="utf-8")

        if not df.empty and not matches_df.empty:
            name_map = matches_df.dropna(subset=["tid", "player_id", "player_name"]).copy()
            name_map["tid_key"] = name_map["tid"].astype(str)
            name_map["pid_key"] = name_map["player_id"].astype(str)
            name_map = name_map.drop_duplicates(subset=["tid_key", "pid_key"])

            df["tid_key"] = df["tid"].astype(str)
            df["pid_key"] = df["player_id"].astype(str)
            df = df.merge(
                name_map[["tid_key", "pid_key", "player_name"]].rename(
                    columns={"player_name": "player_name_match"}
                ),
                on=["tid_key", "pid_key"],
                how="left",
            )
            df["player_name"] = df["player_name"].fillna(df["player_name_match"])
            df = df.drop(columns=["player_name_match", "tid_key", "pid_key"], errors="ignore")

        if not df.empty and not event_scores_df.empty:
            df["tid"] = df["tid"].astype("string")
            df["player_name_norm"] = df["player_name"].map(normalize_player_name)
            event_scores_merge = event_scores_df[["tid", "player_name", "opp_win_rate"]].copy()
            event_scores_merge["tid"] = event_scores_merge["tid"].astype("string")
            event_scores_merge["player_name_norm"] = event_scores_merge["player_name"].map(normalize_player_name)
            event_scores_merge = event_scores_merge.rename(columns={"opp_win_rate": "opp_win_rate_match"})
            df = df.merge(
                event_scores_merge[["tid", "player_name_norm", "opp_win_rate_match"]],
                on=["tid", "player_name_norm"],
                how="left",
            )
            if "opp_win_rate" in df.columns:
                df["opp_win_rate"] = df["opp_win_rate"].fillna(df["opp_win_rate_match"])
            df = df.drop(columns=["opp_win_rate_match", "player_name_norm"], errors="ignore")

        if not df.empty:
            df = apply_official_rank_to_standings(df, matches_df=matches_df)

        df.to_csv(os.path.join(out_dir, "standings.csv"), index=False, encoding="utf-8")
        tables_df.to_csv(os.path.join(out_dir, "tables.csv"), index=False, encoding="utf-8")
        matches_df.to_csv(os.path.join(out_dir, "matches.csv"), index=False, encoding="utf-8")

        df2 = event_scores_df.dropna(subset=["month", "player_name"]).copy()
        df2["event_points"] = pd.to_numeric(df2.get("event_points"), errors="coerce").fillna(0)
        df2["points_match"] = pd.to_numeric(df2.get("points_match"), errors="coerce").fillna(0)
        df2["win_rate"] = pd.to_numeric(df2.get("win_rate"), errors="coerce").fillna(0.0)
        df2["opp_win_rate"] = pd.to_numeric(df2.get("opp_win_rate"), errors="coerce").fillna(0.0)
        df2["start_date_val"] = pd.to_datetime(df2.get("start_date"), errors="coerce")

        df2 = df2.sort_values(
            by=[
                "month",
                "player_name",
                "event_points",
                "points_match",
                "win_rate",
                "opp_win_rate",
                "start_date_val",
            ],
            ascending=[True, True, False, False, False, False, False],
        )

        monthly_best = df2.groupby(["month", "player_name"], as_index=False).head(1)
        if "event_points" not in monthly_best.columns:
            monthly_best["event_points"] = 0
        monthly_best = monthly_best.drop(columns=["start_date_val"], errors="ignore")
        monthly_best.to_csv(os.path.join(out_dir, "monthly_best.csv"), index=False, encoding="utf-8")

        league = compute_quarterly_points(
            df2,
            discard_worst_results=(league_type in {"online", "presencial"}),
            preserve_best_month=(league_type != "presencial"),
            include_absences_as_zero=(league_type in {"online", "presencial"}),
            discard_count=discard_count_for_league(league_type),
        )

        if not df2.empty:
            event_counts = df2.groupby("player_name", as_index=False)["tid"].nunique().rename(
                columns={"tid": "eventos"}
            )
            league = league.merge(event_counts, on="player_name", how="left")
        else:
            league["eventos"] = 0

        league = league.sort_values(
            ["league_points", "eventos", "player_name"],
            ascending=[False, False, True],
        )

        deck_principal = {}
        if not matches_df.empty:
            if "deck_name_pt" in matches_df.columns:
                matches_df["deck_key"] = matches_df["deck_name_pt"].fillna(matches_df["deck_url"])
            else:
                matches_df["deck_key"] = matches_df["deck_url"]

            matches_df["deck_key"] = matches_df["deck_key"].astype("string").str.strip()
            matches_df["deck_key"] = matches_df["deck_key"].mask(matches_df["deck_key"].eq(""))
            matches_df["deck_key"] = matches_df["deck_key"].fillna(matches_df["deck_url"])

            tmp = matches_df.dropna(subset=["player_name", "deck_key"]).copy()
            tmp = tmp[~tmp["deck_key"].map(is_placeholder_deck)]
            if not tmp.empty:
                counts = (
                    tmp.groupby(["player_name", "deck_key"])
                    .size()
                    .reset_index(name="games")
                    .sort_values(["player_name", "games", "deck_key"], ascending=[True, False, True])
                )
                deck_principal = (
                    counts.groupby("player_name").head(1).set_index("player_name")["deck_key"].to_dict()
                )

        league["deck_principal"] = league["player_name"].map(lambda n: deck_principal.get(n))
        league.to_csv(os.path.join(out_dir, "league_table.csv"), index=False, encoding="utf-8")

        deck_stats_cols = [
            "deck_key",
            "partidas_jogadas",
            "vitorias",
            "empates",
            "jogadores_unicos",
            "eventos",
            "derrotas",
            "win_rate",
            "deck_name_pt",
            "deck_name_en",
            "colecao",
            "deck_url",
        ]
        deck_stats_df = pd.DataFrame(columns=deck_stats_cols)
        if not matches_df.empty:
            if "deck_key" not in matches_df.columns:
                matches_df["deck_key"] = matches_df.get("deck_name_pt", pd.Series([None] * len(matches_df))).fillna(
                    matches_df["deck_url"]
                )

            played = matches_df.dropna(subset=["deck_key", "player_name"]).copy()
            played = played[~played["deck_key"].map(is_placeholder_deck)].copy()

            status_lower = played.get("status", pd.Series([""] * len(played))).astype(str).str.lower()
            played["is_draw"] = (
                status_lower.str.contains("draw", na=False)
                | status_lower.str.contains("empate", na=False)
                | status_lower.str.contains(r"\bid\b", regex=True, na=False)
            ).astype(int)

            played["wins"] = played["is_winner"].fillna(0).astype(int)
            played["draws"] = played["is_draw"].fillna(0).astype(int)

            agg = played.groupby("deck_key").agg(
                partidas_jogadas=("deck_key", "size"),
                vitorias=("wins", "sum"),
                empates=("draws", "sum"),
                jogadores_unicos=("player_name", "nunique"),
                eventos=("tid", "nunique"),
            ).reset_index()

            agg["derrotas"] = agg["partidas_jogadas"] - agg["vitorias"] - agg["empates"]
            agg["win_rate"] = (agg["vitorias"] / agg["partidas_jogadas"]).fillna(0)

            meta_cols = ["deck_name_pt", "deck_name_en", "colecao", "deck_url"]
            for c in meta_cols:
                if c not in played.columns:
                    played[c] = None

            meta = played.groupby("deck_key").agg(
                deck_name_pt=("deck_name_pt", lambda x: next((v for v in x if pd.notna(v)), None)),
                deck_name_en=("deck_name_en", lambda x: next((v for v in x if pd.notna(v)), None)),
                colecao=("colecao", lambda x: next((v for v in x if pd.notna(v)), None)),
                deck_url=("deck_url", lambda x: next((v for v in x if isinstance(v, str)), None)),
            ).reset_index()

            deck_stats_df = agg.merge(meta, on="deck_key", how="left").sort_values(
                "partidas_jogadas", ascending=False
            )
            deck_stats_df = deck_stats_df.reindex(columns=deck_stats_cols)

        deck_stats_df.to_csv(os.path.join(out_dir, "deck_stats.csv"), index=False, encoding="utf-8")

        site = {
            **site_meta,
            "standings": df.to_dict(orient="records"),
            "tables": tables_df.to_dict(orient="records"),
            "matches": matches_df.to_dict(orient="records"),
            "event_scores": event_scores_df.to_dict(orient="records"),
            "monthly_best": monthly_best.to_dict(orient="records"),
            "league_table": league.to_dict(orient="records"),
            "deck_stats": deck_stats_df.to_dict(orient="records"),
            "player_aliases": aliases,
        }
        with open(os.path.join(out_dir, "site.json"), "w", encoding="utf-8") as f:
            json.dump(clean_nan(site), f, ensure_ascii=False, allow_nan=False)

        print(
            f"\nOK ({league_type})! Gerado: standings.csv, tables.csv, matches.csv, event_summary.csv, "
            f"player.csv, monthly_best.csv, league_table.csv, deck_stats.csv, site.json em {out_dir}"
        )
        if failed_tids:
            print(f"Aviso: {len(failed_tids)} evento(s) falharam nesta liga: {', '.join(failed_tids)}")
        summaries.append({"league": league_type, "events": len(event_tids), "out_dir": out_dir})

    print("\nConcluido! Conjuntos gerados:")
    for summary in summaries:
        print(f"- {summary['league']}: {summary['events']} evento(s) -> {summary['out_dir']}")

if __name__ == "__main__":
    main()
