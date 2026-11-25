import json
import os
import re
import time
import unicodedata
from datetime import datetime, timezone
from difflib import get_close_matches
from email.utils import parsedate_to_datetime
from urllib.parse import unquote, urlparse, urlunparse

import pandas as pd
import requests

API_BASE = "https://topdeck.gg/api"
API_KEY = os.getenv("TOPDECK_API_KEY")  # NAO colocar key no arquivo
LEAGUE_ORDER = ["presencial", "online", "presencial2x2", "online2x2"]
TWO_BY_TWO_LEAGUES = {"presencial2x2", "online2x2"}
LEAGUE_ALIASES = {
    "presencial-2x2": "presencial2x2",
    "online-2x2": "online2x2",
    "2x2-presencial": "presencial2x2",
    "2x2-online": "online2x2",
}
DEFAULT_LEAGUE = "presencial"
TEAM_MAP_2X2_PATH = "Team_map_2x2.csv"
DECK_MAP_PATH = "Deck_map.csv"
DECK_ALIASES_PATH = "deck_aliases.json"
TOPDECK_MAX_RETRIES = 5
TOPDECK_BASE_BACKOFF_SECONDS = 1.5
TOPDECK_MAX_BACKOFF_SECONDS = 45.0
TOPDECK_MIN_INTERVAL_SECONDS = 0.5
_LAST_TOPDECK_REQUEST_MONO = 0.0
_DECK_ALIASES_CACHE = None
_DECK_NAME_INDEX_CACHE: dict[int, dict] = {}

PLACEHOLDER_DECKS = {"sem deck", "sem decks", "s deck", "no deck", "no decks", "deck missing", "missing deck"}
SCORE_SORT_COLS = ["event_points", "points_match", "win_rate", "opp_win_rate", "start_date_val"]
STANDINGS_COLS = [
    "tid", "tournament_name", "start_date", "month", "player_name", "player_id",
    "standing", "points", "win_rate", "opp_win_rate", "decklist", "commanders",
]
TABLES_COLS = ["tid", "tournament_name", "start_date", "round", "table", "status"]
MATCHES_COLS = [
    "tid", "tournament_name", "start_date", "month", "round", "table", "status",
    "winner_id", "winner_name", "player_id", "player_name", "is_winner", "deck_url", "deck_raw",
]
EVENT_SCORE_COLS = [
    "tid", "player_name", "tournament_name", "start_date", "month", "matches", "wins", "draws",
    "losses", "points_match", "win_rate", "opp_win_rate", "event_rank", "event_points", "event_players",
]
DECK_STATS_COLS = [
    "deck_key", "partidas_jogadas", "vitorias", "empates", "jogadores_unicos", "eventos", "derrotas",
    "win_rate", "is_banned", "ban_rule", "deck_name_pt", "deck_name_en", "colecao", "deck_url",
]
TEAM_MAP_2X2_COLUMNS = [
    "tid", "raw_team", "team_name", "player_1", "player_2", "deck_1", "deck_2", "deck_url_1", "deck_url_2", "notes",
]
TEAM_INFO_COLS = [
    "raw_team", "team_name", "team_key", "player_1", "player_2", "deck_1", "deck_2", "deck_url_1", "deck_url_2",
    "deck_name_pt_1", "deck_name_pt_2", "deck_name_en_1", "deck_name_en_2", "colecao_1", "colecao_2",
    "map_status", "missing_players", "missing_decks",
]
PLAYER_MATCHUP_COLS = [
    "tid", "tournament_name", "start_date", "event_day", "month", "round", "table", "player_id",
    "player_name", "player_deck", "opponent_id", "opponent_name", "opponent_deck", "result",
]
OFFICIAL_COLS = [
    "tid", "player_name_norm", "event_rank_official", "event_points_official",
    "event_players_official", "points_official",
]


# ---------------------------------------------------------------------------
# Regras da liga
# ---------------------------------------------------------------------------

def discard_count_for_league(league_type: str) -> int:
    return 2 if league_type in {"online", "online2x2"} else 3


def points_for_event_position(rank: int, player_count: int) -> int:
    """Calcula pontuacao por colocacao do evento (regras 2026)."""
    if rank == 1:
        return 3
    for limit, top in [(12, 4), (24, 8), (36, 12)]:
        if player_count <= limit:
            return 2 if rank <= top else 1
    return 2 if rank <= 16 else 1


def ban_rule_for_league(league_type: str | None) -> dict[str, float | int | str]:
    min_matches = 12 if league_type in {"online", "online2x2"} else 24
    if league_type in TWO_BY_TWO_LEAGUES:
        return {"min_matches": min_matches, "min_win_rate": 0.4875, "label": "48,75%"}
    return {"min_matches": min_matches, "min_win_rate": 1 / 3, "label": "33,33%"}


def annotate_deck_stats_with_ban_rule(deck_stats_df: pd.DataFrame, league_type: str | None = None) -> pd.DataFrame:
    if deck_stats_df.empty:
        return deck_stats_df

    out = deck_stats_df.copy()
    rule = ban_rule_for_league(league_type)
    partidas = pd.to_numeric(out.get("partidas_jogadas"), errors="coerce").fillna(0)
    win_rate = pd.to_numeric(out.get("win_rate"), errors="coerce").fillna(0)
    out["is_banned"] = (partidas >= int(rule["min_matches"])) & (win_rate >= float(rule["min_win_rate"]))
    out["ban_rule"] = f"{int(rule['min_matches'])}+ jogos e {rule['label']}+ WR"
    return out


# ---------------------------------------------------------------------------
# TopDeck API
# ---------------------------------------------------------------------------

def parse_retry_after_seconds(value: str | None) -> float | None:
    text = str(value).strip() if value else ""
    if not text:
        return None
    try:
        return max(0.0, float(text))
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
    except Exception:
        return None


def topdeck_request(method: str, path: str, json_payload: dict | None = None, timeout: int = 30) -> requests.Response:
    global _LAST_TOPDECK_REQUEST_MONO

    url = f"{API_BASE}{path}" if path.startswith("/") else path
    retry_statuses = {429, 500, 502, 503, 504}

    for attempt in range(1, TOPDECK_MAX_RETRIES + 1):
        wait_gap = TOPDECK_MIN_INTERVAL_SECONDS - (time.monotonic() - _LAST_TOPDECK_REQUEST_MONO)
        if wait_gap > 0:
            time.sleep(wait_gap)
        _LAST_TOPDECK_REQUEST_MONO = time.monotonic()
        backoff = min(TOPDECK_MAX_BACKOFF_SECONDS, TOPDECK_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))

        try:
            resp = requests.request(
                method=method.upper(), url=url, headers={"Authorization": API_KEY}, json=json_payload, timeout=timeout
            )
        except requests.exceptions.RequestException as exc:
            if attempt >= TOPDECK_MAX_RETRIES:
                raise
            print(f"Aviso: erro de rede na TopDeck ({type(exc).__name__}). "
                  f"Retry {attempt}/{TOPDECK_MAX_RETRIES} em {backoff:.1f}s.")
            time.sleep(backoff)
            continue

        if resp.status_code in retry_statuses and attempt < TOPDECK_MAX_RETRIES:
            retry_after = parse_retry_after_seconds(resp.headers.get("Retry-After"))
            delay = retry_after if retry_after is not None else backoff
            print(f"Aviso: TopDeck retornou HTTP {resp.status_code} para {path}. "
                  f"Retry {attempt}/{TOPDECK_MAX_RETRIES} em {delay:.1f}s.")
            time.sleep(delay)
            continue

        resp.raise_for_status()
        return resp

    raise RuntimeError(f"Falha ao consultar TopDeck: {path}")


def fetch_tournament_query(tid: str) -> dict:
    """POST /api/v2/tournaments pedindo rounds+tables+players com decklist."""
    payload = {
        "columns": ["decklist", "wins", "draws", "losses"],
        "rounds": True,
        "tables": ["table", "players", "winner"],
        "players": ["name", "id", "decklist"],
        "TID": [tid],
        "game": "Magic: The Gathering",
        "format": "EDH",
    }
    data = topdeck_request("POST", "/v2/tournaments", json_payload=payload).json()
    if isinstance(data, list):
        if not data:
            raise RuntimeError(f"POST /tournaments retornou lista vazia para: {tid}")
        data = data[0]

    standings = data.get("standings") if isinstance(data, dict) else None
    sample = standings[0] if isinstance(standings, list) and standings else None
    has_full_standings = (
        isinstance(sample, dict)
        and ("name" in sample or "id" in sample)
        and ("opponentWinRate" in sample or "opponentSuccessRate" in sample)
    )
    if not has_full_standings:
        try:
            details = topdeck_request("GET", f"/v2/tournaments/{tid}/").json()
            if isinstance(details, dict):
                info = details.get("data") if isinstance(details.get("data"), dict) else None
                if isinstance(details.get("standings"), list) and details["standings"]:
                    data["standings"] = details["standings"]
                if info is not None:
                    data["data"] = info
                if not data.get("tournamentName"):
                    data["tournamentName"] = info.get("name") if info else None
                if not data.get("startDate"):
                    data["startDate"] = info.get("startDate") if info else None
        except Exception as exc:
            print(f"Aviso: falha ao buscar standings completos para {tid}: {exc}")

    return data


def fetch_rounds(tid: str) -> list:
    data = topdeck_request("GET", f"/v2/tournaments/{tid}/rounds").json()
    if isinstance(data, dict) and "rounds" in data:
        return data["rounds"]
    return data if isinstance(data, list) else []


def load_event_data(tid: str, raw_path: str, offline_mode: bool) -> dict | None:
    """Baixa o evento (ou usa o RAW local). Retorna None quando o evento deve ser pulado."""
    def read_raw():
        with open(raw_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def write_raw(payload):
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    if offline_mode:
        if not os.path.exists(raw_path):
            print(f"Erro: RAW local ausente para {tid}. Pulando evento.")
            return None
        data = read_raw()
        print(f"Modo offline: RAW local carregado para {tid}.")
    else:
        try:
            data = fetch_tournament_query(tid)
            write_raw(data)
        except Exception as exc:
            if not os.path.exists(raw_path):
                print(f"Erro: falha ao baixar {tid}: {exc}. Pulando evento.")
                write_raw({"tid": tid, "error": str(exc), "fetched": False})
                return None
            print(f"Aviso: falha ao baixar {tid}: {exc}. Usando raw local.")
            data = read_raw()

    if isinstance(data, dict) and data.get("fetched") is False and data.get("error"):
        print(f"Erro: RAW local de erro para {tid}: {data.get('error')}. Pulando evento.")
        return None
    return data


# ---------------------------------------------------------------------------
# Normalizacao de texto, nomes, URLs e decks
# ---------------------------------------------------------------------------

def _utc_from_ts(ts: int | None) -> datetime | None:
    if ts is None:
        return None
    ts_int = int(ts)
    if ts_int > 10**12:  # se vier em ms
        ts_int //= 1000
    return datetime.fromtimestamp(ts_int, tz=timezone.utc)


def unix_to_date(ts: int | None) -> str | None:
    dt = _utc_from_ts(ts)
    return dt.strftime("%Y-%m-%d") if dt else None


def month_key(ts: int | None) -> str | None:
    dt = _utc_from_ts(ts)
    return dt.strftime("%Y-%m") if dt else None


def quarter_key_from_month(month: str | None) -> str | None:
    try:
        year_str, month_str = month.split("-", 1)
        year, m = int(year_str), int(month_str)
    except Exception:
        return None
    if not 1 <= m <= 12:
        return None
    return f"{year}-Q{(m - 1) // 3 + 1}"


def clean_player_name(name: object) -> str | None:
    if name is None:
        return None
    try:
        if pd.isna(name):
            return None
    except (TypeError, ValueError):
        pass

    text = re.sub(r"\s+", " ", str(name)).strip()
    if text and not any(ch.isalnum() for ch in text):
        return None
    return text or None


def normalize_player_name(name: object) -> str | None:
    text = clean_player_name(name)
    return text.lower() if text else None


def normalize_lookup_key(value: object) -> str | None:
    text = clean_player_name(value)
    if not text:
        return None
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip() or None


normalize_team_key = normalize_lookup_key


def is_placeholder_deck(value: object) -> bool:
    return normalize_lookup_key(value) in PLACEHOLDER_DECKS


def first_notna(values):
    return next((v for v in values if pd.notna(v)), None)


def _load_json_dict(path: str, label: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8-sig") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path}: esperado um objeto JSON com aliases de {label}.")
    return raw


def load_player_aliases(path: str = "player_aliases.json") -> dict[str, str]:
    aliases = {}
    for raw_name, alias_name in _load_json_dict(path, "jogadores").items():
        key, value = clean_player_name(raw_name), clean_player_name(alias_name)
        if key and value:
            aliases[key] = value
    return aliases


def load_deck_aliases(path: str = DECK_ALIASES_PATH) -> dict[str, str]:
    global _DECK_ALIASES_CACHE
    if _DECK_ALIASES_CACHE is None:
        _DECK_ALIASES_CACHE = {}
        for raw_name, canonical_name in _load_json_dict(path, "decks").items():
            key, value = normalize_lookup_key(raw_name), clean_player_name(canonical_name)
            if key and value:
                _DECK_ALIASES_CACHE[key] = value
    return _DECK_ALIASES_CACHE


def resolve_player_alias(name: object, aliases: dict[str, str]) -> str | None:
    clean_name = clean_player_name(name)
    if not clean_name:
        return None
    exact = aliases.get(clean_name)
    if exact:
        return exact
    norm_name = clean_name.lower()
    for alias_name, canonical_name in aliases.items():
        if normalize_player_name(alias_name) == norm_name:
            return canonical_name
    return clean_name


def normalize_league(value: str | None) -> str:
    if value is None:
        return DEFAULT_LEAGUE
    league = str(value).strip().lower()
    league = LEAGUE_ALIASES.get(league, league)
    return league if league in LEAGUE_ORDER else DEFAULT_LEAGUE


def fix_moxfield(url: str | None) -> str | None:
    if url and "moxfield.com/decks/" in url and not url.startswith("https://moxfield.com/decks/"):
        m = re.search(r"q=(https://moxfield\.com/decks/[^&]+)", url)
        if m:
            return m.group(1)
    return url or None


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

    p = urlparse(u.replace("http://", "https://"))
    netloc = p.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return urlunparse(p._replace(scheme="https", netloc=netloc, query="", fragment="")).rstrip("/")


def load_deck_map_csv(path: str = DECK_MAP_PATH) -> dict:
    """Le Deck_map.csv (#,Coleção,Nome PT-BR,Nome ENG,Link) -> {link_normalizado: infos}."""
    if not os.path.exists(path):
        return {}
    dfm = pd.read_csv(path, encoding="utf-8")
    if dfm.empty:
        return {}
    for c in ["Link", "Nome PT-BR", "Nome ENG", "Coleção"]:
        if c not in dfm.columns:
            raise SystemExit(f"{path}: coluna obrigatoria faltando: {c}")

    def text(value):
        return None if pd.isna(value) else str(value)

    mapping = {}
    for _, r in dfm.iterrows():
        link_norm = None if pd.isna(r.get("Link")) else canonicalize_url(str(r.get("Link")))
        if link_norm:
            mapping[link_norm] = {
                "deck_name_pt": text(r.get("Nome PT-BR")),
                "deck_name_en": text(r.get("Nome ENG")),
                "colecao": text(r.get("Coleção")),
            }
    return mapping


def _deck_name_index(deck_map: dict) -> dict:
    """Indice nome_normalizado -> info do deck (cacheado por deck_map)."""
    cache = _DECK_NAME_INDEX_CACHE.get(id(deck_map))
    if cache is not None and cache[0] is deck_map and cache[1] == len(deck_map):
        return cache[2]
    by_name = {}
    for url, info in deck_map.items():
        for value in [info.get("deck_name_pt"), info.get("deck_name_en")]:
            value_key = normalize_lookup_key(value)
            if value_key:
                by_name[value_key] = {**info, "deck_url": url}
    _DECK_NAME_INDEX_CACHE[id(deck_map)] = (deck_map, len(deck_map), by_name)
    return by_name


def find_deck_by_name(deck_label: object, deck_map: dict) -> dict | None:
    key = normalize_lookup_key(deck_label)
    if not key or not deck_map:
        return None

    by_name = _deck_name_index(deck_map)
    if key in by_name:
        return by_name[key]
    alias_key = normalize_lookup_key(load_deck_aliases().get(key))
    if alias_key and alias_key in by_name:
        return by_name[alias_key]
    matches = get_close_matches(key, by_name.keys(), n=1, cutoff=0.92)
    return by_name[matches[0]] if matches else None


def resolve_manual_deck(deck_label: object, deck_url: object, deck_map: dict) -> dict:
    url = canonicalize_url(deck_url)
    if is_placeholder_deck(deck_label) and not url:
        return {"deck_key": None, "deck_name_pt": None, "deck_name_en": None, "colecao": None, "deck_url": None}

    label = clean_player_name(deck_label)
    info = deck_map.get(url) if url and deck_map else None
    if not info and label and deck_map:
        info = find_deck_by_name(label, deck_map)
        if info:
            url = info.get("deck_url") or url

    deck_name_pt = (info.get("deck_name_pt") or label) if info else label
    return {
        "deck_key": clean_player_name(deck_name_pt) or url,
        "deck_name_pt": deck_name_pt,
        "deck_name_en": info.get("deck_name_en") if info else None,
        "colecao": info.get("colecao") if info else None,
        "deck_url": url,
    }


def _imported_from(obj) -> str | None:
    meta = obj.get("metadata") if isinstance(obj, dict) else None
    return meta.get("importedFrom") if isinstance(meta, dict) else None


def extract_decklist(obj: dict) -> str | None:
    if not isinstance(obj, dict):
        return None
    value = obj.get("decklist") or _imported_from(obj) or _imported_from(obj.get("deckObj"))
    return fix_moxfield(value) if value else None


def extract_deck_url(obj: dict) -> str | None:
    if not isinstance(obj, dict):
        return None
    imported = _imported_from(obj.get("deckObj")) or _imported_from(obj)
    if imported:
        return fix_moxfield(imported)
    dl = obj.get("decklist")
    if isinstance(dl, str) and re.match(r"^https?://", dl.strip()):
        return fix_moxfield(dl.strip())
    return None


def extract_commanders(standing: dict) -> str | None:
    deckobj = standing.get("deckObj")
    cmds = deckobj.get("Commanders") if isinstance(deckobj, dict) else None
    return " / ".join(sorted(cmds.keys())) if isinstance(cmds, dict) and cmds else None


def normalize_rounds(raw_rounds: list) -> list:
    """Se 'round' vier zoado, renumera por ordem (1..N)."""
    if not raw_rounds:
        return []
    round_vals = [r.get("round") for r in raw_rounds]
    if all(isinstance(x, int) for x in round_vals) and sorted(set(round_vals)) == list(range(1, len(raw_rounds) + 1)):
        return sorted(raw_rounds, key=lambda r: r.get("round", 10**9))
    return [{**r, "round_norm": idx} for idx, r in enumerate(raw_rounds, start=1)]


# ---------------------------------------------------------------------------
# Duplas 2x2
# ---------------------------------------------------------------------------

def two_by_two_player_pair_key(player_1: object, player_2: object) -> str | None:
    parts = sorted(key for key in [normalize_lookup_key(player_1), normalize_lookup_key(player_2)] if key)
    return " & ".join(parts) if len(parts) == 2 else None


def canonical_two_by_two_team_name(player_1: object, player_2: object) -> str | None:
    players = [p for p in [clean_player_name(player_1), clean_player_name(player_2)] if p]
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

    team_part, deck_part = text, None
    if "//" in text:
        team_part, deck_part = re.split(r"\s*//\s*", text, maxsplit=1)
    else:
        # Alguns resultados do TopDeck usam "Jogador A & Jogador B - Deck A + Deck B".
        # So interpreta o hifen como separador quando a parte anterior ja forma uma dupla.
        dashed_parts = re.split(r"\s+-\s+", text, maxsplit=1)
        if len(dashed_parts) == 2 and len(split_two_by_two_values(dashed_parts[0])) >= 2:
            team_part, deck_part = dashed_parts

    return {
        "team_name": clean_player_name(team_part),
        "players": split_two_by_two_values(team_part)[:2],
        "decks": split_two_by_two_values(deck_part)[:2] if deck_part else [],
    }


def load_team_map_2x2(path: str = TEAM_MAP_2X2_PATH) -> dict[tuple[str, str], dict]:
    if not os.path.exists(path):
        return {}
    df = pd.read_csv(path, encoding="utf-8-sig").reindex(columns=TEAM_MAP_2X2_COLUMNS)

    mapping = {}
    for _, row in df.iterrows():
        item = {col: clean_player_name(row.get(col)) for col in TEAM_MAP_2X2_COLUMNS}
        tid, raw_key = item["tid"], normalize_team_key(item["raw_team"])
        if not tid or not raw_key:
            continue
        parsed = parse_two_by_two_raw_team(item["raw_team"])
        keys = [
            raw_key,
            normalize_team_key(item["team_name"]) or normalize_team_key(parsed["team_name"]),
            two_by_two_player_pair_key(item["player_1"], item["player_2"]),
            two_by_two_player_pair_key(*parsed["players"]) if len(parsed["players"]) >= 2 else None,
        ]
        for key in dict.fromkeys(k for k in keys if k):
            mapping[(tid, key)] = item
    return mapping


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
    raw_key = normalize_team_key(raw_team_text)

    manual = {}
    if tid_text and raw_key:
        candidate_keys = [raw_key, normalize_team_key(parsed["team_name"])]
        if len(parsed["players"]) >= 2:
            candidate_keys.append(two_by_two_player_pair_key(*parsed["players"]))
            candidate_keys.append(
                two_by_two_player_pair_key(*[resolve_player_alias(p, player_aliases) for p in parsed["players"]])
            )
        for key in dict.fromkeys(k for k in candidate_keys if k):
            if team_map.get((tid_text, key)):
                manual = team_map[(tid_text, key)]
                break

    def pick(field: str, fallback: list, idx: int):
        return clean_player_name(manual.get(field)) or (fallback[idx] if len(fallback) > idx else None)

    player_1 = resolve_player_alias(pick("player_1", parsed["players"], 0), player_aliases)
    player_2 = resolve_player_alias(pick("player_2", parsed["players"], 1), player_aliases)
    deck_1 = resolve_manual_deck(pick("deck_1", parsed["decks"], 0), manual.get("deck_url_1"), deck_map)
    deck_2 = resolve_manual_deck(pick("deck_2", parsed["decks"], 1), manual.get("deck_url_2"), deck_map)

    manual_team = clean_player_name(manual.get("team_name"))
    team_key = two_by_two_player_pair_key(player_1, player_2) or normalize_team_key(
        manual_team or parsed["team_name"] or raw_team_text
    )
    team_name = manual_team or canonical_two_by_two_team_name(player_1, player_2) or parsed["team_name"] or raw_team_text

    info = {
        "tid": tid_text,
        "raw_team": raw_team_text,
        "team_name": team_name,
        "team_key": team_key,
        "player_1": player_1,
        "player_2": player_2,
    }
    for field in ["deck_key", "deck_url", "deck_name_pt", "deck_name_en", "colecao"]:
        prefix = "deck" if field == "deck_key" else field
        info[f"{prefix}_1"], info[f"{prefix}_2"] = deck_1.get(field), deck_2.get(field)
    info["map_status"] = "manual" if manual else ("parsed" if player_1 and player_2 else "missing")
    info["missing_players"] = int(not (player_1 and player_2))
    info["missing_decks"] = int(not (deck_1.get("deck_url") and deck_2.get("deck_url")))
    return info


def enrich_two_by_two_team_frame(
    frame: pd.DataFrame,
    team_map: dict[tuple[str, str], dict],
    player_aliases: dict[str, str],
    deck_map: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    empty_map = pd.DataFrame(columns=["tid", *TEAM_INFO_COLS])
    if frame.empty:
        return frame.copy(), empty_map

    out = frame.copy()
    infos = pd.DataFrame(
        [resolve_two_by_two_team_info(r.tid, r.player_name, team_map, player_aliases, deck_map)
         for r in out.itertuples(index=False)]
    )
    for col in TEAM_INFO_COLS:
        out[col] = infos[col].values if col in infos.columns else None

    out = out[out["raw_team"].map(clean_player_name).notna()].copy()
    if out.empty:
        return out, empty_map
    out["player_name"] = out["team_name"]

    team_map_df = (
        out[["tid", "tournament_name", "start_date", "month", *TEAM_INFO_COLS]]
        .drop_duplicates(subset=["tid", "raw_team"])
        .sort_values(["tid", "team_name"], kind="mergesort")
        .reset_index(drop=True)
    )
    return out, team_map_df


def normalize_two_by_two_winner_names(matches_df: pd.DataFrame, team_map_df: pd.DataFrame) -> pd.DataFrame:
    if matches_df.empty or team_map_df.empty or "winner_name" not in matches_df.columns:
        return matches_df

    lookup = {}
    for row in team_map_df.itertuples(index=False):
        for value in [row.raw_team, row.team_name]:
            key = (clean_player_name(row.tid), normalize_team_key(value))
            if key[0] and key[1]:
                lookup[key] = row.team_name

    def resolve(row):
        winner = row.get("winner_name")
        if not isinstance(winner, str) or not winner.strip():
            return winner
        if is_draw_result(row.get("status"), row.get("winner_id"), winner):
            return clean_player_name(winner)
        return lookup.get((clean_player_name(row.get("tid")), normalize_team_key(winner)), clean_player_name(winner))

    out = matches_df.copy()
    out["winner_name"] = out.apply(resolve, axis=1)
    return out


def expand_two_by_two_matches_to_players(team_matches_df: pd.DataFrame) -> pd.DataFrame:
    if team_matches_df.empty:
        return team_matches_df.copy()

    rows = []
    for row in team_matches_df.to_dict(orient="records"):
        for slot, other in [(1, 2), (2, 1)]:
            player_name = clean_player_name(row.get(f"player_{slot}"))
            if not player_name:
                continue
            deck_key = row.get(f"deck_{slot}")
            rows.append({
                **row,
                "team_player_slot": slot,
                "teammate_name": clean_player_name(row.get(f"player_{other}")),
                "player_name": player_name,
                "player_id": f'{row.get("player_id") or row.get("team_key") or row.get("team_name")}#{slot}',
                "deck_key": deck_key,
                "deck_url": row.get(f"deck_url_{slot}"),
                "deck_name_pt": row.get(f"deck_name_pt_{slot}") or deck_key,
                "deck_name_en": row.get(f"deck_name_en_{slot}"),
                "colecao": row.get(f"colecao_{slot}"),
            })
    return pd.DataFrame(rows)


def expand_two_by_two_scores_to_players(team_scores_df: pd.DataFrame, team_map_df: pd.DataFrame) -> pd.DataFrame:
    cols = [*EVENT_SCORE_COLS[:2], "team_name", "team_key", *EVENT_SCORE_COLS[2:], "event_teams"]
    if team_scores_df.empty:
        return pd.DataFrame(columns=cols)

    meta = team_map_df.reindex(columns=["tid", "team_name", "team_key", "player_1", "player_2"]).drop_duplicates(
        subset=["tid", "team_name"]
    )
    merged = team_scores_df.merge(meta, left_on=["tid", "player_name"], right_on=["tid", "team_name"], how="left")

    rows = []
    for row in merged.to_dict(orient="records"):
        for player_col in ["player_1", "player_2"]:
            player_name = clean_player_name(row.get(player_col))
            if player_name:
                rows.append({
                    **{k: row.get(k) for k in team_scores_df.columns},
                    "team_name": row.get("team_name") or row.get("player_name"),
                    "team_key": row.get("team_key"),
                    "player_name": player_name,
                    "event_teams": row.get("event_players"),
                })
    return pd.DataFrame(rows).reindex(columns=cols) if rows else pd.DataFrame(columns=cols)


# ---------------------------------------------------------------------------
# Pontuacao de eventos
# ---------------------------------------------------------------------------

def group_apply_preserve_keys(df: pd.DataFrame, by: str | list[str], func, *, sort: bool = False) -> pd.DataFrame:
    """groupby().apply() que mantem as colunas de agrupamento (compatibilidade com pandas 3)."""
    group_cols = [by] if isinstance(by, str) else list(by)
    parts = []
    for keys, group in df.groupby(group_cols, sort=sort):
        keys = keys if isinstance(keys, tuple) else (keys,)
        result = func(group).copy()
        for col, key in zip(group_cols, keys):
            if col not in result.columns:
                result[col] = key
        parts.append(result)
    return pd.concat(parts, ignore_index=True) if parts else df.iloc[0:0].copy()


def rank_events(df: pd.DataFrame, sort_cols: list[str]) -> pd.DataFrame:
    """Rankeia cada evento por sort_cols (desc), com empates dividindo a mesma colocacao."""
    def assign(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_values(by=sort_cols, ascending=False, kind="mergesort").copy()
        ranks, current_rank, last_key = [], 0, None
        for key in zip(*(group[c] for c in sort_cols)):
            if key != last_key:
                current_rank, last_key = current_rank + 1, key
            ranks.append(current_rank)
        player_count = group["player_name"].nunique()
        group["event_rank"] = ranks
        group["event_points"] = [points_for_event_position(r, player_count) for r in ranks]
        group["event_players"] = player_count
        return group

    return group_apply_preserve_keys(df, "tid", assign)


def compute_event_points(standings_df: pd.DataFrame) -> pd.DataFrame:
    if standings_df.empty:
        return standings_df
    df = standings_df.copy()
    df["player_name"] = df["player_name"].map(clean_player_name)
    df = df.dropna(subset=["player_name"])
    if df.empty:
        return standings_df.iloc[0:0].copy()
    for src, dst in [("points", "points_val"), ("win_rate", "win_rate_val"), ("opp_win_rate", "opp_win_rate_val")]:
        df[dst] = pd.to_numeric(df.get(src), errors="coerce").fillna(-10**9)
    return rank_events(df, ["points_val", "win_rate_val", "opp_win_rate_val"])


def draw_mask(df: pd.DataFrame) -> pd.Series:
    def lower(col):
        series = df[col] if col in df.columns else pd.Series([""] * len(df), index=df.index)
        return series.astype(str).str.lower()

    status = lower("status")
    return (
        lower("winner_id").eq("draw")
        | lower("winner_name").eq("draw")
        | status.str.contains("draw", na=False)
        | status.str.contains("empate", na=False)
        | status.str.contains(r"\bid\b", regex=True, na=False)
    )


def is_draw_result(status: object, winner_id: object = None, winner_name: object = None) -> bool:
    def lower(value):
        return str(value).strip().lower() if pd.notna(value) else ""

    status_text = lower(status)
    return (
        lower(winner_id) == "draw"
        or lower(winner_name) == "draw"
        or "draw" in status_text
        or "empate" in status_text
        or bool(re.search(r"\bid\b", status_text))
    )


def compute_event_scores_from_matches(matches_df: pd.DataFrame) -> pd.DataFrame:
    if matches_df.empty:
        return pd.DataFrame(columns=EVENT_SCORE_COLS)

    df = matches_df.copy()
    df["player_name"] = df["player_name"].map(clean_player_name)
    df = df.dropna(subset=["tid", "player_name"])
    if df.empty:
        return pd.DataFrame(columns=EVENT_SCORE_COLS)

    df["is_winner"] = pd.to_numeric(df.get("is_winner"), errors="coerce").fillna(0).astype(int)
    df["is_draw"] = draw_mask(df).astype(int)
    df["points_match"] = df["is_winner"] * 3 + df["is_draw"]

    agg = df.groupby(["tid", "player_name"], as_index=False).agg(
        tournament_name=("tournament_name", first_notna),
        start_date=("start_date", first_notna),
        month=("month", first_notna),
        matches=("player_name", "size"),
        wins=("is_winner", "sum"),
        draws=("is_draw", "sum"),
        points_match=("points_match", "sum"),
    )
    agg["losses"] = agg["matches"] - agg["wins"] - agg["draws"]
    agg["win_rate"] = agg["wins"] / agg["matches"]

    # opp_win_rate: media do win rate de todos os oponentes de mesa (com repeticao).
    win_rate_map = {(r.tid, r.player_name): r.win_rate for r in agg.itertuples(index=False)}
    opp_sum, opp_count = {}, {}
    for (tid, _, _), tbl in df.groupby(["tid", "round", "table"], dropna=False):
        names = [n for n in tbl["player_name"].tolist() if isinstance(n, str)]
        for name in names:
            for opp in names:
                if opp != name:
                    opp_sum[(tid, name)] = opp_sum.get((tid, name), 0.0) + win_rate_map.get((tid, opp), 0.0)
                    opp_count[(tid, name)] = opp_count.get((tid, name), 0) + 1
    agg["opp_win_rate"] = [
        opp_sum[key] / opp_count[key] if opp_count.get(key) else 0.0 for key in zip(agg["tid"], agg["player_name"])
    ]

    scored = rank_events(agg, ["points_match", "win_rate", "opp_win_rate"])
    return scored.reindex(columns=EVENT_SCORE_COLS, fill_value=None)


def compute_fourth_tiebreak_from_matches(matches_df: pd.DataFrame, standings_df: pd.DataFrame) -> pd.DataFrame:
    """4o criterio de desempate: media do winRate dos oponentes unicos enfrentados no evento."""
    cols = ["tid", "player_name_norm", "opp2_win_rate_val"]
    st = standings_df.dropna(subset=["tid", "player_name"]).copy()
    st["player_name_norm"] = st["player_name"].map(normalize_player_name)
    st["win_rate_val"] = pd.to_numeric(st.get("win_rate"), errors="coerce")
    st = st.dropna(subset=["player_name_norm", "win_rate_val"])
    win_map = {(str(r.tid), r.player_name_norm): float(r.win_rate_val) for r in st.itertuples(index=False)}

    m = matches_df.dropna(subset=["tid", "player_name"]).copy()
    m["tid"] = m["tid"].astype(str)
    m["player_name_norm"] = m["player_name"].map(normalize_player_name)
    m = m.dropna(subset=["player_name_norm"])

    faced = {}
    for (tid, _, _), table_df in m.groupby(["tid", "round", "table"], dropna=False):
        names = list(dict.fromkeys(n for n in table_df["player_name_norm"] if isinstance(n, str) and n.strip()))
        if len(names) < 2:
            continue
        for player_name in names:
            faced.setdefault((tid, player_name), set()).update(n for n in names if n != player_name)

    out = []
    for (tid, player_name_norm), opponents in faced.items():
        rates = [win_map[(tid, opp)] for opp in opponents if (tid, opp) in win_map]
        if rates:
            out.append({"tid": tid, "player_name_norm": player_name_norm, "opp2_win_rate_val": sum(rates) / len(rates)})
    return pd.DataFrame(out, columns=cols)


def apply_tied_first_place_points(df: pd.DataFrame, tol: float = 1e-9) -> pd.DataFrame:
    """Empate tecnico no topo (points + win_rate + opp_win_rate [+ 4o criterio]) -> todos recebem 3 pontos."""
    for _, idx in df.groupby("tid").groups.items():
        group = df.loc[idx]
        group = group[group[["points_val", "win_rate_val", "opp_win_rate_val"]].notna().all(axis=1)]
        if len(group) < 2:
            continue

        top_mask = pd.Series(True, index=group.index)
        for col in ["points_val", "win_rate_val", "opp_win_rate_val"]:
            top_mask &= group[col] >= (group.loc[top_mask, col].max() - tol)
        tied = group[top_mask]
        if len(tied) < 2:
            continue

        fourth = pd.to_numeric(tied["opp2_win_rate_val"], errors="coerce")
        if fourth.notna().any():
            best4 = tied.index[fourth >= (fourth.max() - tol)]
            if len(best4) == 1:
                continue
            tied = tied.loc[best4]

        if len(tied) >= 2:
            df.loc[tied.index, "event_points_official"] = 3
    return df


def build_official_event_rank_df(standings_df: pd.DataFrame, matches_df: pd.DataFrame | None = None) -> pd.DataFrame:
    if standings_df.empty or not {"tid", "player_name", "standing"}.issubset(standings_df.columns):
        return pd.DataFrame(columns=OFFICIAL_COLS)

    df = standings_df.dropna(subset=["tid", "player_name"]).copy()
    df["event_rank_official"] = pd.to_numeric(df["standing"], errors="coerce")
    df["points_official"] = pd.to_numeric(df.get("points"), errors="coerce")
    df["player_name_norm"] = df["player_name"].map(normalize_player_name)
    df = df.dropna(subset=["event_rank_official", "player_name_norm"])
    if df.empty:
        return pd.DataFrame(columns=OFFICIAL_COLS)

    df["event_rank_official"] = df["event_rank_official"].astype(int)
    df["tid"] = df["tid"].astype("string")
    df = df.sort_values(["tid", "event_rank_official", "player_name_norm"], kind="mergesort")
    df = df.drop_duplicates(subset=["tid", "player_name_norm"], keep="first")
    df["event_players_official"] = df.groupby("tid")["player_name_norm"].transform("nunique")
    df["event_points_official"] = [
        points_for_event_position(int(r), int(n)) for r, n in zip(df["event_rank_official"], df["event_players_official"])
    ]
    df["points_val"] = df["points_official"]
    df["win_rate_val"] = pd.to_numeric(df.get("win_rate"), errors="coerce")
    df["opp_win_rate_val"] = pd.to_numeric(df.get("opp_win_rate"), errors="coerce")
    df["opp2_win_rate_val"] = pd.NA
    if matches_df is not None and not matches_df.empty:
        fourth_df = compute_fourth_tiebreak_from_matches(matches_df, standings_df)
        if not fourth_df.empty:
            df = df.drop(columns=["opp2_win_rate_val"]).merge(
                fourth_df.astype({"tid": "string"}), on=["tid", "player_name_norm"], how="left"
            )
    return apply_tied_first_place_points(df).reindex(columns=OFFICIAL_COLS)


def _merge_official(frame: pd.DataFrame, official_df: pd.DataFrame, with_points: bool) -> pd.DataFrame:
    out = frame.copy()
    out["tid"] = out["tid"].astype("string")
    out["player_name_norm"] = out["player_name"].map(normalize_player_name)
    out = out.merge(official_df, on=["tid", "player_name_norm"], how="left")

    has_official = out["event_rank_official"].notna()
    for col in ["event_rank", "event_points", "event_players"]:
        out[col] = pd.to_numeric(out.get(col), errors="coerce")
        out.loc[has_official, col] = out.loc[has_official, f"{col}_official"]
    drop = ["player_name_norm", "event_rank_official", "event_points_official", "event_players_official"]
    if with_points:
        out["points_match"] = pd.to_numeric(out.get("points_match"), errors="coerce")
        has_points = out["points_official"].notna()
        out.loc[has_points, "points_match"] = out.loc[has_points, "points_official"]
        drop.append("points_official")
    return out.drop(columns=drop, errors="ignore")


def apply_official_event_rank(
    event_scores_df: pd.DataFrame, standings_df: pd.DataFrame, matches_df: pd.DataFrame | None = None
) -> pd.DataFrame:
    if event_scores_df.empty:
        return event_scores_df
    official_df = build_official_event_rank_df(standings_df, matches_df=matches_df)
    return event_scores_df if official_df.empty else _merge_official(event_scores_df, official_df, with_points=True)


def apply_official_rank_to_standings(standings_df: pd.DataFrame, matches_df: pd.DataFrame | None = None) -> pd.DataFrame:
    if standings_df.empty:
        return standings_df
    official_df = build_official_event_rank_df(standings_df, matches_df=matches_df)
    if official_df.empty:
        fallback = compute_event_points(standings_df)
        return fallback.drop(columns=["points_val", "win_rate_val", "opp_win_rate_val"], errors="ignore")
    return _merge_official(standings_df, official_df, with_points=False)


# ---------------------------------------------------------------------------
# Tabelas derivadas
# ---------------------------------------------------------------------------

def resolve_deck_label(row: pd.Series) -> str | None:
    for key in ["deck_name_pt", "deck_name_en", "deck_url"]:
        value = row.get(key)
        if pd.notna(value) and str(value).strip():
            return str(value).strip()
    return None


def build_player_matchups(matches_df: pd.DataFrame) -> pd.DataFrame:
    if matches_df.empty:
        return pd.DataFrame(columns=PLAYER_MATCHUP_COLS)

    df = matches_df.copy()
    df["player_name"] = df["player_name"].map(clean_player_name)
    df = df.dropna(subset=["tid", "player_name"])
    if df.empty:
        return pd.DataFrame(columns=PLAYER_MATCHUP_COLS)
    for col in ["round", "table", "player_id", "is_winner", "status", "winner_id", "winner_name",
                "tournament_name", "start_date", "month", "deck_name_pt", "deck_name_en", "deck_url"]:
        if col not in df.columns:
            df[col] = None
    df["player_deck"] = df.apply(resolve_deck_label, axis=1)

    out_rows = []
    for _, table_df in df.groupby(["tid", "round", "table"], dropna=False, sort=False):
        participants = table_df.to_dict(orient="records")
        for i, player in enumerate(participants):
            player_name = player["player_name"]
            player_id = player["player_id"]
            if is_draw_result(player["status"], player["winner_id"], player["winner_name"]):
                result = "draw"
            else:
                try:
                    result = "win" if int(player["is_winner"]) == 1 else "lose"
                except (TypeError, ValueError):
                    result = "lose"

            for j, opp in enumerate(participants):
                opp_name = opp["player_name"]
                if i == j:
                    continue
                opp_id = opp["player_id"]
                if pd.notna(player_id) and pd.notna(opp_id):
                    same_player = str(player_id) == str(opp_id)
                else:
                    same_player = player_name.lower() == opp_name.lower()
                if same_player:
                    continue
                out_rows.append({
                    "tid": player["tid"],
                    "tournament_name": player["tournament_name"],
                    "start_date": player["start_date"],
                    "event_day": player["start_date"],
                    "month": player["month"],
                    "round": player["round"],
                    "table": player["table"],
                    "player_id": player_id,
                    "player_name": player_name,
                    "player_deck": player["player_deck"],
                    "opponent_id": opp_id,
                    "opponent_name": opp_name,
                    "opponent_deck": opp["player_deck"],
                    "result": result,
                })

    if not out_rows:
        return pd.DataFrame(columns=PLAYER_MATCHUP_COLS)
    return (
        pd.DataFrame(out_rows, columns=PLAYER_MATCHUP_COLS)
        .sort_values(["start_date", "tid", "round", "table", "player_name", "opponent_name"], kind="mergesort")
        .reset_index(drop=True)
    )


def clean_deck_keys(matches_df: pd.DataFrame) -> pd.DataFrame:
    """Linhas com jogador e deck valido (sem vazio/placeholder), com deck_key normalizado."""
    played = matches_df.copy()
    played["deck_key"] = played["deck_key"].astype("string").str.strip()
    played["deck_key"] = played["deck_key"].mask(played["deck_key"].eq(""))
    played = played.dropna(subset=["deck_key", "player_name"])
    return played[~played["deck_key"].map(is_placeholder_deck)].copy()


def compute_deck_stats_from_matches(matches_df: pd.DataFrame, league_type: str | None = None) -> pd.DataFrame:
    if matches_df.empty:
        return pd.DataFrame(columns=DECK_STATS_COLS)

    played = matches_df.copy()
    if "deck_key" not in played.columns:
        played["deck_key"] = played.get("deck_name_pt", pd.Series(None, index=played.index, dtype=object))
        if "deck_url" in played.columns:
            played["deck_key"] = played["deck_key"].fillna(played["deck_url"])
    played = clean_deck_keys(played)
    if played.empty:
        return pd.DataFrame(columns=DECK_STATS_COLS)

    for c in ["deck_name_pt", "deck_name_en", "colecao", "deck_url"]:
        if c not in played.columns:
            played[c] = None
    played["wins"] = played["is_winner"].fillna(0).astype(int)
    played["draws"] = draw_mask(played).astype(int)

    stats = played.groupby("deck_key").agg(
        partidas_jogadas=("deck_key", "size"),
        vitorias=("wins", "sum"),
        empates=("draws", "sum"),
        jogadores_unicos=("player_name", "nunique"),
        eventos=("tid", "nunique"),
        deck_name_pt=("deck_name_pt", first_notna),
        deck_name_en=("deck_name_en", first_notna),
        colecao=("colecao", first_notna),
        deck_url=("deck_url", lambda x: next((v for v in x if isinstance(v, str)), None)),
    ).reset_index()
    stats["derrotas"] = stats["partidas_jogadas"] - stats["vitorias"] - stats["empates"]
    stats["win_rate"] = (stats["vitorias"] / stats["partidas_jogadas"]).fillna(0)
    stats = stats.sort_values("partidas_jogadas", ascending=False).reindex(columns=DECK_STATS_COLS)
    return annotate_deck_stats_with_ban_rule(stats, league_type=league_type)


def compute_main_decks(matches_df: pd.DataFrame) -> dict:
    """Deck mais jogado por jogador (desempate alfabetico)."""
    if matches_df.empty or "deck_key" not in matches_df.columns:
        return {}
    played = clean_deck_keys(matches_df)
    if played.empty:
        return {}
    counts = (
        played.groupby(["player_name", "deck_key"]).size().reset_index(name="games")
        .sort_values(["player_name", "games", "deck_key"], ascending=[True, False, True])
    )
    return counts.groupby("player_name").head(1).set_index("player_name")["deck_key"].to_dict()


def prepare_scores(event_scores_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normaliza os resultados por evento e devolve (resultados ordenados, melhor resultado do mes)."""
    df = event_scores_df.dropna(subset=["month", "player_name"]).copy()
    df["event_points"] = pd.to_numeric(df.get("event_points"), errors="coerce").fillna(0)
    df["points_match"] = pd.to_numeric(df.get("points_match"), errors="coerce").fillna(0)
    df["win_rate"] = pd.to_numeric(df.get("win_rate"), errors="coerce").fillna(0.0)
    df["opp_win_rate"] = pd.to_numeric(df.get("opp_win_rate"), errors="coerce").fillna(0.0)
    df["start_date_val"] = pd.to_datetime(df.get("start_date"), errors="coerce")
    df = df.sort_values(by=["month", "player_name", *SCORE_SORT_COLS], ascending=[True, True] + [False] * 5)
    monthly_best = df.groupby(["month", "player_name"], as_index=False).head(1).drop(columns=["start_date_val"])
    return df, monthly_best


def expand_absences_with_zero_rows(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ["quarter", "player_name", "tid"]
    if df.empty or not set(required_cols).issubset(df.columns):
        return df
    valid = df.dropna(subset=required_cols)
    if valid.empty:
        return df

    meta_cols = [c for c in ["month", "start_date", "start_date_val", "tournament_name", "event_players"] if c in valid.columns]
    order = ["quarter", "start_date_val", "tid"] if "start_date_val" in valid.columns else ["quarter", "tid"]
    event_meta = (
        valid[["quarter", "tid", *meta_cols]]
        .sort_values(order, kind="mergesort")
        .drop_duplicates(subset=["quarter", "tid"], keep="first")
    )
    full_grid = valid[["quarter", "player_name"]].drop_duplicates().merge(event_meta, on="quarter", how="inner")
    present = valid[required_cols].drop_duplicates().assign(_present=True)
    missing = full_grid.merge(present, on=required_cols, how="left")
    missing = missing[missing["_present"].isna()].drop(columns=["_present"])
    if missing.empty:
        return df

    missing_rows = pd.DataFrame(index=missing.index, columns=df.columns)
    for col in missing.columns:
        if col in missing_rows.columns:
            missing_rows[col] = missing[col].values
    for col, default in [("event_points", 0), ("points_match", 0), ("win_rate", 0.0), ("opp_win_rate", 0.0)]:
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
    empty = pd.DataFrame(columns=["player_name", "league_points"])
    if events_df.empty:
        return empty

    df = events_df.dropna(subset=["month", "player_name"]).copy()
    df["player_name"] = df["player_name"].map(clean_player_name)
    df["quarter"] = df["month"].map(quarter_key_from_month)
    df = df.dropna(subset=["player_name", "quarter"])
    if df.empty:
        return empty

    df["event_points"] = pd.to_numeric(df.get("event_points"), errors="coerce").fillna(0)
    df["points_match"] = pd.to_numeric(df.get("points_match"), errors="coerce").fillna(0)
    df["win_rate"] = pd.to_numeric(df.get("win_rate"), errors="coerce").fillna(0.0)
    df["opp_win_rate"] = pd.to_numeric(df.get("opp_win_rate"), errors="coerce").fillna(0.0)
    df["start_date_val"] = pd.to_datetime(df.get("start_date"), errors="coerce")
    if include_absences_as_zero:
        df = expand_absences_with_zero_rows(df)

    def total(frame):
        return frame.groupby("player_name", as_index=False)["event_points"].sum().rename(
            columns={"event_points": "league_points"}
        )

    if not discard_worst_results:
        return total(df)

    df["preserved"] = False
    if preserve_best_month:
        best_month = (
            df.sort_values(SCORE_SORT_COLS, ascending=False, kind="mergesort")
            .groupby(["player_name", "quarter", "month"], as_index=False)
            .head(1)
        )
        best_keys = set(zip(best_month["tid"], best_month["player_name"]))
        df["preserved"] = [key in best_keys for key in zip(df["tid"], df["player_name"])]

    def apply_discards(group: pd.DataFrame) -> pd.DataFrame:
        group = group.copy()
        group["kept"] = True
        if len(group) >= 4 and discard_count > 0:
            candidates = group[~group["preserved"]].sort_values(SCORE_SORT_COLS, ascending=True, kind="mergesort")
            group.loc[candidates.head(discard_count).index, "kept"] = False
        return group

    df = group_apply_preserve_keys(df, ["player_name", "quarter"], apply_discards)
    return total(df[df["kept"]])


def add_event_counts(league: pd.DataFrame, scores: pd.DataFrame, key_col: str = "player_name") -> pd.DataFrame:
    if scores.empty:
        league["eventos"] = 0
        return league
    counts = scores.groupby("player_name", as_index=False)["tid"].nunique().rename(
        columns={"player_name": key_col, "tid": "eventos"}
    )
    return league.merge(counts, on=key_col, how="left")


def clean_nan(obj):
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    try:
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass
    return obj


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    return default if raw is None else str(raw).strip().lower() in {"1", "true", "yes", "on"}


# ---------------------------------------------------------------------------
# Leitura dos eventos
# ---------------------------------------------------------------------------

def parse_event(tid: str, data: dict, aliases: dict, offline_mode: bool, ctx: dict) -> None:
    """Converte o JSON da TopDeck em linhas de standings/tables/matches (acumuladas em ctx)."""
    tdata = data.get("data") if isinstance(data.get("data"), dict) else {}
    tournament_name = data.get("tournamentName") or tdata.get("name") or tid
    start_ts = data.get("startDate") or tdata.get("startDate")
    base = {"tid": tid, "tournament_name": tournament_name, "start_date": unix_to_date(start_ts)}
    mkey = month_key(start_ts)
    name_by_pid, deck_by_pid, deck_by_name = ctx["name_by_pid"], ctx["deck_by_pid"], ctx["deck_by_name"]

    for s in data.get("standings", []):
        name = resolve_player_alias(s.get("name"), aliases)
        pid = str(s["id"]) if s.get("id") is not None else None
        if pid and name:
            name_by_pid[(tid, pid)] = name
        deck_url = canonicalize_url(extract_deck_url(s))
        name_key = normalize_player_name(name)
        if deck_url and pid:
            deck_by_pid[(tid, pid)] = deck_url
        if deck_url and name_key:
            deck_by_name[(tid, name_key)] = deck_url
        ctx["standings"].append({
            **base,
            "month": mkey,
            "player_name": name,
            "player_id": pid,
            "standing": s.get("standing"),
            "points": s.get("points"),
            "win_rate": s.get("winRate", s.get("successRate")),
            "opp_win_rate": s.get("opponentWinRate", s.get("opponentSuccessRate")),
            "decklist": extract_decklist(s),
            "commanders": extract_commanders(s),
        })

    raw_rounds = data.get("rounds")
    if raw_rounds is None and not offline_mode:
        raw_rounds = fetch_rounds(tid)
    norm_rounds = normalize_rounds(raw_rounds or [])

    seen_tables = set()
    tables_added = matches_added = 0
    for rnd in norm_rounds:
        round_val = rnd.get("round")
        round_no = round_val if isinstance(round_val, int) else rnd.get("round_norm")

        for tbl in rnd.get("tables", []):
            table_no, status = tbl.get("table"), tbl.get("status")
            if (round_no, str(table_no)) in seen_tables:
                continue
            seen_tables.add((round_no, str(table_no)))
            ctx["tables"].append({**base, "round": round_no, "table": table_no, "status": status})
            tables_added += 1

            winner_id, winner_name = tbl.get("winner_id"), tbl.get("winner")
            if isinstance(winner_name, dict):
                winner_id = winner_id or winner_name.get("id")
                winner_name = winner_name.get("name")
            winner_id = str(winner_id) if winner_id is not None else None
            winner_name = (name_by_pid.get((tid, winner_id)) if winner_id else None) or resolve_player_alias(
                winner_name, aliases
            )
            winner_norm = normalize_player_name(winner_name) if isinstance(winner_name, str) else None

            for p in tbl.get("players", []):
                pname = resolve_player_alias(p.get("name"), aliases)
                pid = str(p["id"]) if p.get("id") is not None else None
                if pid:
                    pname = name_by_pid.get((tid, pid), pname)
                deck_url = canonicalize_url(extract_deck_url(p))
                if not deck_url:
                    deck_url = (deck_by_pid.get((tid, pid)) if pid else None) or deck_by_name.get(
                        (tid, normalize_player_name(pname))
                    )
                is_winner = (winner_id is not None and pid == winner_id) or bool(
                    winner_norm and winner_norm == normalize_player_name(pname)
                )
                ctx["matches"].append({
                    **base,
                    "month": mkey,
                    "round": round_no,
                    "table": table_no,
                    "status": status,
                    "winner_id": winner_id,
                    "winner_name": winner_name,
                    "player_id": pid,
                    "player_name": pname,
                    "is_winner": int(is_winner),
                    "deck_url": deck_url,
                    "deck_raw": extract_decklist(p),
                })
                matches_added += 1

    print(f"Rounds recebidos: {len(norm_rounds)} | Mesas: {tables_added} | Linhas matches: {matches_added}")


# ---------------------------------------------------------------------------
# Saidas por formato
# ---------------------------------------------------------------------------

def write_outputs(out_dir: str, csvs: dict[str, pd.DataFrame], site: dict) -> None:
    for name, frame in csvs.items():
        frame.to_csv(os.path.join(out_dir, name), index=False, encoding="utf-8")
    with open(os.path.join(out_dir, "site.json"), "w", encoding="utf-8") as f:
        json.dump(clean_nan(site), f, ensure_ascii=False, allow_nan=False)


def process_two_by_two(league_type, out_dir, df, tables_df, matches_df, team_map_2x2, aliases, deck_map, site_meta):
    team_df, map_standings = enrich_two_by_two_team_frame(df, team_map_2x2, aliases, deck_map)
    team_matches_df, map_matches = enrich_two_by_two_team_frame(matches_df, team_map_2x2, aliases, deck_map)
    team_map_df = pd.concat([map_standings, map_matches], ignore_index=True)
    if not team_map_df.empty:
        team_map_df = (
            team_map_df.drop_duplicates(subset=["tid", "raw_team"])
            .sort_values(["tid", "team_name"], kind="mergesort")
            .reset_index(drop=True)
        )

    team_matches_df = normalize_two_by_two_winner_names(team_matches_df, team_map_df)
    player_matches_df = expand_two_by_two_matches_to_players(team_matches_df)
    team_scores_df = apply_official_event_rank(
        compute_event_scores_from_matches(team_matches_df), team_df, matches_df=team_matches_df
    )
    player_scores_df = expand_two_by_two_scores_to_players(team_scores_df, team_map_df)
    team_df = apply_official_rank_to_standings(team_df, matches_df=team_matches_df)
    discard_count = discard_count_for_league(league_type)

    # Tabela da liga por jogador
    df2, monthly_best = prepare_scores(player_scores_df)
    league = compute_quarterly_points(
        df2, preserve_best_month=False, include_absences_as_zero=True, discard_count=discard_count
    )
    league = add_event_counts(league, df2)
    deck_principal = compute_main_decks(player_matches_df)
    league["deck_principal"] = league["player_name"].map(deck_principal.get)
    league = league.sort_values(["league_points", "eventos", "player_name"], ascending=[False, False, True])

    # Tabela da liga por dupla (agrupada pela chave dos dois jogadores)
    team_df2 = team_scores_df.dropna(subset=["month", "player_name"]).copy()
    if not team_df2.empty and not team_map_df.empty:
        identity = team_map_df.reindex(columns=["tid", "team_name", "team_key", "player_1", "player_2"]).copy()
        identity["display_team_name"] = [
            canonical_two_by_two_team_name(p1, p2) or name
            for p1, p2, name in zip(identity["player_1"], identity["player_2"], identity["team_name"])
        ]
        identity = identity.drop_duplicates(subset=["tid", "team_name"])
        team_df2 = team_df2.merge(
            identity[["tid", "team_name", "team_key", "display_team_name"]],
            left_on=["tid", "player_name"], right_on=["tid", "team_name"], how="left",
        )
    for col in ["team_key", "display_team_name"]:
        if col not in team_df2.columns:
            team_df2[col] = None
    team_df2["team_key"] = team_df2["team_key"].fillna(team_df2["player_name"].map(normalize_team_key))
    team_df2["display_team_name"] = team_df2["display_team_name"].fillna(team_df2["player_name"])
    team_display = (
        team_df2.dropna(subset=["team_key", "display_team_name"])
        .groupby("team_key")["display_team_name"]
        .agg(lambda values: values.value_counts().index[0])
        .rename("team_name")
        .reset_index()
    )
    team_df2["player_name"] = team_df2["team_key"]
    team_league = compute_quarterly_points(
        team_df2, preserve_best_month=False, include_absences_as_zero=True, discard_count=discard_count
    ).rename(columns={"player_name": "team_key"})
    team_league = add_event_counts(team_league, team_df2, key_col="team_key")
    team_league = team_league.merge(team_display, on="team_key", how="left")
    team_league["team_name"] = team_league["team_name"].fillna(team_league["team_key"])
    team_league = team_league.sort_values(["league_points", "eventos", "team_name"], ascending=[False, False, True])
    team_league = team_league.reindex(columns=["team_name", "team_key", "league_points", "eventos"])

    # Pendencias de cadastro
    missing_flag = lambda col: pd.to_numeric(team_map_df.get(col), errors="coerce").fillna(1) > 0  # noqa: E731
    unmapped_teams_df = team_map_df[missing_flag("missing_players") | missing_flag("missing_decks")].copy()
    missing_deck_rows = [
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
        for row in team_map_df.to_dict(orient="records")
        for slot in [1, 2]
        if not clean_player_name(row.get(f"deck_url_{slot}"))
    ]
    unmapped_decks_df = pd.DataFrame(
        missing_deck_rows,
        columns=["tid", "tournament_name", "start_date", "team_name", "raw_team", "player_name", "slot", "deck"],
    )
    deck_stats_df = compute_deck_stats_from_matches(player_matches_df, league_type=league_type)

    write_outputs(
        out_dir,
        {
            "unmapped_teams.csv": unmapped_teams_df,
            "unmapped_decks.csv": unmapped_decks_df,
            "standings.csv": team_df,
            "team_standings.csv": team_df,
            "tables.csv": tables_df,
            "matches.csv": player_matches_df,
            "team_matches.csv": team_matches_df,
            "event_summary.csv": player_scores_df,
            "player_event_summary.csv": player_scores_df,
            "team_event_summary.csv": team_scores_df,
            "player.csv": build_player_matchups(player_matches_df),
            "monthly_best.csv": monthly_best,
            "league_table.csv": league,
            "player_league_table.csv": league,
            "team_league_table.csv": team_league,
            "team_player_map.csv": team_map_df,
            "deck_stats.csv": deck_stats_df,
        },
        {
            **site_meta,
            "league_format": "2x2",
            "rounds_per_event": 3,
            "standings": team_df.to_dict(orient="records"),
            "tables": tables_df.to_dict(orient="records"),
            "matches": player_matches_df.to_dict(orient="records"),
            "team_matches": team_matches_df.to_dict(orient="records"),
            "event_scores": player_scores_df.to_dict(orient="records"),
            "team_event_scores": team_scores_df.to_dict(orient="records"),
            "monthly_best": monthly_best.to_dict(orient="records"),
            "league_table": league.to_dict(orient="records"),
            "player_league_table": league.to_dict(orient="records"),
            "team_league_table": team_league.to_dict(orient="records"),
            "team_player_map": team_map_df.to_dict(orient="records"),
            "deck_stats": deck_stats_df.to_dict(orient="records"),
            "player_aliases": aliases,
        },
    )
    print(
        f"\nOK ({league_type})! Gerado 2x2: standings.csv, team_matches.csv, matches.csv, "
        f"player_event_summary.csv, team_event_summary.csv, league_table.csv, "
        f"team_league_table.csv, deck_stats.csv, site.json em {out_dir}"
    )
    if not unmapped_teams_df.empty:
        print(f"Aviso 2x2: {len(unmapped_teams_df)} dupla(s) precisam revisao em {out_dir}/unmapped_teams.csv")


def process_single(league_type, out_dir, df, tables_df, matches_df, ctx, deck_map, aliases, site_meta):
    unmapped_cols = ["tid", "tournament_name", "start_date", "player_name", "deck_url"]
    unmapped_df = pd.DataFrame(columns=unmapped_cols)

    if not matches_df.empty:
        # Deck do standing tem prioridade sobre o deck informado na mesa.
        tids = matches_df["tid"].map(lambda v: str(v) if pd.notna(v) else None)
        pids = matches_df["player_id"].map(lambda v: str(v) if pd.notna(v) else None)
        names = matches_df["player_name"].map(normalize_player_name)
        matches_df["deck_url"] = [
            (ctx["deck_by_pid"].get((t, p)) if t and p else None)
            or (ctx["deck_by_name"].get((t, n)) if t and n else None)
            or (u if isinstance(u, str) and u else None)
            for t, p, n, u in zip(tids, pids, names, matches_df["deck_url"])
        ]
        matches_df["deck_url"] = matches_df["deck_url"].map(canonicalize_url)

        if deck_map:
            for key in ["deck_name_pt", "deck_name_en", "colecao"]:
                matches_df[key] = matches_df["deck_url"].map(
                    lambda u: (deck_map.get(u) or {}).get(key) if isinstance(u, str) else None
                )

        total = len(matches_df)
        print(f"Matches com deck_url preenchido: {matches_df['deck_url'].notna().sum()} / {total}")
        examples = matches_df["deck_url"].dropna().unique()[:5]
        print(f"Exemplos deck_url: {', '.join(examples) if len(examples) else '-'}")
        mapped = matches_df["deck_name_pt"].notna().sum() if "deck_name_pt" in matches_df.columns else 0
        print(f"Matches com deck mapeado: {mapped} / {total}")

        deck_url_series = matches_df["deck_url"].astype("string").str.strip()
        missing_url_mask = deck_url_series.isna() | deck_url_series.eq("")
        unmapped_url_mask = ~missing_url_mask & ~deck_url_series.isin(set(deck_map))
        problems = matches_df[missing_url_mask | unmapped_url_mask]
        if not problems.empty:
            unmapped_df = (
                problems.reindex(columns=unmapped_cols).drop_duplicates().sort_values(["tid", "player_name", "deck_url"])
            )
        print(
            f"Decks com problema de cadastro: {len(unmapped_df)} "
            f"(sem URL: {int(missing_url_mask.sum())} | URL sem mapping: {int(unmapped_url_mask.sum())}) "
            f"(salvo em {out_dir}/unmapped_decks.csv)"
        )
    else:
        print(f"Matches vazios: {out_dir}/unmapped_decks.csv gerado vazio.")

    event_scores_df = apply_official_event_rank(compute_event_scores_from_matches(matches_df), df, matches_df=matches_df)

    if not df.empty and not matches_df.empty:
        # Completa nomes de standings a partir das mesas (tid + player_id).
        name_map = matches_df.dropna(subset=["tid", "player_id", "player_name"]).copy()
        name_map["tid_key"], name_map["pid_key"] = name_map["tid"].astype(str), name_map["player_id"].astype(str)
        name_map = name_map.drop_duplicates(subset=["tid_key", "pid_key"])
        df["tid_key"], df["pid_key"] = df["tid"].astype(str), df["player_id"].astype(str)
        df = df.merge(
            name_map[["tid_key", "pid_key", "player_name"]].rename(columns={"player_name": "player_name_match"}),
            on=["tid_key", "pid_key"],
            how="left",
        )
        df["player_name"] = df["player_name"].fillna(df["player_name_match"])
        df = df.drop(columns=["player_name_match", "tid_key", "pid_key"])

    if not df.empty and not event_scores_df.empty:
        # Completa opp_win_rate ausente com o calculado a partir das mesas.
        df["tid"] = df["tid"].astype("string")
        df["player_name_norm"] = df["player_name"].map(normalize_player_name)
        opp = event_scores_df[["tid", "player_name", "opp_win_rate"]].copy()
        opp["tid"] = opp["tid"].astype("string")
        opp["player_name_norm"] = opp["player_name"].map(normalize_player_name)
        df = df.merge(
            opp[["tid", "player_name_norm", "opp_win_rate"]].rename(columns={"opp_win_rate": "opp_win_rate_match"}),
            on=["tid", "player_name_norm"],
            how="left",
        )
        df["opp_win_rate"] = df["opp_win_rate"].fillna(df["opp_win_rate_match"])
        df = df.drop(columns=["opp_win_rate_match", "player_name_norm"])

    if not df.empty:
        df = apply_official_rank_to_standings(df, matches_df=matches_df)

    # matches.csv nao leva deck_key; o site.json leva.
    csvs = {
        "unmapped_decks.csv": unmapped_df,
        "event_summary.csv": event_scores_df,
        "player.csv": build_player_matchups(matches_df),
        "standings.csv": df,
        "tables.csv": tables_df,
        "matches.csv": matches_df.copy(),
    }

    df2, monthly_best = prepare_scores(event_scores_df)
    league = compute_quarterly_points(
        df2,
        discard_worst_results=True,
        preserve_best_month=(league_type != "presencial"),
        include_absences_as_zero=True,
        discard_count=discard_count_for_league(league_type),
    )
    league = add_event_counts(league, df2)
    league = league.sort_values(["league_points", "eventos", "player_name"], ascending=[False, False, True])

    if not matches_df.empty:
        deck_key = matches_df.get("deck_name_pt", matches_df["deck_url"]).fillna(matches_df["deck_url"])
        deck_key = deck_key.astype("string").str.strip()
        matches_df["deck_key"] = deck_key.mask(deck_key.eq("")).fillna(matches_df["deck_url"])
    league["deck_principal"] = league["player_name"].map(compute_main_decks(matches_df).get)
    deck_stats_df = compute_deck_stats_from_matches(matches_df, league_type=league_type)

    csvs.update({"monthly_best.csv": monthly_best, "league_table.csv": league, "deck_stats.csv": deck_stats_df})
    write_outputs(
        out_dir,
        csvs,
        {
            **site_meta,
            "standings": df.to_dict(orient="records"),
            "tables": tables_df.to_dict(orient="records"),
            "matches": matches_df.to_dict(orient="records"),
            "event_scores": event_scores_df.to_dict(orient="records"),
            "monthly_best": monthly_best.to_dict(orient="records"),
            "league_table": league.to_dict(orient="records"),
            "deck_stats": deck_stats_df.to_dict(orient="records"),
            "player_aliases": aliases,
        },
    )
    print(
        f"\nOK ({league_type})! Gerado: standings.csv, tables.csv, matches.csv, event_summary.csv, "
        f"player.csv, monthly_best.csv, league_table.csv, deck_stats.csv, site.json em {out_dir}"
    )


def main():
    run_started_at = datetime.now().astimezone()
    site_meta = {
        "last_updated": run_started_at.strftime("%Y-%m-%d"),
        "last_updated_at": run_started_at.isoformat(timespec="seconds"),
    }
    offline_mode = env_flag("TOPDECK_OFFLINE")
    if not API_KEY and not offline_mode:
        raise SystemExit("Defina TOPDECK_API_KEY no terminal (nao coloque a key no arquivo).")
    if offline_mode:
        print("Modo offline ativo (TOPDECK_OFFLINE=1): usando apenas RAW local.")

    with open("events.json", "r", encoding="utf-8-sig") as f:
        events = json.load(f)
    aliases = load_player_aliases("player_aliases.json")
    deck_map = load_deck_map_csv(DECK_MAP_PATH)
    team_map_2x2 = load_team_map_2x2(TEAM_MAP_2X2_PATH)
    team_map_rows = len(pd.read_csv(TEAM_MAP_2X2_PATH, encoding="utf-8-sig")) if os.path.exists(TEAM_MAP_2X2_PATH) else 0
    print(
        f"Aliases carregados: {len(aliases)} | Deck map carregado: {len(deck_map)} | "
        f"Team map 2x2 carregado: {team_map_rows} linha(s), {len(team_map_2x2)} chave(s)"
    )

    grouped = {league: [] for league in LEAGUE_ORDER}
    for e in events:
        grouped[normalize_league(e.get("league"))].append(e)

    for league_type in LEAGUE_ORDER:
        league_events = grouped[league_type]
        event_tids = {str(e.get("tid")) for e in league_events if e.get("tid")}
        raw_dir = os.path.join("data", "raw", league_type)
        out_dir = os.path.join("data", league_type)
        print(f"==== Processando liga {league_type} ({len(event_tids)} evento(s)) ====")
        os.makedirs(raw_dir, exist_ok=True)
        os.makedirs(out_dir, exist_ok=True)

        # Remove RAWs de eventos que sairam do events.json.
        stale = [n for n in os.listdir(raw_dir) if n.endswith(".json") and n[:-5] not in event_tids]
        for name in stale:
            os.remove(os.path.join(raw_dir, name))
        if stale:
            print(f"{raw_dir} limpo: {len(stale)} arquivo(s) stale removido(s)")

        ctx = {"standings": [], "tables": [], "matches": [], "name_by_pid": {}, "deck_by_pid": {}, "deck_by_name": {}}
        failed_tids = []
        for e in league_events:
            tid = str(e["tid"])
            print(f"\n=== Processando: {tid} ===")
            data = load_event_data(tid, os.path.join(raw_dir, f"{tid}.json"), offline_mode)
            if data is None:
                failed_tids.append(tid)
                continue
            parse_event(tid, data, aliases, offline_mode, ctx)

        df = pd.DataFrame(ctx["standings"]).reindex(columns=STANDINGS_COLS)
        tables_df = pd.DataFrame(ctx["tables"]).reindex(columns=TABLES_COLS)
        matches_df = pd.DataFrame(ctx["matches"]).reindex(columns=MATCHES_COLS)
        df["player_name"] = df["player_name"].map(clean_player_name)
        for col in ["winner_name", "player_name"]:
            matches_df[col] = matches_df[col].map(clean_player_name)

        if league_type in TWO_BY_TWO_LEAGUES:
            process_two_by_two(league_type, out_dir, df, tables_df, matches_df, team_map_2x2, aliases, deck_map, site_meta)
        else:
            process_single(league_type, out_dir, df, tables_df, matches_df, ctx, deck_map, aliases, site_meta)
        if failed_tids:
            print(f"Aviso: {len(failed_tids)} evento(s) falharam nesta liga: {', '.join(failed_tids)}")

    print("\nConcluido! Conjuntos gerados:")
    for league_type in LEAGUE_ORDER:
        print(f"- {league_type}: {len(grouped[league_type])} evento(s) -> {os.path.join('data', league_type)}")


if __name__ == "__main__":
    main()
