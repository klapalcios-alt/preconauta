import argparse
import csv
import re
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

from sync_topdeck import (
    TEAM_MAP_2X2_PATH,
    clean_player_name,
    find_deck_by_name,
    load_deck_map_csv,
    load_player_aliases,
    is_placeholder_deck,
    normalize_lookup_key,
    resolve_player_alias,
    canonical_two_by_two_team_name,
    two_by_two_player_pair_key,
)


OUTPUT_COLUMNS = [
    "tid",
    "tournament_name",
    "start_date",
    "month",
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
    "notes",
]


COLUMN_ALIASES = {
    "tid": ["tid", "evento", "event", "tournament"],
    "raw_team": ["raw_team", "dupla", "team", "time"],
    "player_1": ["player_1", "player 1", "jogador 1"],
    "player_2": ["player_2", "player 2", "jogador 2"],
    "deck_1": ["deck_1", "deck 1"],
    "deck_2": ["deck_2", "deck 2"],
}


NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}


def read_rows(path: Path) -> list[dict]:
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))
    if path.suffix.lower() in {".xlsx", ".xlsm"}:
        return read_xlsx_rows(path)
    raise SystemExit(f"Formato nao suportado: {path.suffix}")


def read_xlsx_rows(path: Path) -> list[dict]:
    with zipfile.ZipFile(path) as z:
        shared_strings = []
        if "xl/sharedStrings.xml" in z.namelist():
            root = ET.fromstring(z.read("xl/sharedStrings.xml"))
            for item in root.findall("a:si", NS):
                shared_strings.append("".join(t.text or "" for t in item.findall(".//a:t", NS)))

        workbook = ET.fromstring(z.read("xl/workbook.xml"))
        rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
        rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
        sheet = workbook.find("a:sheets/a:sheet", NS)
        if sheet is None:
            return []
        rel_id = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
        target = rel_map[rel_id]
        if not target.startswith("xl/"):
            target = f"xl/{target}"

        root = ET.fromstring(z.read(target))
        rows = []
        for row in root.findall("a:sheetData/a:row", NS):
            values = {column_index(cell.attrib["r"]): cell_value(cell, shared_strings) for cell in row.findall("a:c", NS)}
            if values:
                rows.append([values.get(i) for i in range(1, max(values) + 1)])

    if not rows:
        return []
    headers = [str(value).strip() if value is not None else "" for value in rows[0]]
    return [
        dict(zip(headers, row + [None] * (len(headers) - len(row))))
        for row in rows[1:]
        if any(value not in (None, "") for value in row)
    ]


def column_index(cell_ref: str) -> int:
    letters = re.match(r"([A-Z]+)", cell_ref).group(1)
    value = 0
    for letter in letters:
        value = value * 26 + ord(letter) - 64
    return value


def cell_value(cell, shared_strings: list[str]):
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(t.text or "" for t in cell.findall(".//a:t", NS))
    value = cell.find("a:v", NS)
    if value is None:
        return None
    text = value.text or ""
    if cell_type == "s":
        return shared_strings[int(text)] if text else ""
    return text


def get_value(row: dict, canonical_name: str) -> str | None:
    lookup = {normalize_lookup_key(key): value for key, value in row.items()}
    for alias in COLUMN_ALIASES[canonical_name]:
        value = lookup.get(normalize_lookup_key(alias))
        cleaned = clean_player_name(value)
        if cleaned:
            return cleaned
    return None


def canonical_team_name(player_1: str | None, player_2: str | None) -> str | None:
    return canonical_two_by_two_team_name(player_1, player_2)


def deck_fields(deck_label: str | None, deck_map: dict, slot: int) -> dict:
    if is_placeholder_deck(deck_label):
        return {
            f"deck_{slot}": None,
            f"deck_url_{slot}": None,
            f"deck_name_pt_{slot}": None,
            f"deck_name_en_{slot}": None,
            f"colecao_{slot}": None,
        }

    info = find_deck_by_name(deck_label, deck_map) if deck_label else None
    deck_name_pt = info.get("deck_name_pt") if info else clean_player_name(deck_label)
    deck_name_en = info.get("deck_name_en") if info else None
    return {
        f"deck_{slot}": deck_name_pt,
        f"deck_url_{slot}": info.get("deck_url") if info else None,
        f"deck_name_pt_{slot}": deck_name_pt,
        f"deck_name_en_{slot}": deck_name_en,
        f"colecao_{slot}": info.get("colecao") if info else None,
    }


def build_map_rows(rows: list[dict], deck_map: dict, player_aliases: dict) -> list[dict]:
    out = []
    for row in rows:
        tid = get_value(row, "tid")
        raw_team = get_value(row, "raw_team")
        player_1 = resolve_player_alias(get_value(row, "player_1"), player_aliases)
        player_2 = resolve_player_alias(get_value(row, "player_2"), player_aliases)
        deck_1_label = get_value(row, "deck_1")
        deck_2_label = get_value(row, "deck_2")
        if not tid or not raw_team:
            continue

        item = {column: None for column in OUTPUT_COLUMNS}
        item["tid"] = tid
        item["raw_team"] = raw_team
        item["player_1"] = player_1
        item["player_2"] = player_2
        item["team_name"] = canonical_team_name(player_1, player_2) or clean_player_name(raw_team)
        item["team_key"] = two_by_two_player_pair_key(player_1, player_2) or normalize_lookup_key(raw_team)
        item.update(deck_fields(deck_1_label, deck_map, 1))
        item.update(deck_fields(deck_2_label, deck_map, 2))
        item["map_status"] = "manual"
        item["missing_players"] = int(not (player_1 and player_2))
        item["missing_decks"] = int(not (item["deck_url_1"] and item["deck_url_2"]))
        item["notes"] = "importado de planilha"
        out.append(item)
    return out


def load_existing(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def merge_rows(existing: list[dict], imported: list[dict]) -> list[dict]:
    merged = {}
    for row in existing:
        tid = clean_player_name(row.get("tid"))
        raw_team = clean_player_name(row.get("raw_team"))
        if tid and raw_team:
            merged[(tid, normalize_lookup_key(raw_team))] = normalize_output_row(row)
    for row in imported:
        key = (row["tid"], normalize_lookup_key(row["raw_team"]))
        incoming = normalize_output_row(row)
        current = merged.get(key)
        if current and is_manual_identity_row(current):
            merged[key] = merge_preserving_manual_identity(current, incoming)
        else:
            merged[key] = incoming
    return sorted(merged.values(), key=lambda row: (row.get("tid") or "", row.get("team_name") or "", row.get("raw_team") or ""))


def is_manual_identity_row(row: dict) -> bool:
    notes = normalize_lookup_key(row.get("notes")) or ""
    return "normalizacao manual" in notes


def merge_preserving_manual_identity(current: dict, incoming: dict) -> dict:
    preserved_identity = {
        column: current.get(column)
        for column in ["team_name", "team_key", "player_1", "player_2", "map_status", "missing_players", "notes"]
    }
    out = {**current, **incoming}
    out.update({column: value for column, value in preserved_identity.items() if value})
    return normalize_output_row(out)


def normalize_output_row(row: dict) -> dict:
    out = {column: clean_player_name(row.get(column)) for column in OUTPUT_COLUMNS}
    return out


def write_rows(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Importa planilha manual de duplas 2x2 para Team_map_2x2.csv.")
    parser.add_argument("source", help="Caminho da planilha .xlsx/.csv com evento, dupla, players e decks.")
    parser.add_argument("--output", default=TEAM_MAP_2X2_PATH, help="Arquivo Team_map_2x2.csv de destino.")
    args = parser.parse_args()

    source = Path(args.source)
    output = Path(args.output)
    deck_map = load_deck_map_csv()
    player_aliases = load_player_aliases()
    rows = build_map_rows(read_rows(source), deck_map, player_aliases)
    merged = merge_rows(load_existing(output), rows)
    write_rows(output, merged)

    unresolved_decks = sum(int(row.get("missing_decks") or 0) for row in rows)
    print(f"Importadas {len(rows)} linha(s) de {source}.")
    print(f"Gravado {output} com {len(merged)} linha(s).")
    if unresolved_decks:
        print(f"Aviso: {unresolved_decks} linha(s) ainda tem deck sem resolver no Deck_map.csv.")


if __name__ == "__main__":
    main()
