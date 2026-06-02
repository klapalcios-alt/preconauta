# PRECONAUTA-LEAGUE

Projeto para sincronizar dados do TopDeck, gerar CSVs/JSON e alimentar o site estático.

## Como adicionar eventos
Edite `events.json` na raiz com a lista de TIDs e a liga:

```json
[
  { "tid": "liga-mesao-online-s1-e01", "league": "online" },
  { "tid": "liga-mesao-presencial-s1-e01", "league": "presencial" },
  { "tid": "liga-2x2-online-s1-e01", "league": "online2x2", "format": "2x2" },
  { "tid": "liga-2x2-presencial-s1-e01-1", "league": "presencial2x2", "format": "2x2" }
]
```

- `league` pode ser `presencial`, `online`, `presencial2x2` ou `online2x2`.
- Se `league` estiver ausente, o padrão é `presencial`.

## Como rodar o sync
Execute:

```powershell
.\run_sync.ps1
```

Antes disso, a chave da TopDeck deve ficar fora do versionamento:

- opcao 1: definir `TOPDECK_API_KEY` no terminal
- opcao 2: criar `topdeck_key.local.txt` na raiz do projeto

Nao suba `topdeck_key.local.txt`, `.env` ou qualquer segredo para o GitHub.

O script `scripts/sync_topdeck.py` vai:
- Ler `events.json`.
- Baixar dados do TopDeck.
- Salvar raw em `data/raw/<league>/<tid>.json`.
- Gerar CSVs e `site.json` por liga.

## Saídas por liga
Os arquivos ficam separados por liga:

- `data/presencial/`
- `data/online/`
- `data/presencial2x2/`
- `data/online2x2/`

Arquivos gerados em cada pasta:
- `standings.csv`
- `tables.csv`
- `matches.csv`
- `event_summary.csv`
- `player.csv`
- `monthly_best.csv`
- `league_table.csv`
- `deck_stats.csv`
- `unmapped_decks.csv`
- `site.json`

Nas ligas 2x2, tambem sao gerados:
- `team_standings.csv`
- `team_matches.csv`
- `team_event_summary.csv`
- `team_league_table.csv`
- `team_player_map.csv`
- `player_event_summary.csv`
- `player_league_table.csv`
- `unmapped_teams.csv`

## Regras de pontuação (evento)
A pontuação por evento segue o tamanho do torneio:

1. Até 12 jogadores (TOP4)
   - 1º: 3 pontos
   - 2º ao 4º: 2 pontos
   - Demais: 1 ponto
2. 13 a 24 jogadores (TOP8)
   - 1º: 3 pontos
   - 2º ao 8º: 2 pontos
   - Demais: 1 ponto
3. 25 a 36 jogadores (TOP12)
   - 1º: 3 pontos
   - 2º ao 12º: 2 pontos
   - Demais: 1 ponto
4. 37+ jogadores (TOP16)
   - 1º: 3 pontos
   - 2º ao 16º: 2 pontos
   - Demais: 1 ponto

## Pontuação da liga (trimestral)
A liga é calculada por trimestre:

- Q1: Jan–Mar
- Q2: Abr–Jun
- Q3: Jul–Set
- Q4: Out–Dez

Regras:
- Se o jogador tiver **menos de 4 eventos no trimestre**, **não descarta** nenhum resultado.
- Caso contrário:
  - **Preserva o melhor resultado de cada mês** do trimestre.
  - Entre os demais resultados, **descarta os 3 piores**.
- A pontuação final do trimestre é a soma dos pontos dos eventos mantidos.

### Critérios de desempate (evento e liga)
1. Pontos (match points)
2. Winrate
3. Opp Winrate

> Observação: Opponent Game Win Rate não existe em Standard Tournament no TopDeck,
> por isso não é usado.

## Site
O site usa os dados por liga:

- `site/index.html`: home com escolha de liga
- `site/liga.html`: classificação da liga
- `site/players.html`: classificação geral de jogadores
- `site/duplas.html`: classificação e estatísticas de duplas 2x2
- `site/decks.html`: estatísticas de decks
- `site/decks-banidos.html`: decks banidos (se houver)

Os arquivos carregam `../data/<league>/site.json` com `?league=presencial|online|presencial2x2|online2x2`.

## Publicacao no GitHub Pages
O workflow em `.github/workflows/deploy-pages.yml` publica apenas:

- `site/`
- `data/presencial/site.json`
- `data/online/site.json`
- `data/presencial2x2/site.json`
- `data/online2x2/site.json`

Isso evita publicar a chave da API e tambem evita expor o cache bruto em `data/raw/`.

## Observações
- `deck_map.csv`, `player_aliases.json` e `Team_map_2x2.csv` ficam na raiz e são globais.
- `unmapped_decks.csv` inclui: `tid`, `tournament_name`, `start_date`, `player_name`, `deck_url`.
- `Team_map_2x2.csv` normaliza duplas e decks em eventos 2x2.
- Para importar planilha manual 2x2: `python scripts/import_team_map_2x2.py "C:\caminho\planilha.xlsx"`.
- `deck_aliases.json` permite mapear apelidos de deck/comandante para o nome oficial do `Deck_map.csv`.

## Handoff para outra IA
- `docs/AI_HANDOFF.md`: mapeamento completo de estrutura, fluxo e regras.
- `docs/site_data_contract.json`: contrato de dados machine-readable.
- `docs/PROMPT_OUTRA_IA.md`: prompt pronto para copiar/colar em outra IA.
