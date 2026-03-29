# AI Handoff - Preconauta League

Documento para outra IA conseguir replicar o projeto em outro site sem perder regras de negocio, estrutura de dados e logica de ranking.

Atualizado em: 2026-03-06

## 1) Visao geral

- Tipo de projeto: site estatico (HTML + React UMD + Babel no browser) + pipeline Python para ingestao/sincronizacao.
- Fonte de dados: Topdeck API.
- Saida final usada no front: `data/<league>/site.json`.
- Ligas suportadas: `presencial` e `online`.
- Parametro de roteamento no front: `?league=presencial|online`.
- Estado atual de eventos em `events.json`: 8 eventos (5 presencial, 3 online).

## 2) Estrutura de pastas e arquivos

```text
preconauta-league/
  data/
    online/
      deck_stats.csv
      event_summary.csv
      league_table.csv
      matches.csv
      monthly_best.csv
      player.csv
      site.json
      standings.csv
      tables.csv
      unmapped_decks.csv
    presencial/
      deck_stats.csv
      event_summary.csv
      league_table.csv
      matches.csv
      monthly_best.csv
      player.csv
      site.json
      standings.csv
      tables.csv
      unmapped_decks.csv
    raw/
      online/
        <tid>.json
      presencial/
        <tid>.json
  scripts/
    sync_topdeck.py
  site/
    index.html
    liga.html
    players.html
    player.html
    decks.html
    decks-banidos.html
    regras.html
    privacidade.html
  events.json
  run_sync.ps1
  topdeck_key.txt
  Deck_map.csv
  player_aliases.json (opcional)
  README.md
  docs/
    AI_HANDOFF.md
    site_data_contract.json
    PROMPT_OUTRA_IA.md
```

## 3) Pipeline ponta a ponta

1. Definir eventos em `events.json`.

2. Definir chave da API Topdeck.
- `run_sync.ps1` le `topdeck_key.txt` e injeta em `TOPDECK_API_KEY`.

3. Opcional: criar `player_aliases.json` na raiz para normalizar nomes.
- Formato esperado: objeto `{ "Nome Original": "Nome Canonico" }`.

4. Rodar:
- `./run_sync.ps1`

5. `scripts/sync_topdeck.py` executa por liga (`presencial`, `online`):
- Le eventos e separa por liga.
- Limpa `data/raw/<league>/` antes do sync da liga.
- Faz download de torneios no Topdeck.
- Se download falhar e existir raw local, usa raw local.
- Salva raw em `data/raw/<league>/<tid>.json`.
- Gera CSVs por liga.
- Gera `site.json` por liga.

6. Frontend le:
- `../data/<league>/site.json` (algumas paginas testam fallback para `/data/...` e `./data/...`).

## 4) Integracao Topdeck API

Referencia de codigo:
- `scripts/sync_topdeck.py:195`
- `scripts/sync_topdeck.py:254`
- `scripts/sync_topdeck.py:261`

### 4.1 Config

- Base URL: `https://topdeck.gg/api`
- Auth: header `Authorization: <TOPDECK_API_KEY>`
- Env var obrigatoria: `TOPDECK_API_KEY`

### 4.2 Endpoints usados

1. POST `/v2/tournaments`
- Payload:
  ```json
  {
    "columns": ["decklist", "wins", "draws", "losses"],
    "rounds": true,
    "tables": ["table", "players", "winner"],
    "players": ["name", "id", "decklist"],
    "TID": ["<tid>"],
    "game": "Magic: The Gathering",
    "format": "EDH"
  }
  ```
- Objetivo: pegar standings + rounds + mesas + jogadores.

2. GET `/v2/tournaments/{tid}/`
- Usado quando standings da chamada POST vierem incompletos (sem `opponentWinRate`).

3. GET `/v2/tournaments/{tid}/rounds`
- Fallback quando rounds nao vierem no objeto principal.

## 5) Contrato de dados

### 5.1 Arquivos de entrada

1. `events.json`
- Campos:
  - `tid` (string, obrigatorio)
  - `league` (string, opcional: `online` ou `presencial`; default `presencial`)

2. `Deck_map.csv`
- Colunas obrigatorias no codigo:
  - `Link`
  - `Nome PT-BR`
  - `Nome ENG`
  - `ColeÃ§Ã£o`

3. `player_aliases.json` (opcional)
- Mapa simples de alias de jogador.

### 5.2 CSVs gerados por liga

Arquivos em `data/<league>/`.

1. `standings.csv`
- Colunas:
  - `tid,tournament_name,start_date,month,player_name,player_id,standing,points,win_rate,opp_win_rate,decklist,commanders,event_rank,event_points,event_players`

2. `tables.csv`
- Colunas:
  - `tid,tournament_name,start_date,round,table,status`

3. `matches.csv`
- Colunas:
  - `tid,tournament_name,start_date,month,round,table,status,winner_id,winner_name,player_id,player_name,is_winner,deck_url,deck_raw,deck_name_pt,deck_name_en,colecao`
- Observacao:
  - Uma linha por jogador por mesa.
  - Uma mesa de 4 jogadores gera 4 linhas.

4. `event_summary.csv`
- Colunas:
  - `tid,player_name,tournament_name,start_date,month,matches,wins,draws,losses,points_match,win_rate,opp_win_rate,event_rank,event_points,event_players`

5. `player.csv`
- Colunas:
  - `tid,tournament_name,start_date,event_day,month,round,table,player_id,player_name,player_deck,opponent_id,opponent_name,opponent_deck,result`
- Observacao:
  - Uma linha por confronto jogador vs oponente na mesma mesa.
  - Em mesa de 4 jogadores, cada jogador gera 3 linhas (12 linhas no total por mesa).

6. `monthly_best.csv`
- Mesma estrutura de `event_summary.csv`, mantendo o melhor evento por jogador por mes.

7. `league_table.csv`
- Colunas:
  - `player_name,league_points,eventos,deck_principal`

8. `deck_stats.csv`
- Colunas:
  - `deck_key,partidas_jogadas,vitorias,empates,jogadores_unicos,eventos,derrotas,win_rate,deck_name_pt,deck_name_en,colecao,deck_url`

9. `unmapped_decks.csv`
- Colunas:
  - `tid,tournament_name,start_date,player_name,deck_url`
- So aparece preenchido quando deck URL nao existe em `Deck_map.csv`.

### 5.3 JSON consumido pelo front: `site.json`

Top-level keys:
- `standings`
- `tables`
- `matches`
- `event_scores`
- `monthly_best`
- `league_table`
- `deck_stats`

Cada lista segue os mesmos campos dos CSVs equivalentes.

### 5.4 Snapshot atual dos dados (2026-03-06)

Contagem abaixo em linhas de arquivo CSV (inclui cabecalho).

### `presencial`
- `standings.csv`: 143 linhas
- `tables.csv`: 75 linhas
- `matches.csv`: 285 linhas
- `event_summary.csv`: 143 linhas
- `player.csv`: 817 linhas
- `monthly_best.csv`: 92 linhas
- `league_table.csv`: 63 linhas
- `deck_stats.csv`: 45 linhas
- `unmapped_decks.csv`: 1 linha (somente cabecalho)

### `online`
- `standings.csv`: 57 linhas
- `tables.csv`: 33 linhas
- `matches.csv`: 113 linhas
- `event_summary.csv`: 57 linhas
- `player.csv`: 289 linhas
- `monthly_best.csv`: 44 linhas
- `league_table.csv`: 30 linhas
- `deck_stats.csv`: 34 linhas
- `unmapped_decks.csv`: 1 linha (somente cabecalho)

Raw disponivel:
- `data/raw/presencial`: 5 arquivos JSON
- `data/raw/online`: 3 arquivos JSON

## 6) Regras de negocio (implementacao real)

Referencia principal:
- `scripts/sync_topdeck.py`

### 6.1 Normalizacao e enriquecimento

- Datas unix em segundos/ms sao convertidas para `YYYY-MM-DD`.
- Mes e `YYYY-MM`.
- URLs de deck sao canonicalizadas:
  - forca `https`
  - remove query/fragment
  - remove `www.`
  - remove `/` final
- Deck pode vir de varios campos (`decklist`, `metadata.importedFrom`, `deckObj.metadata.importedFrom`).
- Alias de jogador (quando `player_aliases.json` existe) e aplicado em standings e matches.

### 6.2 Definicao de empate

Empate quando qualquer uma for verdadeira:
- `winner_id == "draw"`
- `winner_name == "draw"`
- `status` contem `draw`
- `status` contem `empate`
- `status` casa regex `\bid\b`

### 6.3 Pontuacao por partida

- `points_match = wins*3 + draws*1`
- `wins` vem de `is_winner`

### 6.4 Win rate e Opp win rate por evento

- `win_rate = wins / matches`
- `opp_win_rate`:
  - para cada jogador, percorre cada mesa do evento
  - soma `win_rate` dos adversarios na mesma mesa
  - divide pelo numero de ocorrencias de adversarios

### 6.5 Ranking de evento e event_points

Ordenacao por evento:
1. `points_match` (desc)
2. `win_rate` (desc)
3. `opp_win_rate` (desc)

Empates recebem rank denso (dense rank):
- se valores de desempate forem iguais, mesmo rank
- proximo rank incrementa em 1

Regra de `event_points` implementada no codigo:
- rank 1 -> 3 pontos
- se `player_count <= 12` -> ranks 2-4 recebem 2; resto 1
- se `player_count <= 24` -> ranks 2-8 recebem 2; resto 1
- se `player_count > 24` -> ranks 2-16 recebem 2; resto 1

### 6.6 Liga trimestral (backend)

Referencia:
- `scripts/sync_topdeck.py:607`

Processo por jogador e trimestre:
1. Se tiver menos de 4 eventos no trimestre:
- mantem todos

2. Se tiver 4+ eventos:
- preserva melhor evento de cada mes do trimestre
- dos eventos restantes, descarta os 3 piores

3. Soma `event_points` dos eventos mantidos -> `league_points`

Ordem para achar "piores":
1. `event_points` asc
2. `points_match` asc
3. `win_rate` asc
4. `opp_win_rate` asc
5. `start_date` asc

### 6.7 Deck principal por jogador (backend)

- Deck principal = deck mais jogado pelo jogador em `matches`.
- Chave usada:
  - `deck_name_pt` se existir
  - senao `deck_url`

### 6.8 Estatistica por deck (backend)

Por `deck_key`:
- `partidas_jogadas = total de linhas`
- `vitorias = soma is_winner`
- `empates = regra de draw`
- `derrotas = partidas_jogadas - vitorias - empates`
- `win_rate = vitorias / partidas_jogadas`
- `jogadores_unicos = distinct player_name`
- `eventos = distinct tid`

### 6.9 Matchups jogador vs jogador (`player.csv`)

- Gerado a partir de `matches.csv`.
- Para cada jogador em cada mesa, gera uma linha para cada oponente da mesma mesa.
- `result` pode ser:
  - `win`
  - `lose`
  - `draw`
- `player_deck` e `opponent_deck` usam preferencia:
  - `deck_name_pt`
  - `deck_name_en`
  - `deck_url`

### 6.10 Deck banido (frontend)

Em `decks.html` e `decks-banidos.html`:
- `partidas_jogadas >= 24`
- `win_rate >= 1/3`

## 7) Divisao de responsabilidades por pagina

### 7.1 `site/index.html`

- Landing page.
- So escolhe liga e redireciona com query string.

### 7.2 `site/players.html`

- Ranking geral com filtros de evento.
- Recalcula ranking no front com:
  - pontos = soma dos melhores `event_points` por mes
  - vitorias / winrate a partir de `matches`
  - opp WR medio ponderado por `matches` em `event_scores`
- Link para detalhe do jogador (`player.html?name=...`).

### 7.3 `site/liga.html`

- Ranking trimestral no front.
- Descobre ano e trimestre pela data mais recente.
- Filtro por trimestre + filtro por evento.
- Recalcula ranking com base em `matches` + `event_scores`.

### 7.4 `site/player.html`

- Detalhe individual:
  - vitorias, derrotas, empates, winrate, opp WR
  - pontos por evento
  - pontos por mes
  - head-to-head (contra quem jogou, win/loss/wr)
  - decks mais jogados e melhor desempenho

### 7.5 `site/decks.html`

- Tabela de desempenho de decks.
- Usa `deck_stats` quando existe; fallback recalcula a partir de `matches`.
- Suporta filtro por evento.
- Exibe lista de decks banidos.

### 7.6 `site/decks-banidos.html`

- Versao focada apenas em decks banidos.
- Mesma regra de banimento.

### 7.7 `site/regras.html` e `site/privacidade.html`

- Conteudo estatico.
- Mantem parametro `league` nos links internos.

## 8) Diferencas e pontos de atencao importantes

1. Divergencia README vs codigo em pontuacao de evento:
- README descreve faixa 25-36 com TOP12.
- Codigo aplica TOP16 para qualquer evento com mais de 24 jogadores.

2. Frontend vs backend para pontos de liga:
- Backend (`league_table.csv`) aplica descarte trimestral completo.
- Players/Liga no front recalculam pontos em logica simplificada (best-of-month), entao podem divergir.

3. Encoding de `Deck_map.csv`:
- Cabecalho esperado no codigo usa `ColeÃ§Ã£o`.
- Alterar esse nome pode quebrar o mapping.

4. Nome de arquivo do deck map:
- Codigo abre `deck_map.csv`.
- No projeto atual o arquivo esta como `Deck_map.csv`.
- Em Windows funciona por case-insensitive; em Linux vale padronizar nome/case.

5. Segredo:
- `topdeck_key.txt` existe localmente.
- Nao comitar chave em repositorios publicos.

6. Alias de jogador:
- `player_aliases.json` e opcional e nao esta presente neste snapshot.

7. Deduplicacao de partidas no frontend:
- Varias metricas contam mesa unica por `tid::round::table` para evitar inflar numeros.

## 9) Como portar para outro site

### 9.1 Estrategia recomendada

1. Manter pipeline Python igual (ou portar para backend novo).
2. Publicar `data/<league>/site.json` em rota acessivel pelo novo site.
3. Reimplementar UI usando o contrato de `site.json`.
4. Manter query `league` para separar online/presencial.
5. Rodar sync periodico (ex.: cron no servidor).

### 9.2 Integracao minima (somente consumo de dados)

- Requisicao:
  - `GET /data/<league>/site.json`
- Campos minimos para ranking:
  - `matches`, `event_scores`, `league_table`, `deck_stats`

### 9.3 Validacao pos-migracao

Checklist:
1. `standings.csv` e `event_summary.csv` tem mesmo numero de linhas por evento.
2. `matches.csv` bate com numero de jogadores por mesa x mesas.
3. `league_table.csv` nao vazio para ligas com eventos.
4. Top 3 de players/decks no novo site bate com o site atual.
5. Lista de banidos bate com regra `24+` e `wr>=33.33%`.

## 10) Arquivos de apoio

- `docs/AI_HANDOFF.md` (este documento)
- `docs/site_data_contract.json` (contrato machine-readable; versao 2026-03-05)
- `docs/PROMPT_OUTRA_IA.md` (prompt pronto para outra IA)
