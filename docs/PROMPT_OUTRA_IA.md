# Prompt pronto para outra IA

Copie e cole o texto abaixo na outra IA.

---

Voce vai migrar a pagina da Liga Preconauta para dentro de outro site, mantendo a logica atual.

Use estes arquivos como fonte unica de verdade:
- `docs/AI_HANDOFF.md`
- `docs/site_data_contract.json`
- `scripts/sync_topdeck.py`
- `site/liga.html`
- `site/players.html`
- `site/player.html`
- `site/decks.html`
- `site/decks-banidos.html`

Objetivo:
1. Reproduzir a estrutura de dados (Topdeck -> raw -> CSV -> site.json).
2. Reproduzir a logica de ranking, pontuacao, win rate, opp win rate, filtro de eventos e decks banidos.
3. Entregar uma pagina integrada ao novo site que leia `site.json` por liga (`presencial` ou `online`).

Regras obrigatorias:
1. Nao inventar formulas. Siga exatamente as formulas documentadas.
2. Nao quebrar o contrato do `site.json`.
3. Tratar empate exatamente como no projeto atual.
4. Suportar `?league=presencial|online`.
5. Manter compatibilidade com os campos atuais dos CSVs e JSON.

Checklist tecnico de implementacao:
1. Configurar variavel `TOPDECK_API_KEY`.
2. Ler `events.json` com `tid` e `league`.
3. Consumir Topdeck pelos endpoints documentados.
4. Gerar os mesmos arquivos por liga:
   - `standings.csv`
   - `tables.csv`
   - `matches.csv`
   - `event_summary.csv`
   - `monthly_best.csv`
   - `league_table.csv`
   - `deck_stats.csv`
   - `unmapped_decks.csv`
   - `site.json`
5. Implementar pagina que renderiza ranking/jogadores/decks via `site.json`.
6. Validar top 3 players, top 3 decks e decks banidos contra os dados atuais.

Pontos de atencao:
1. Existe divergencia entre texto do README e codigo de pontuacao por faixa de jogadores. Confie no codigo.
2. `Deck_map.csv` usa cabecalho legado `ColeÃ§Ã£o`.
3. Ranking de liga no backend usa descarte trimestral; alguns componentes no front usam logica simplificada.

Entregaveis esperados:
1. Codigo da pagina no novo site.
2. Pipeline de sync funcionando.
3. Documento curto de validacao com:
   - exemplos de entradas
   - saidas geradas
   - comparacao de ranking com o projeto original.

---

