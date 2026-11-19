# Databricks: Bronze, Prata e Ouro

Notebooks do lakehouse no **Databricks Free Edition** (catálogo `workspace`, esquema `gs`, Volume `chegada`).
Para importar: *Workspace > Import* e escolha cada arquivo (o Databricks reconhece o formato de notebook pelo cabeçalho).

| Ordem | Notebook | O que faz |
| --- | --- | --- |
| 0 | `00_preparar.sql` | cria esquema, Volume e a tabela `prata_qualidade` |
| 1 | `01_bronze.py` | Auto Loader: um `.jsonl` novo vira linhas na tabela Bronze, sem duplicar |
| 2 | `02_prata.sql` | tipos, nulos e inválidos, regras de qualidade com contagem, `MERGE` pela chave |
| 3 | `03_ouro.sql` | fato e dimensões, LGPD (hash e máscara), tabela do MVP |
| 4 | `04_consultas.sql` | as 5 consultas, uma delas de impacto |

## Passo a passo

1. Rode `00_preparar.sql`.
2. No repositório, rode `python scripts/preparar_lotes.py`. Ele cria `lotes/lote_1` (dias 12 a 14/11) e `lotes/lote_2` (15 e 16/11).
3. **Lote 1**: em *Catalog > workspace > gs > chegada*, crie uma pasta por fonte (`lugares`, `osm_acessibilidade`, `ibge_pcd`, `clima`, `relatos`) e suba para cada uma os arquivos de `lotes/lote_1/<fonte>/`.
4. Rode `01_bronze.py` e tire print da tabela de contagens (`antes` = 0, `novos` = total do lote). Rode `02_prata.sql`.
5. **Lote 2**: suba os arquivos de `lotes/lote_2/<fonte>/` nas mesmas pastas e rode `01_bronze.py` de novo: só os arquivos novos entram (print das contagens antes e depois). Rode `02_prata.sql` de novo.
6. Rode `01_bronze.py` uma terceira vez, sem arquivo novo: `novos = 0` (prova de que não duplica).
7. Rode `03_ouro.sql`. Ele pede o widget `salt_lgpd` (qualquer texto de 8+ caracteres; guarde e **não** commite). Depois rode `04_consultas.sql`.

## Modelo

```text
bronze_*  (texto cru + arquivo_origem + carregado_em)
   v  tipos, qualidade, dedup, MERGE
prata_lugares  prata_relatos  prata_osm  prata_clima  prata_ibge_pcd   (+ prata_relatos_quarentena, prata_qualidade)
   v
ouro_dim_lugar  ouro_dim_necessidade  ouro_dim_tempo  ouro_dim_usuario
ouro_fato_relato  ouro_fato_clima_dia  ouro_osm_cobertura
ouro_mvp_nota_lugar   <- a tabela que o MVP consome
```

- **Fato**: `ouro_fato_relato` (um relato de acessibilidade). Dimensões: lugar, necessidade, tempo e usuário. `ouro_fato_clima_dia` é um segundo fato, ligado a lugar e tempo.
- **Contrato com o MVP**: o cartão do lugar no Inclui+ lê `ouro_mvp_nota_lugar` (uma linha por lugar e necessidade): `nota_media`, `qtd_relatos`, `qtd_barreiras`, `pct_barreiras` e `recursos_faltando` (a lista "o que falta"). Hoje o site lê `places.js`; para a demonstração, exporte a tabela como JSON e use-a no lugar de `PLACES`.

## LGPD

- `autor_nome` não chega à Ouro. `autor_email` vira `usuario_hash` (SHA-256 com sal do widget) e `email_mascarado` (`m***@example.com`).
- O sal não está no repositório. Trocar o sal muda todos os hashes.
- Contatos do OSM (telefone, e-mail, redes) são descartados na Prata: só as tags necessárias passam.
- Os dados pessoais dos relatos são sintéticos.

## Regras de qualidade da Prata

Cada execução grava a contagem de cada regra em `prata_qualidade`. Para os relatos, as contagens conferem exatamente com os defeitos injetados (gabarito no log da tarefa `gerar_relatos`):

| Regra (relatos) | Ação | Total (dias 12 a 16) |
| --- | --- | --- |
| `nota_nao_numerica` | barrado | 3 |
| `nota_fora_da_faixa` | barrado | 1 |
| `lugar_inexistente` | barrado | 1 |
| `email_invalido` | barrado | 1 |
| `sem_data_hora` | barrado | 5 |
| `necessidade_invalida` | barrado | 0 |
| `necessidade_normalizada` (`"MOBILITY "` vira `mobility`) | tratado | 1 |
| `duplicado_removido` | removido | 2 |

Os barrados dos relatos ficam em `prata_relatos_quarentena`, com os motivos.

## As 5 consultas (resultado dos dias 12 a 16/11) e uma interpretação possível

Os relatos são simulados; diga isso no relatório.

1. **Top 3 por necessidade.** Visão: Parque Águas Claras (4,7), Cine Vila (4,4), Livraria Margem (4,3). Mobilidade: Escola Técnica Ponte e Parque Águas Claras (5,0). Cada nota vem de 2 a 6 relatos, então o ranking é frágil; o mínimo de 2 relatos evita nota de caso isolado.
2. **O que falta (mobilidade).** O Cinema do Largo tem 5 relatos e 5 barreiras (100%) e não tem rampa, elevador, banheiro adaptado nem vaga. É a lista que o cartão do lugar mostra como "o que falta".
3. **Chuva e mobilidade.** Em dias de chuva forte (5 mm ou mais), nota média 3,24 e 52,4% de barreiras (21 relatos); nos demais, 4,0 e 33,3% (30 relatos). A diferença não prova causa: o gerador não liga chuva a barreira, então reflete o mix de lugares de cada dia.
4. **Por dia.** 12/11 foi o dia com mais barreiras (66,7%) e 15/11 o com menos (31,3%); 32 a 39 relatos por dia.
5. **Impacto.** São 719 mil pessoas com deficiência em São Paulo (Censo 2022). Há 4 lugares bem avaliados para visão (cerca de 100 mil pessoas por lugar), 8 para mobilidade (33 mil), 3 para cognitiva (47 mil) e 1 para audição (146 mil). O OSM tem 506 lugares acessíveis mapeados e o Inclui+ cobre 20 de 610 (3,3%): a lacuna a atacar é audição e visão, e a expansão do catálogo.

## Como isto foi validado

`02_prata.sql`, `03_ouro.sql` e `04_consultas.sql` foram executados num Spark 3.5 com Delta 3.3 local (container), carregando o lote 1, depois o lote 2 e uma terceira vez sem dado novo: as contagens da Prata ficaram idênticas, sem duplicar. Para repetir depois de mudar o SQL:

```bash
docker build -t gs-sparktest databricks/teste_local
docker run --rm -v "$PWD/chegada:/data:ro" -v "$PWD/databricks:/work/databricks:ro" -v "$PWD/databricks/teste_local:/harness:ro" gs-sparktest python /harness/rodar.py incremental
```

**Não testado no Databricks real:** o `01_bronze.py` (Auto Loader precisa do Volume) e o uso do widget no SQL (`:salt_lgpd`); no teste local o sal foi trocado por texto. Se algo falhar ao importar, o erro costuma ser de caminho do Volume ou de versão do runtime.
