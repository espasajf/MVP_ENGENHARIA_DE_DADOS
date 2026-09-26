# Documentação — Arquitetura Medalhão Pescador

> **Catálogo:** `pescador`
> **Workspace:** Databricks (dbc-1c744d47-d06f)
> **Data de geração:** 26/09/2026
> **Volume de dados:** /Volumes/pescador/staging/dados_rgp/
> **Repositório:** https://github.com/espasajf/MVP_ENGENHARIA_DE_DADOS

---

## Sumário

1. [Visão Geral](#1-visão-geral)
2. [Arquitetura Medalhão](#2-arquitetura-medalhão)
3. [Notebook MVP00 — Prepara Ambiente](#3-notebook-mvp00--prepara-ambiente)
4. [Notebook MVP01 — Importar Municípios](#4-notebook-mvp01--importar-municípios)
5. [Notebook MVP02 — Importação Info RGP](#5-notebook-mvp02--importação-info-rgp)
6. [Notebook MVP03 — Importação SeguroDefeso](#6-notebook-mvp03--importação-segurodefeso)
7. [Notebook MVP04 — Importação Info Censo IBGE](#7-notebook-mvp04--importação-info-censo-ibge)
8. [Notebook MVP05 — Análise da Qualidade dos Dados](#8-notebook-mvp05--análise-da-qualidade-dos-dados)
9. [Notebook MVP06 — Gold Resultado Final](#9-notebook-mvp06--gold-resultado-final)
10. [Dicionário de Tabelas](#10-dicionário-de-tabelas)
11. [Fluxo de Dados](#11-fluxo-de-dados)
12. [Resumo de Transformações](#12-resumo-de-transformações)

---

## 1. Visão Geral

Este projeto implementa uma arquitetura medalhão (bronze/silver/gold) no catálogo `pescador` do Databricks para processar dados do Registro Geral da Pesca (RGP) e do Seguro Defeso. Os dados são importados de arquivos Excel (.xls, .xlsx) e CSV armazenados em um UC Volume, processados em camadas consecutivas com qualidade crescente.

**Arquivos de origem (41 no total):**
- 2 arquivos Excel de municípios IBGE (.xls e .xlsx)
- 26 arquivos Excel de pescadores por UF (.xlsx)
- 13 arquivos CSV de seguro defeso por competência

**Tabelas criadas (14 no total):**
- 7 tabelas na camada bronze
- 5 tabelas na camada silver
- 2 tabelas na camada gold

**Notebooks (7 no total):**
- MVP00 — Prepara Ambiente (cria catálogo e schemas)
- MVP01 — Importar Municípios (IBGE)
- MVP02 — Importação Info RGP (pescadores)
- MVP03 — Importação SeguroDefeso (seguro defeso)
- MVP04 — Importação Info Censo IBGE (censo 2022)
- MVP05 — Análise da Qualidade dos Dados
- MVP06 — Gold Resultado Final (tabelas gold)

---

## 2. Arquitetura Medalhão

```
ARQUIVOS                     BRONZE                          SILVER                           GOLD
─────────                    ──────                          ──────                           ────

.xls DTB Brasil ────────► RELATORIO_DTB_BRASIL_MUNICIPIO ┐
                          (5.570 regs)                    ├──► MUNICIPIOS ─────────────┐
.xlsx Codigos UF ───────► CODIGOS_UF_IBGE                │    (5.570 regs)             │
                          (27 regs)                      └──► MUNICIPIOS_BR ──────────┤
                                                             (5.570 regs)              │
                                                                                        ├──► RESULTADO1
26× .xlsx Pescadores ──► pescadores_AC..TO (26 tabelas) ──► pescadores_BR ──────► pescadores ──┤  (3.430 municípios)
                          (1.562.042 regs)               (1.501.795 regs)              │
                                                        dedup CPF + sem nulos          ├──► RESULTADO_2
                                                                                       │   (2.250.427 regs)
.csv Censo IBGE ────────► IBGE_CENSO_2022_MUNICIPIO ─────► CENSO_IBGE ─────────────────┘
                          (5.570 regs)                  (5.570 regs)                    join CPF
                                                                                       seguro×pescador
13× .csv SeguroDefeso ► seguro_defeso_202503..202607 ────► SEGURO_DEFESO_2025_2026 ► seguro_defeso
                          (5.845.058 regs)               (2.651.116 regs)
                                                        sem CPF nulo
```

| Camada | Função | Transformações aplicadas |
| --- | --- | --- |
| **Bronze** | Dados brutos, preserva tudo | Sanitização de colunas, adição de UF de origem, consolidação |
| **Silver** | Dados limpos e deduplicados | Remoção de nulos, deduplicação de CPF, seleção de colunas, joins |
| **Gold** | Agregações e regras de negócio | Joins entre silvers, agregações por município, cruzamento CPF |

---

## 3. Notebook MVP00 — Prepara Ambiente

- **ID:** 559335713768810
- **Caminho:** Notebooks_MVP/MVP00 - Prepara Ambiente
- **Objetivo:** Criar o catálogo `pescador` e os schemas necessários para a arquitetura medalhão.

### Etapas

| Célula | Comando SQL | Descrição |
| --- | --- | --- |
| 1 | `DROP CATALOG IF EXISTS pescador CASCADE` | Remove catálogo existente (se houver) |
| 2 | `CREATE CATALOG pescador` | Cria o catálogo pescador |
| 3 | `USE CATALOG pescador` | Define o catálogo ativo |
| 4 | `DROP SCHEMA IF EXISTS staging CASCADE` | Remove schema staging existente |
| 5 | `CREATE SCHEMA staging` | Cria schema staging (para UC Volume) |
| 6 | `DROP/CREATE SCHEMA layer_bronze, layer_silver, layer_gold` | Cria os 3 schemas da arquitetura medalhão |

### Estrutura criada

```
pescador/
├── staging/        (UC Volume: /Volumes/pescador/staging/dados_rgp/)
├── layer_bronze/   (dados brutos)
├── layer_silver/   (dados limpos e deduplicados)
└── layer_gold/     (agregações e regras de negócio)
```

---

## 4. Notebook MVP01 — Importar Municípios

- **ID:** 4353004639350821
- **Caminho:** Notebooks_MVP/MVP01 - Importar Municípios
- **Objetivo:** Importar dados de municípios brasileiros do IBGE e criar dimensões de município para cruzamento com tabelas operacionais.

### Etapas

#### Etapa 1 — Importação Relatório DTB Brasil Município

Carrega o arquivo `/Volumes/pescador/staging/dados_rgp/Dados/RELATORIO_DTB_BRASIL_MUNICIPIO.xls` para a tabela bronze `pescador.layer_bronze.RELATORIO_DTB_BRASIL_MUNICIPIO`.

- Sanitiza nomes de colunas (remove acentos, espaços e caracteres especiais)
- 5.570 registros, 9 colunas

#### Etapa 2 — Importação Códigos UF IBGE

Carrega o arquivo `/Volumes/pescador/staging/dados_rgp/Dados/codigos_uf_ibge.xlsx` para a tabela bronze `pescador.layer_bronze.CODIGOS_UF_IBGE`.

- 27 registros (26 estados + DF), 3 colunas
- Colunas já sanitizadas: Codigo_UF, Sigla_UF, Nome_UF

#### Etapa 3 — Criação da tabela Silver MUNICIPIOS

Join entre as duas tabelas bronze (RELATORIO_DTB_BRASIL_MUNICIPIO × CODIGOS_UF_IBGE pelo código da UF).

- Colunas: SIGLA_UF, NM_MUNICIPIO, CODIGO_MUNICIPIO
- 5.570 registros

#### Etapa 4 — Criação da tabela Silver MUNICIPIOS_BR

Join das mesmas tabelas bronze, com seleção de colunas diferente.

- Colunas: CODIGO_MUNICIPIO, SIGLA_UF, NOME_MUNICIPIO
- 5.570 registros

### Tabelas criadas

| Camada | Tabela | Registros | Colunas | Descrição |
| --- | --- | --- | --- | --- |
| Bronze | `pescador.layer_bronze.RELATORIO_DTB_BRASIL_MUNICIPIO` | 5.570 | 9 | Dados do DTB Brasil Município |
| Bronze | `pescador.layer_bronze.CODIGOS_UF_IBGE` | 27 | 3 | Códigos UF IBGE |
| Silver | `pescador.layer_silver.MUNICIPIOS` | 5.570 | 3 | Municípios (SIGLA_UF, NM_MUNICIPIO, CODIGO_MUNICIPIO) |
| Silver | `pescador.layer_silver.MUNICIPIOS_BR` | 5.570 | 3 | Municípios BR (CODIGO_MUNICIPIO, SIGLA_UF, NOME_MUNICIPIO) |

### Schema — RELATORIO_DTB_BRASIL_MUNICIPIO

| Coluna | Tipo |
| --- | --- |
| UF | long |
| Nome_UF | string |
| Regiao_Geografica_Intermediaria | long |
| Nome_Regiao_Geografica_Intermediaria | string |
| Regiao_Geografica_Imediata | long |
| Nome_Regiao_Geografica_Imediata | string |
| Municipio | long |
| Codigo_Municipio_Completo | long |
| Nome_Municipio | string |

### Schema — CODIGOS_UF_IBGE

| Coluna | Tipo |
| --- | --- |
| Codigo_UF | long |
| Sigla_UF | string |
| Nome_UF | string |

---

## 5. Notebook MVP02 — Importação Info RGP

- **ID:** 2765210736726162
- **Caminho:** Notebooks_MVP/MVP02 - Importacao Info RGP
- **Objetivo:** Importar dados de pescadores do RGP (26 arquivos Excel, um por UF) e criar tabela bronze consolidada e tabela silver deduplicada.

### Etapas

#### Etapa 1 — Leitura dos 26 arquivos Excel por UF

Lê cada arquivo `.xlsx` do diretório `/Volumes/pescador/staging/dados_rgp/`, extrai a UF do nome do arquivo, sanitiza colunas e cria uma tabela bronze por UF.

- 26 tabelas bronze: `pescador.layer_bronze.pescadores_AC` até `pescador.layer_bronze.pescadores_TO`
- Cada tabela tem 13 colunas + coluna UF (adicionada do nome do arquivo)

#### Etapa 2 — Consolidação na tabela Bronze pescadores_BR

Une os 26 DataFrames via `unionByName` e adiciona o campo `CPF_PRIMEIRO_NOME` (parte numérica do CPF + primeiro nome).

- 1.562.042 registros, 14 colunas
- Salva em `pescador.layer_bronze.pescadores_BR`

#### Etapa 3 — Criação da tabela Silver pescadores

A partir da tabela bronze `pescadores_BR`, aplica:
1. **Remoção de CPFs nulos** — elimina registros sem CPF identificado
2. **Deduplicação completa** — elimina TODOS os registros cujo `CPF_PRIMEIRO_NOME` aparece mais de uma vez (nenhuma ocorrência é mantida)
3. **Seleção de colunas** — mantém apenas 5 colunas analíticas

- 1.501.795 registros (60.247 registros removidos), 5 colunas
- Salva em `pescador.layer_silver.pescadores`

### Tabelas criadas

| Camada | Tabela | Registros | Colunas | Descrição |
| --- | --- | --- | --- | --- |
| Bronze | `pescador.layer_bronze.pescadores_{UF}` (×26) | 1.562.042 | 13+1 | Pescadores por UF (raw, uma tabela por estado) |
| Bronze | `pescador.layer_bronze.pescadores_BR` | 1.562.042 | 14 | Pescadores Brasil consolidado |
| Silver | `pescador.layer_silver.pescadores` | 1.501.795 | 5 | Pescadores deduplicados (CPF único e não nulo) |

### Schema — pescadores_BR (bronze)

| Coluna | Tipo |
| --- | --- |
| UF | string |
| CPF | string |
| Nome_do_Pescador | string |
| Dt_Nascimento | long |
| Nivel_Escolaridade | string |
| Tipo_Alfabetizacao | string |
| Sexo | string |
| Categoria | string |
| Forma_de_Atuacao | string |
| UF_ESTADO | string |
| Municipio | string |
| Codigo_IBGE | long |
| UF_NOME | string |
| CPF_PRIMEIRO_NOME | string |

### Schema — pescadores (silver)

| Coluna | Tipo |
| --- | --- |
| CPF_PRIMEIRO_NOME | string |
| UF | string |
| Municipio | string |
| Categoria | string |
| Forma_de_Atuacao | string |

### Registros por UF (bronze pescadores_BR)

| UF | Registros | UF | Registros |
| --- | --- | --- | --- |
| AC | 21.350 | PA | 435.023 |
| AL | 32.219 | PB | 59.135 |
| AM | 147.823 | PE | 25.361 |
| AP | 28.921 | PI | 79.218 |
| CE | 34.256 | PR | 14.308 |
| DF | 834 | RJ | 20.650 |
| ES | 46.745 | RN | 26.933 |
| GO | 3.609 | RO | 12.229 |
| MA | 337.280 | RR | 10.064 |
| MG | 37.468 | RS | 22.964 |
| MS | 8.278 | SC | 32.118 |
| MT | 18.477 | SE | 48.481 |
| | | SP | 48.816 |
| | | TO | 9.482 |

---

## 6. Notebook MVP03 — Importação SeguroDefeso

- **ID:** 2765210736726165
- **Caminho:** Notebooks_MVP/MVP03 - Importacao SeguroDefeso
- **Objetivo:** Importar dados do Seguro Defeso (13 arquivos CSV por competência) e criar tabela bronze consolidada e tabela silver filtrada.

### Etapas

#### Etapa 1 — Leitura dos 13 arquivos CSV por competência

Lê cada arquivo `.csv` do diretório `/Volumes/pescador/staging/dados_rgp/`, sanitiza colunas e cria uma tabela bronze por competência (mês de referência).

- 13 tabelas bronze: `pescador.layer_bronze.seguro_defeso_202503` até `pescador.layer_bronze.seguro_defeso_202607`
- Cada tabela tem 9 colunas originais

#### Etapa 2 — Consolidação na tabela Bronze SEGURO_DEFESO_2025_2026

Une os 13 DataFrames, adiciona o campo `CPF_PRIMEIRO_NOME` e seleciona apenas as colunas analíticas relevantes.

- 5.845.058 registros, 5 colunas
- Salva em `pescador.layer_bronze.SEGURO_DEFESO_2025_2026`

#### Etapa 3 — Análise da tabela bronze

Inclui análise exploratória com:
- Visão geral (registros, CPFs distintos, nulos, municípios, UFs, competências)
- Estatísticas de VALOR_PARCELA (min, max, avg, soma total)
- Distribuição por UF (top 15)
- Distribuição por mês de referência
- Top 10 municípios por valor total pago
- Valores anômalos (VALOR_PARCELA ≤ 0): 28.621 registros
- Valores nulos por coluna: CPF_PRIMEIRO_NOME tem 54,6% de nulos (3.193.942 registros)

#### Etapa 4 — Criação da tabela Silver seguro_defeso

A partir da tabela bronze `SEGURO_DEFESO_2025_2026`, remove registros onde `CPF_PRIMEIRO_NOME` é nulo.

- 2.651.116 registros (3.193.942 removidos), 5 colunas
- Salva em `pescador.layer_silver.seguro_defeso`

### Tabelas criadas

| Camada | Tabela | Registros | Colunas | Descrição |
| --- | --- | --- | --- | --- |
| Bronze | `pescador.layer_bronze.seguro_defeso_{AAAAMM}` (×13) | 5.845.058 | 9 | Seguro defeso por competência (raw) |
| Bronze | `pescador.layer_bronze.SEGURO_DEFESO_2025_2026` | 5.845.058 | 5 | Seguro defeso consolidado |
| Silver | `pescador.layer_silver.seguro_defeso` | 2.651.116 | 5 | Seguro defeso sem CPF nulo |

### Schema — SEGURO_DEFESO_2025_2026 (bronze) e seguro_defeso (silver)

| Coluna | Tipo |
| --- | --- |
| MES_REFERENCIA | long |
| UF | string |
| NOME_MUNICIPIO | string |
| CPF_PRIMEIRO_NOME | string |
| VALOR_PARCELA | double |

### Arquivos processados

| Arquivo | Registros | Tamanho |
| --- | --- | --- |
| 202503_SeguroDefeso.csv | 1.037.584 | 109,1 MB |
| 202504_SeguroDefeso.csv | 675.306 | 71,0 MB |
| 202506_SeguroDefeso.csv | 462.575 | 48,0 MB |
| 202507_SeguroDefeso.csv | 213.198 | 22,1 MB |
| 202508_SeguroDefeso.csv | 169.879 | 17,6 MB |
| 202509_SeguroDefeso.csv | 40.676 | 4,3 MB |
| 202510_SeguroDefeso.csv | 46 | 0,0 MB |
| 202602_SeguroDefeso.csv | 62.065 | 6,5 MB |
| 202603_SeguroDefeso.csv | 322.345 | 33,8 MB |
| 202604_SeguroDefeso.csv | 535.657 | 56,2 MB |
| 202605_SeguroDefeso.csv | 653.584 | 68,6 MB |
| 202606_SeguroDefeso.csv | 704.791 | 73,8 MB |
| 202607_SeguroDefeso.csv | 967.352 | 101,0 MB |

### Estatísticas de VALOR_PARCELA

| Métrica | Valor |
| --- | --- |
| Mínimo | -R$ 2.836,64 |
| Máximo | R$ 5.672,00 |
| Média | R$ 1.563,22 |
| Soma total | R$ 9,14 bilhões |
| Registros ≤ 0 (anomalias) | 28.621 |
| Registros com CPF nulo | 3.193.942 (54,6%) |

### Top 5 UFs por volume de parcelas

| UF | Parcelas | CPFs distintos | Valor total |
| --- | --- | --- | --- |
| MA | 1.738.007 | 190.428 | R$ 2,70 bi |
| PA | 1.692.352 | 178.765 | R$ 2,64 bi |
| BA | 516.534 | 45.371 | R$ 811 mi |
| AM | 395.353 | 55.244 | R$ 618 mi |
| PI | 251.059 | 22.219 | R$ 395 mi |

---

## 7. Notebook MVP04 — Importação Info Censo IBGE

- **ID:** 2765210736726166
- **Caminho:** Notebooks_MVP/MVP04 - Importação Info Censo IBGE
- **Objetivo:** Importar dados do Censo IBGE 2022 por município e criar tabela silver com população.

### Etapas

#### Etapa 1 — Importação Censo IBGE 2022

Carrega o arquivo `/Volumes/pescador/staging/dados_rgp/Dados/br_ibge_censo_2022_municipio.csv` para a tabela bronze `pescador.layer_bronze.IBGE_CENSO_2022_MUNICIPIO`.

- Encoding: latin-1, separador vírgula
- 5.570 registros, 13 colunas

#### Etapa 2 — Criação da tabela Silver CENSO_IBGE

Seleciona apenas código do município e população da tabela bronze.

- Colunas: CODIGO_MUNICIPIO, POPULACAO
- 5.570 registros

### Tabelas criadas

| Camada | Tabela | Registros | Colunas | Descrição |
| --- | --- | --- | --- | --- |
| Bronze | `pescador.layer_bronze.IBGE_CENSO_2022_MUNICIPIO` | 5.570 | 13 | Dados do Censo IBGE 2022 por município |
| Silver | `pescador.layer_silver.CENSO_IBGE` | 5.570 | 2 | População por município (CODIGO_MUNICIPIO, POPULACAO) |

### Schema — IBGE_CENSO_2022_MUNICIPIO (bronze)

| Coluna | Tipo |
| --- | --- |
| id_municipio | long |
| sigla_uf | string |
| domicilios | long |
| populacao | long |
| area | long |
| taxa_alfabetizacao | double |
| idade_mediana | long |
| razao_sexo | double |
| indice_envelhecimento | double |
| populacao_indigena | long |
| populacao_indigena_terra_indigena | long |
| populacao_quilombola | long |
| populacao_quilombola_territorio_quilombola | long |

### Schema — CENSO_IBGE (silver)

| Coluna | Tipo |
| --- | --- |
| CODIGO_MUNICIPIO | long |
| POPULACAO | long |

---

## 8. Notebook MVP05 — Análise da Qualidade dos Dados

- **ID:** 2765210736726167
- **Caminho:** Notebooks_MVP/MVP05 - Análise da qualidade dos dados
- **Objetivo:** Análise de qualidade das tabelas silver: pescadores e seguro_defeso. Verifica registros, nulos, duplicidades, integridade referencial e domínios.

### Etapas

#### Etapa 1 — Visão geral das tabelas

| Tabela | Registros | Colunas |
| --- | --- | --- |
| pescador.layer_silver.pescadores | 1.562.042 | 14 |
| pescador.layer_silver.seguro_defeso | 5.845.058 | 10 |

#### Etapa 2 — Análise de valores nulos (pescadores)

| Coluna | Nulos | % |
| --- | --- | --- |
| UF | 0 | 0,0% |
| CPF | 0 | 0,0% |
| Nome_do_Pescador | 3.478 | 0,2% |
| Nivel_Escolaridade | 2.971 | 0,2% |
| Tipo_Alfabetizacao | 2.971 | 0,2% |
| Sexo | 0 | 0,0% |
| Demais colunas | 0 | 0,0% |

#### Etapa 3 — Análise de valores nulos (seguro_defeso)

| Coluna | Nulos | % |
| --- | --- | --- |
| CPF | 3.193.942 | **54,6%** |
| Demais colunas | 0 | 0,0% |

#### Etapa 4 — Duplicidades (pescadores)

| Métrica | Valor |
| --- | --- |
| Total de registros | 1.562.042 |
| Combinações distintas (UF+CPF) | 843.931 |
| Duplicados | 718.111 (46,0%) |

#### Etapa 5 — Domínios categóricos

- UF: 26 valores distintos
- Sexo: Masculino, Feminino
- Categoria: ARTESANAL, ...
- Forma_de_Atuacao: DESEMBARCADO, ...

#### Etapa 6 — Integridade referencial

- Registros do seguro_defeso com CPF nulo: 3.193.942
- CPFs distintos no seguro_defeso: 310.717
- CPFs não encontrados em pescadores: verificados

#### Etapa 7 — Anomalias em Dt_Nascimento

Verificação de anos inválidos na data de nascimento dos pescadores.

---

## 9. Notebook MVP06 — Gold Resultado Final

- **ID:** 4402098238455338
- **Caminho:** Notebooks_MVP/MVP06 - Gold Resultado Final
- **Objetivo:** Criar tabelas gold com join de seguro_defeso, municipios_br, censo_ibge e pescadores.

### Etapas

#### Etapa 1 — Gold RESULTADO1 (Seguro Defeso × Municípios × Censo)

Cria `pescador.layer_gold.RESULTADO1` juntando:
- `seguro_defeso` — parcelas pagas e beneficiários
- `MUNICIPIOS_BR` — código do município (IBGE)
- `CENSO_IBGE` — população do município

Normaliza nomes de municípios (maiúsculas + sem acentos) para o join.

- 3.430 municípios

#### Etapa 2 — Gold RESULTADO_2 (Seguro Defeso × Pescadores por CPF)

Cria `pescador.layer_gold.RESULTADO_2` com join entre `seguro_defeso` e `pescadores` pelo `CPF_PRIMEIRO_NOME`.

- Normaliza formato do CPF (remove underscore de pescadores para matching)
- 2.250.427 registros
- Identifica CPFs com UF e município divergentes entre seguro e pescadores: **18.298 CPFs**

### Tabelas criadas

| Camada | Tabela | Registros | Colunas | Descrição |
| --- | --- | --- | --- | --- |
| Gold | `pescador.layer_gold.RESULTADO1` | 3.430 | 6 | Parcelas, valor total, beneficiários e população por município |
| Gold | `pescador.layer_gold.RESULTADO_2` | 2.250.427 | 5 | Cruzamento seguro_defeso × pescadores por CPF |

### Schema — RESULTADO1 (gold)

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| NM_MUNICIPIO | string | Nome do município |
| UF | string | Sigla da UF |
| QTD_PARCELAS | long | Quantidade de parcelas pagas |
| VALOR_TOTAL_PAGO | double | Soma de todas as parcelas |
| QTD_BENEFICIARIOS | long | Beneficiários distintos (count distinct CPF) |
| POPULACAO | long | População do município (Censo 2022) |

### Schema — RESULTADO_2 (gold)

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| CPF_PRIMEIRO_NOME | string | Chave de join (CPF + primeiro nome) |
| UF_SEGURO | string | UF do seguro defeso |
| NM_MUNICIPIO_SEGURO | string | Município do seguro defeso |
| UF_PESCADOR | string | UF do pescador |
| NM_MUNICIPIO_PESCADOR | string | Município do pescador |

### Taxa de beneficiados por UF (RESULTADO1)

| UF | Beneficiários | População | Taxa |
| --- | --- | --- | --- |
| MA | 196.459 | 6.738.476 | 2,92% |
| PA | 188.639 | 7.990.392 | 2,36% |
| AP | 12.679 | 729.353 | 1,74% |
| AM | 57.029 | 3.929.369 | 1,45% |
| AC | 8.899 | 817.731 | 1,09% |

### Visão geral RESULTADO_2

| Métrica | Valor |
| --- | --- |
| Total de registros | 2.250.427 |
| CPFs distintos | 466.669 |
| UFs (seguro) | 27 |
| UFs (pescador) | 26 |
| Municípios (seguro) | 2.807 |
| Municípios (pescador) | 2.913 |
| Média de parcelas por CPF | 4,8 |

---

## 10. Dicionário de Tabelas

### Camada Bronze

| Tabela | Catálogo.Schema | Registros | Colunas | Arquivo-fonte |
| --- | --- | --- | --- | --- |
| RELATORIO_DTB_BRASIL_MUNICIPIO | pescador.layer_bronze | 5.570 | 9 | RELATORIO_DTB_BRASIL_MUNICIPIO.xls |
| CODIGOS_UF_IBGE | pescador.layer_bronze | 27 | 3 | codigos_uf_ibge.xlsx |
| pescadores_{UF} (×26) | pescador.layer_bronze | 1.562.042 | 13+1 | AC Pescadores.xlsx ... TO Pescadores.xlsx |
| pescadores_BR | pescador.layer_bronze | 1.562.042 | 14 | 26 arquivos .xlsx (consolidado) |
| IBGE_CENSO_2022_MUNICIPIO | pescador.layer_bronze | 5.570 | 13 | br_ibge_censo_2022_municipio.csv |
| seguro_defeso_{AAAAMM} (×13) | pescador.layer_bronze | 5.845.058 | 9 | 13 arquivos .csv |
| SEGURO_DEFESO_2025_2026 | pescador.layer_bronze | 5.845.058 | 5 | 13 arquivos .csv (consolidado) |

### Camada Silver

| Tabela | Catálogo.Schema | Registros | Colunas | Origem |
| --- | --- | --- | --- | --- |
| MUNICIPIOS | pescador.layer_silver | 5.570 | 3 | JOIN RELATORIO_DTB × CODIGOS_UF_IBGE |
| MUNICIPIOS_BR | pescador.layer_silver | 5.570 | 3 | JOIN RELATORIO_DTB × CODIGOS_UF_IBGE |
| pescadores | pescador.layer_silver | 1.501.795 | 5 | pescadores_BR (dedup CPF + sem nulos) |
| seguro_defeso | pescador.layer_silver | 2.651.116 | 5 | SEGURO_DEFESO_2025_2026 (sem CPF nulo) |
| CENSO_IBGE | pescador.layer_silver | 5.570 | 2 | IBGE_CENSO_2022_MUNICIPIO (seleção de colunas) |

### Camada Gold

| Tabela | Catálogo.Schema | Registros | Colunas | Origem |
| --- | --- | --- | --- | --- |
| RESULTADO1 | pescador.layer_gold | 3.430 | 6 | JOIN seguro_defeso × MUNICIPIOS_BR × CENSO_IBGE |
| RESULTADO_2 | pescador.layer_gold | 2.250.427 | 5 | JOIN seguro_defeso × pescadores (CPF_PRIMEIRO_NOME) |

---

## 11. Fluxo de Dados

```
[Volumes/pescador/staging/dados_rgp/Dados/]
  │
  ├── RELATORIO_DTB_BRASIL_MUNICIPIO.xls ──────► [Bronze] RELATORIO_DTB_BRASIL_MUNICIPIO ──┐
  │                                                                                       ├──► [Silver] MUNICIPIOS
  ├── codigos_uf_ibge.xlsx ─────────────────────► [Bronze] CODIGOS_UF_IBGE ───────────────┘──► [Silver] MUNICIPIOS_BR
  │
  ├── AC Pescadores.xlsx ───────────────────────► [Bronze] pescadores_AC ─────────────┐
  ├── AL Pescadores.xlsx ───────────────────────► [Bronze] pescadores_AL ─────────────┤
  ├── ... (26 UFs) ──────────────────────────────────────────────────────────────────┤
  ├── TO Pescadores.xlsx ───────────────────────► [Bronze] pescadores_TO ─────────────┘
  │                                                    │
  │                                              unionByName + CPF_PRIMEIRO_NOME
  │                                                    ▼
  │                                           [Bronze] pescadores_BR (1.562.042 regs)
  │                                                    │
  │                                              remove nulos + dedup CPF
  │                                                    ▼
  │                                           [Silver] pescadores (1.501.795 regs)
  │
  ├── 202503_SeguroDefeso.csv ──────────────────► [Bronze] seguro_defeso_202503 ──────┐
  ├── 202504_SeguroDefeso.csv ──────────────────► [Bronze] seguro_defeso_202504 ──────┤
  ├── ... (13 competências) ────────────────────────────────────────────────────────┤
  ├── 202607_SeguroDefeso.csv ──────────────────► [Bronze] seguro_defeso_202607 ──────┘
  │                                                    │
  │                                              unionByName + CPF_PRIMEIRO_NOME
  │                                                    ▼
  │                                           [Bronze] SEGURO_DEFESO_2025_2026 (5.845.058 regs)
  │                                                    │
  │                                              remove CPF nulo
  │                                                    ▼
  │                                           [Silver] seguro_defeso (2.651.116 regs)
  │
  ├── br_ibge_censo_2022_municipio.csv ─────────► [Bronze] IBGE_CENSO_2022_MUNICIPIO ──► [Silver] CENSO_IBGE ──┐
  │                                             (5.570 regs)                        (5.570 regs)             │
  │                                                                                                        │
  └── [Análise] CPFs distintos: 1.528.779                                                                  │
                                                                                                           │
                                                                                                    [Gold] RESULTADO1
                                                                                                    (3.430 municipios)
                                                                                                    join: seguro×municipios×censo
```

---

## 12. Resumo de Transformações

| Transformação | Notebook | Camada | Descrição |
| --- | --- | --- | --- |
| Criação do catálogo e schemas | MVP00 | N/A | Cria catálogo pescador e schemas staging, layer_bronze, layer_silver, layer_gold |
| Sanitização de colunas | MVP01, MVP02, MVP03, MVP04 | Bronze | Remove acentos, espaços e caracteres especiais dos nomes de colunas |
| Adição de UF de origem | MVP02 | Bronze | Extrai a UF do nome do arquivo e adiciona como primeira coluna |
| Consolidação (unionByName) | MVP02, MVP03 | Bronze | Une múltiplas tabelas por UF/competência em uma tabela consolidada |
| CPF_PRIMEIRO_NOME | MVP02, MVP03 | Bronze | Concatena parte numérica do CPF + primeiro nome do pescador |
| Join | MVP01 | Silver | Join entre tabelas bronze pelo código da UF |
| Seleção de colunas | MVP02, MVP04 | Silver | Mantém apenas colunas analíticas relevantes |
| Remoção de nulos | MVP02, MVP03 | Silver | Remove registros com CPF_PRIMEIRO_NOME nulo |
| Deduplicação completa | MVP02 | Silver | Elimina TODOS os registros cujo CPF aparece mais de uma vez |
| Análise de qualidade | MVP05 | Silver | Verifica nulos, duplicidades, domínios, integridade referencial |
| Join multi-tabela | MVP06 | Gold | Join seguro_defeso × municipios_br × censo_ibge (RESULTADO1) |
| Join por CPF | MVP06 | Gold | Join seguro_defeso × pescadores por CPF_PRIMEIRO_NOME (RESULTADO_2) |
| Normalização de municípios | MVP06 | Gold | Maiúsculas + sem acentos para matching de nomes de municípios |
| Normalização de CPF | MVP06 | Gold | Remove underscore para matching entre seguro_defeso e pescadores |

---

*Documentação atualizada em 26/09/2026.*