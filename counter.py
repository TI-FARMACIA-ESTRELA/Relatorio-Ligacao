#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Relatório: perdas ajustadas por loja (perdidas totais - atendidas pela Estrela Televendas)
+ abas por loja com o dia a dia das chamadas ATENDIDAS pela Estrela Televendas.

Dependências: pandas, openpyxl
Como rodar: py gera_relatorio_televendas_v3.py
"""

from pathlib import Path
import re
import pandas as pd

# ===================== CONFIG =====================
CSV_PATH = "queue-caller-details_20250901_20250930.csv"
OUT_XLSX = "separacao_contact_center_completo_v3.xlsx"

# Match da fila "Estrela Televendas"
# "contains": aceita variações contendo 'estrela' E 'tele'
# "exact": exige nome exato igual a QUEUE_TARGET
QUEUE_MATCH_MODE = "contains"   # "contains" | "exact"
QUEUE_TARGET = "Estrela Televendas"  # usado se modo "exact"

# Status que contam como "NÃO ATENDIDA" (perdida)
NA_TERMS = [
    "não atend", "nao atend", "abandon", "no answer",
    "sem resposta", "timeout", "cancel", "not answered", "no-answer"
]

# Status que contam como "ATENDIDA"
OK_TERMS = [
    "atend", "answer", "conclu", "success", "sucesso", "complete", "completed", "connected"
]

# Chaves para detecção de colunas
STORE_KEYS  = ["loja","store","unidade","filial","site","branch"]
QUEUE_KEYS  = ["queue","fila","skill","department","grupo","setor"]
STATUS_KEYS = ["status","result","disposition","motivo","outcome","termina","final"]
AGENT_KEYS  = ["agent","agente","operador","user","atendente"]
# ==================================================


def read_csv_auto(path: str) -> tuple[pd.DataFrame, str]:
    for d in [",",";","|","\t"]:
        try:
            t = pd.read_csv(path, delimiter=d, engine="python", dtype=str, nrows=200)
            if t.shape[1] > 1 and t.notna().sum().sum() > t.shape[0]:
                df = pd.read_csv(path, delimiter=d, engine="python", dtype=str)
                print(f"[INFO] Delimitador detectado: {repr(d)}")
                return df, d
        except Exception:
            pass
    raise RuntimeError("Não consegui detectar o delimitador do CSV.")


def find_candidates(cols, keys):
    return [c for c in cols if any(k in c.lower() for k in keys)]


def looks_like_store_values(s: pd.Series) -> float:
    vals = s.dropna().astype(str).str.lower()
    # pontos por conter palavras de loja/filial/site/branch ou padrão "Loja 123"
    return float(vals.str.contains(r"\bloja\b|\bfilial\b|\bsite\b|\bbranch\b|loja\s*\d+", regex=True).mean())


def pick_store_column(df: pd.DataFrame) -> str:
    cand = find_candidates(df.columns, STORE_KEYS)
    if cand:
        return max(cand, key=lambda c: looks_like_store_values(df[c]))
    # fallback evitando colunas de tempo
    non_time = [c for c in df.columns if not re.search(r"date|data|start|hora|time|timestamp", c, re.I)]
    if not non_time:
        return df.columns[0]
    scores = {c: looks_like_store_values(df[c]) for c in non_time}
    return max(scores, key=scores.get)


def pick_first_or_fallback(df: pd.DataFrame, keys, idx):
    cand = find_candidates(df.columns, keys)
    return cand[0] if cand else df.columns[min(idx, len(df.columns)-1)]


def normalize(s: pd.Series) -> pd.Series:
    return s.fillna("(vazio)").astype(str).str.strip()


def status_is_na(s: pd.Series) -> pd.Series:
    low = s.str.lower()
    return low.apply(lambda x: any(term in x for term in NA_TERMS))


def status_is_ok(s: pd.Series) -> pd.Series:
    low = s.str.lower()
    return low.apply(lambda x: any(term in x for term in OK_TERMS))


def fila_match(series: pd.Series) -> pd.Series:
    if QUEUE_MATCH_MODE == "contains":
        a = series.str.contains("estrela", case=False, na=False)
        b = series.str.contains("tele", case=False, na=False)
        return a & b
    elif QUEUE_MATCH_MODE == "exact":
        return series.str.strip().eq(QUEUE_TARGET)
    else:
        raise ValueError("QUEUE_MATCH_MODE inválido.")


def first_datetime_col(df: pd.DataFrame):
    date_cols = [c for c in df.columns if re.search(r"date|data|start|hora|time|timestamp", c, re.I)]
    for c in date_cols:
        ts = pd.to_datetime(df[c], errors="coerce", dayfirst=True, infer_datetime_format=True)
        if ts.notna().sum() > 0.2*len(df):
            return c, ts
    return None, None


def loja_sort_key(name: str):
    m = re.search(r"\d+", str(name))
    return (0 if (m and m.group(0) == "1") else 1, int(m.group(0)) if m else 999999, str(name))


def main():
    # 1) Leitura
    df, _ = read_csv_auto(CSV_PATH)
    df.columns = [c.strip() for c in df.columns]

    # 2) Detecção de colunas
    store_col  = pick_store_column(df)
    queue_col  = pick_first_or_fallback(df, QUEUE_KEYS, 1)
    status_col = pick_first_or_fallback(df, STATUS_KEYS, len(df.columns)-1)

    print(f"[INFO] Loja:   {store_col}")
    print(f"[INFO] Fila:   {queue_col}")
    print(f"[INFO] Status: {status_col}")

    # 3) Normalização
    df[store_col]  = normalize(df[store_col])
    df[queue_col]  = normalize(df[queue_col])
    df[status_col] = normalize(df[status_col])

    # 4) Coluna de data/hora (para detalhe por loja)
    dt_col, dt_series = first_datetime_col(df)
    if dt_series is not None:
        df["__data__"] = dt_series.dt.date
        df["__hora__"] = dt_series.dt.time
    else:
        df["__data__"] = pd.NaT
        df["__hora__"] = pd.NaT

    # 5) Métricas
    mask_na_total   = status_is_na(df[status_col])             # perdidas (todas as filas)
    mask_ok_telev   = status_is_ok(df[status_col]) & fila_match(df[queue_col])  # atendidas pela Estrela Televendas

    # perdidas totais por loja
    perdidas_total_por_loja = (
        df[mask_na_total].groupby(store_col).size()
        .rename("perdidas_total")
        .reset_index()
    )

    # atendidas pela Estrela Televendas por loja
    atendidas_telev_por_loja = (
        df[mask_ok_telev].groupby(store_col).size()
        .rename("atendidas_est_telev")
        .reset_index()
    )

    # perdas ajustadas = perdidas_total - atendidas_telev
    ajuste = (
        perdidas_total_por_loja.merge(atendidas_telev_por_loja, on=store_col, how="left")
        .fillna({"atendidas_est_telev": 0})
    )
    ajuste["perdidas_ajustadas"] = ajuste["perdidas_total"] - ajuste["atendidas_est_telev"]
    ajuste = ajuste.sort_values(["perdidas_ajustadas", "perdidas_total"], ascending=[False, False])

    # 6) Abas por loja com ATENDIDAS pela Televendas (data/hora)
    df_ok_telev = df[mask_ok_telev][[store_col, "__data__", "__hora__", queue_col, status_col]].rename(columns={
        "__data__": "data",
        "__hora__": "hora",
        queue_col: "fila",
        status_col: "status"
    }).sort_values(["data","hora"], na_position="last")

    lojas = df_ok_telev[store_col].dropna().unique().tolist()
    lojas_sorted = sorted(lojas, key=loja_sort_key)

    # 7) Extras úteis
    calls_por_fila = (df[queue_col].value_counts(dropna=False)
                      .rename("chamadas").reset_index()
                      .rename(columns={"index": queue_col}))
    calls_por_status = (df[status_col].str.lower().value_counts(dropna=False)
                        .rename("chamadas").reset_index()
                        .rename(columns={"index": status_col}))

    dicionario = pd.DataFrame({
        "coluna": df.columns,
        "n_unique": [df[c].nunique(dropna=True) for c in df.columns],
        "exemplo": [df[c].dropna().astype(str).head(1).tolist()[0] if df[c].dropna().shape[0] else "" for c in df.columns]
    })

    # 8) Exporta
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as xw:
        dicionario.to_excel(xw, index=False, sheet_name="dicionario")
        calls_por_fila.to_excel(xw, index=False, sheet_name="calls_por_fila")
        calls_por_status.to_excel(xw, index=False, sheet_name="calls_por_status")
        ajuste.to_excel(xw, index=False, sheet_name="perdas_ajustadas_por_loja")
        # abas por loja (ATENDIDAS pela Televendas)
        for loja in lojas_sorted:
            sub = df_ok_telev[df_ok_telev[store_col] == loja].drop(columns=[store_col]).copy()
            safe = re.sub(r"[^A-Za-z0-9 _-]+","_", str(loja))[:31] or "loja"
            sheet_name = f"loja_{safe}"
            sub.to_excel(xw, index=False, sheet_name=sheet_name)

    print(f"[OK] Gerado: {OUT_XLSX}")
    print(f"[OK] Linhas (perdidas totais): {len(df[mask_na_total])}")
    print(f"[OK] Linhas (atendidas Televendas): {len(df[mask_ok_telev])}")

if __name__ == "__main__":
    main()
