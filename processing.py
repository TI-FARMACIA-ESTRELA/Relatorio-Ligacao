from __future__ import annotations
import re
import unicodedata
from pathlib import Path
from typing import Optional, Iterable, Dict
import pandas as pd

# ================= Helpers gerais =================

def _strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

RE_LOJA_NUM = re.compile(r"(?:\bloja\b|\bfilial\b|\blj\b)\D*?(\d{1,3})", re.IGNORECASE)

def norm_loja_text(s: str) -> Optional[str]:
    """Extrai número e retorna 'Loja NN'. Aceita 'Loja 9', 'Filial-09', 'LJ 21', 'Loja21', só '21' etc."""
    if not isinstance(s, str):
        return None
    s0 = s.strip()
    m = RE_LOJA_NUM.search(s0)
    if not m:
        onlynum = re.sub(r"\D", "", s0)
        if onlynum.isdigit() and 1 <= int(onlynum) <= 999:
            return f"Loja {int(onlynum):02d}"
        return None
    return f"Loja {int(m.group(1)):02d}"

def _col_like(df: pd.DataFrame, *alts: Iterable[str]) -> Optional[str]:
    cols = [c for c in df.columns if isinstance(c, str)]
    low = {c.lower(): c for c in cols}
    for a in alts:
        if isinstance(a, str) and a.lower() in low:
            return low[a.lower()]
    for c in cols:
        cl = c.lower()
        for a in alts:
            if isinstance(a, str) and a.lower() in cl:
                return c
    return None

# =============== CSV de chamadas (queue-caller-details_*.csv) ===============

# mapeia status p/ PT-BR (ajustado)
STATUS_PT = {
    "handled": "atendida",
    "completed": "atendida",
    "connected": "atendida",
    "success": "atendida",
    "answer": "atendida",

    "abandoned": "Cliente desistiu",
    "no answer": "não atendida",
    "not answered": "não atendida",
    "timeout": "tempo esgotado",
    "cancel": "cancelada",

    "evicted system": "Televendas não atendeu",
    "evicted by system": "Televendas não atendeu",
}

# chave pra contar “perdidas”
LOST_PT = {"Cliente desistiu", "não atendida", "tempo esgotado", "cancelada", "Televendas não atendeu"}
LOST_PT_LOWER = {s.lower() for s in LOST_PT}

def _status_pt(v: str) -> str:
    base = _strip_accents((v or "").lower().strip())
    for k, pt in STATUS_PT.items():
        if k in base:
            return pt
    # fallback
    return v or "-"

QUEUE_MATCH_MODE = "smart"  # "smart" | "contains" | "exact"
QUEUE_TARGET = "Estrela Televendas"

def _queue_is_televendas(name: str) -> bool:
    """Match robusto ignorando acentos/pontuação. Requer 'estrela' e (televendas|tele|tlv)."""
    s = _strip_accents((name or "").lower())
    s = re.sub(r"[^a-z0-9]+", " ", s)
    tokens = set(s.split())
    has_estrela = "estrela" in tokens or "estrela" in s
    has_tele = any(t in tokens for t in ["televendas", "tele", "tlv"]) or ("tele" in s or "televenda" in s)
    return bool(has_estrela and has_tele)

def _fila_match(series: pd.Series) -> pd.Series:
    if QUEUE_MATCH_MODE == "exact":
        return series.fillna("").str.strip().eq(QUEUE_TARGET)
    if QUEUE_MATCH_MODE == "contains":
        a = series.str.contains("estrela", case=False, na=False)
        b = series.str.contains("tele", case=False, na=False) | series.str.contains("televenda", case=False, na=False)
        return a & b
    # smart
    return series.map(_queue_is_televendas)

# -------- detecção “inteligente” de colunas --------
STORE_KEYS  = ["loja","store","unidade","filial","site","branch","origem"]
QUEUE_KEYS  = ["queue","fila","skill","department","grupo","setor"]
STATUS_KEYS = ["status","result","disposition","motivo","outcome","termina","final"]

def _find_candidates(cols, keys):
    return [c for c in cols if any(k in c.lower() for k in keys)]

def _looks_like_store_values(s: pd.Series) -> float:
    vals = s.dropna().astype(str).str.lower()
    if len(vals) == 0:
        return 0.0
    score = 0.0
    score += float(vals.str.contains(r"\bloja\b|\bfilial\b|\bbranch\b|\bsite\b|\blj\b", regex=True).mean()) * 0.6
    score += float(vals.str.contains(r"loja\D*\d+$|lj\D*\d+$", regex=True).mean()) * 0.4
    return score

def _pick_store_column(df: pd.DataFrame) -> str:
    cand = _find_candidates(df.columns, STORE_KEYS)
    if cand:
        return max(cand, key=lambda c: _looks_like_store_values(df[c]))
    non_time = [c for c in df.columns if not re.search(r"date|data|start|hora|time|timestamp", c, re.I)]
    if not non_time:
        return df.columns[0]
    scores = {c: _looks_like_store_values(df[c]) for c in non_time}
    return max(scores, key=scores.get)

def _pick_first_or_fallback(df: pd.DataFrame, keys, idx):
    cand = _find_candidates(df.columns, keys)
    return cand[0] if cand else df.columns[min(idx, len(df.columns)-1)]

def _guess_dayfirst(series: pd.Series) -> bool:
    """Heurística: se maioria dos primeiros números > 12, é dia/mês."""
    vals = series.dropna().astype(str).head(300).tolist()
    hits = 0
    total = 0
    for v in vals:
        m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", v)
        if m:
            total += 1
            d1 = int(m.group(1))
            if d1 > 12:
                hits += 1
    return hits > (total * 0.6) if total else True  # assume dayfirst se não der pra decidir

def _best_datetime_series(df: pd.DataFrame):
    """
    Tenta extrair uma série de datetimes estáveis a partir do DF.
    - Prioriza colunas com 'America/Sao_Paulo' no nome.
    - Aguenta epoch em segundos/milisegundos.
    - Faz heurística de dayfirst para textos.
    - Normaliza para timezone America/Sao_Paulo quando possível.
    Retorna: (nome_col_escolhida ou None, pandas.Series de datetimes ou None)
    """
    cols = [c for c in df.columns if re.search(r"date|data|start|hora|time|timestamp", str(c), re.I)]
    if not cols:
        return None, None

    def _score_col(name: str) -> int:
        n = name.lower()
        score = 0
        if "america" in n and ("sao_paulo" in n or "sao paulo" in n):
            score += 5
        if "start" in n or "inicio" in n:
            score += 2
        if "time" in n or "hora" in n:
            score += 1
        return score

    cols = sorted(cols, key=_score_col, reverse=True)

    def _parse_epoch(series: pd.Series):
        s = pd.to_numeric(series, errors="coerce")
        if s.notna().sum() == 0:
            return None
        median = float(s.dropna().median())
        if median >= 1e12:
            return pd.to_datetime(s, unit="ms", errors="coerce", utc=True)
        if median >= 1e9:
            return pd.to_datetime(s, unit="s", errors="coerce", utc=True)
        return None

    def _parse_text(series: pd.Series):
        dayfirst = _guess_dayfirst(series.astype(str))
        return pd.to_datetime(series.astype(str), errors="coerce", dayfirst=dayfirst, utc=False)

    for c in cols:
        raw = df[c]
        ts = _parse_epoch(raw)
        if ts is None:
            ts = _parse_text(raw)

        if ts is not None and ts.notna().sum() >= max(5, int(0.2 * len(raw))):
            try:
                if getattr(ts.dt, "tz", None) is not None:
                    ts = ts.dt.tz_convert("America/Sao_Paulo")
            except Exception:
                pass
            return c, ts

    return None, None  # fim correto da função

def _normalize(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()

def _force_any_store_column(df: pd.DataFrame) -> pd.Series:
    cols = [c for c in df.columns if isinstance(c, str)]
    out = pd.Series([None]*len(df), dtype="object")
    for c in cols:
        attempt = df[c].astype(str).map(norm_loja_text)
        out = out.where(out.notna(), attempt)
    return out

# ==================== Parser principal ====================

def load_calls_only(path: Path) -> pd.DataFrame:
    """
    Lê CSV do sistema, filtra fila Televendas (se detectada),
    normaliza:
      - store: 'Loja NN'
      - dt/hr: data e hora como strings (sem 'NaT')
      - status: em PT-BR
      - is_lost: flag pra perdidas
    """
    path = Path(path)

    # detecta separador
    sep = None
    for d in [",",";","|","\t"]:
        try:
            t = pd.read_csv(path, delimiter=d, engine="python", dtype=str, nrows=100)
            if t.shape[1] > 1 and t.notna().sum().sum() > t.shape[0]:
                sep = d
                break
        except Exception:
            pass
    if sep is None:
        sep = ","

    df = pd.read_csv(path, delimiter=sep, engine="python", dtype=str)
    df = df.fillna("")
    df.columns = [c.strip() for c in df.columns]

    print(f"[CONS] CSV: {path.name} | colunas: {len(df.columns)} -> {list(df.columns)[:8]}{'...' if len(df.columns)>8 else ''}")

    store_col  = _pick_store_column(df)
    queue_col  = _pick_first_or_fallback(df, QUEUE_KEYS, 1)
    status_col = _pick_first_or_fallback(df, STATUS_KEYS, len(df.columns)-1)

    print(f"[CONS] Escolhido -> Loja: '{store_col}' | Fila: '{queue_col}' | Status: '{status_col}'")

    df[store_col]  = _normalize(df[store_col])
    df[queue_col]  = _normalize(df[queue_col])
    df[status_col] = _normalize(df[status_col])

    # ================= DATA/HORA =================
    dt_col, ts = _best_datetime_series(df)
    if ts is not None:
        try:
            if getattr(ts.dt, "tz", None) is not None:
                ts_local = ts.dt.tz_convert("America/Sao_Paulo")
            else:
                ts_local = ts
        except Exception:
            ts_local = ts

        df["dt"] = ts_local.dt.strftime("%Y-%m-%d")
        df["hr"] = ts_local.dt.strftime("%H:%M:%S")
        df.loc[ts_local.isna(), ["dt", "hr"]] = "-"
        print(f"[CONS] Coluna de tempo: '{dt_col}' (ok)")
    else:
        df["dt"] = "-"
        df["hr"] = "-"
        print("[CONS] Nenhuma coluna de data/hora convincente.")

    # Log das top filas pra debug
    print("[CONS] Top filas:", df[queue_col].value_counts().head(8).to_dict())

    # Filtra fila televendas (ou fallback)
    mask_fila = _fila_match(df[queue_col])
    fila_detectada = mask_fila.sum() > 0
    out = df[mask_fila].copy() if fila_detectada else df.copy()
    out.attrs["from_all_queues"] = not fila_detectada
    print(f"[CONS] Fila televendas detectada? {'SIM' if fila_detectada else 'NÃO (usando todas as filas)'}")

    # Normaliza loja (e tenta forçar de qualquer coluna se necessário)
    lojas = out[store_col].map(norm_loja_text)
    if lojas.notna().sum() < max(1, int(0.2 * len(out))):
        print("[CONS] Poucas lojas reconhecidas no store_col. Tentando extrair de quaisquer colunas…")
        lojas = _force_any_store_column(out)

    out = out.assign(store=lojas)[["store", queue_col, status_col, "dt", "hr"]]
    out = out.rename(columns={queue_col: "queue", status_col: "status"})
    out = out[out["store"].notna()].copy()

    # traduz status e marca perdidas (sem tabs, apenas 4 espaços)
    out["status"] = out["status"].map(_status_pt)
    out["is_lost"] = out["status"].str.lower().isin(LOST_PT_LOWER)

    print(f"[CONS] Linhas após normalização & filtro: {len(out)} | Lojas únicas: {out['store'].nunique()}")

    if len(out) == 0:
        top_store_vals = df[store_col].value_counts().head(5).to_dict()
        top_queue_vals = df[queue_col].value_counts().head(5).to_dict()
        top_status_vals= df[status_col].value_counts().head(5).to_dict()
        raise RuntimeError(
            "Nenhuma loja reconhecida após normalização. "
            f"Coluna loja='{store_col}' exemplos={top_store_vals} | "
            f"fila='{queue_col}' exemplos={top_queue_vals} | status='{status_col}' exemplos={top_status_vals}"
        )

    return out

# ==================== Agregações ====================

def agregados_por_loja(calls: pd.DataFrame) -> pd.DataFrame:
    """
    Gera recebidas, perdidas e pct_perda (provisório; ajusta com volume depois).
    - recebidas: total de registros (dataset já filtrado na fila)
    - perdidas: contagem de is_lost
    - pct_perda: será recalculado com volume_total, mas deixo algo provisório
    """
    # calls = calls.drop_duplicates(subset=["store","dt","hr","status"], keep="first")  # se precisar

    rec = calls.groupby("store", dropna=True).size().rename("recebidas")
    per = calls[calls["is_lost"]].groupby("store").size().rename("perdidas")
    res = pd.concat([rec, per], axis=1).fillna(0).astype(int).reset_index()

    # provisório (% perda com base no próprio recebido); o correto vem com volume_total via merge
    res["pct_perda"] = (res["perdidas"] / res["recebidas"]).where(res["recebidas"] > 0, 0.0) * 100.0

    # ordena por número da loja
    res["loja_num"] = res["store"].str.extract(r"(\d+)$").astype(float)
    res = res.sort_values(["loja_num","store"]).drop(columns=["loja_num"]).reset_index(drop=True)

    print(f"[CONS] Agregado -> {len(res)} lojas, total recebidas={res['recebidas'].sum()} perdidas={res['perdidas'].sum()}")
    return res

def aplicar_volumes(agregado: pd.DataFrame, volumes: Dict[str, int]) -> pd.DataFrame:
    """
    Recebe o DataFrame de agregados e um dict { 'Loja 01': volume_total, ... }.
    Retorna um novo DF com 'volume_total' e 'pct_perda' recalculado:
      pct_perda = perdidas / volume_total * 100
    Se uma loja não tiver volume informado, usa recebidas como fallback.
    """
    df = agregado.copy()
    df["volume_total"] = df["store"].map(lambda s: int(volumes.get(s, 0)) if volumes else 0)
    df["volume_total"] = df.apply(lambda r: int(r["recebidas"]) if int(r["volume_total"]) <= 0 else int(r["volume_total"]), axis=1)
    df["pct_perda"] = (df["perdidas"] / df["volume_total"]).where(df["volume_total"] > 0, 0.0) * 100.0
    return df

# ==================== Detalhe ====================

def detalhe_chamadas(calls: pd.DataFrame, loja: str) -> pd.DataFrame:
    """
    Detalhe por loja, com data/hora legíveis e status PT-BR.
    Ordena por dt/hr (quando possível); mantém '-'/nulos no final.
    """
    sub = calls[calls["store"] == loja][["dt","hr","status"]].copy()
    sub["dt"] = sub["dt"].fillna("-").astype(str)
    sub["hr"] = sub["hr"].fillna("-").astype(str)

    try:
        k1 = pd.to_datetime(sub["dt"], errors="coerce")
        k2 = pd.to_datetime(sub["hr"], errors="coerce", format="%H:%M:%S")
        sub["__k1"] = k1
        sub["__k2"] = k2
        sub = sub.sort_values(["__k1","__k2"], na_position="last").drop(columns=["__k1","__k2"])
    except Exception:
        sub = sub.sort_values(["dt","hr"], na_position="last")

    sub = sub.rename(columns={"dt":"Data","hr":"Hora","status":"Status"})
    return sub.reset_index(drop=True)
