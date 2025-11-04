# routes.py
from __future__ import annotations
from flask import Blueprint, render_template, request, redirect, url_for, send_file, flash, session
from pathlib import Path
import re
import pandas as pd
from io import BytesIO

from db import get_db
from processing import load_calls_only, agregados_por_loja, detalhe_chamadas

# ======= Constantes de status (PT-BR) para filtros =======
STATUS_HANDLED   = {"atendida"}
STATUS_EVICTED   = {"Televendas não atendeu"}
STATUS_ABANDONED = {"Cliente desistiu"}
STATUS_LOST_ALL  = STATUS_EVICTED | STATUS_ABANDONED | {"não atendida", "tempo esgotado", "cancelada"}

# ======= Blueprint / Pastas =======
bp = Blueprint("bp", __name__)

BASE_DIR   = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
CALLS_DIR  = UPLOAD_DIR / "calls"
for d in (UPLOAD_DIR, CALLS_DIR):
    d.mkdir(parents=True, exist_ok=True)

ADMIN_PASS = "EstrelaGOTO"

# ======= Helpers básicos =======
def is_admin() -> bool:
    return session.get("is_admin") is True

def sanitize_ym(ym: str) -> str:
    ym = (ym or "").strip().replace("/", "-")
    m = re.fullmatch(r"(\d{4})[-]?(\d{2})", ym)
    return f"{m.group(1)}-{m.group(2)}" if m else ym

def month_id_for(con, ym: str) -> int:
    row = con.execute("SELECT id FROM months WHERE ym=?", (ym,)).fetchone()
    if row:
        return row["id"]
    cur = con.execute("INSERT INTO months (ym) VALUES (?)", (ym,))
    con.commit()
    return cur.lastrowid

def _last_calls_file(ym: str) -> Path | None:
    files = sorted(CALLS_DIR.glob(f"{ym}__*"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None

def _slug(s: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^A-Za-z0-9]+", "-", s.strip())).strip("-").lower()

def _deslug(slug: str, options: list[str]) -> str | None:
    tgt = _slug(slug)
    for o in options:
        if _slug(o) == tgt:
            return o
    return None

# ======= Auth =======
@bp.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        if request.form.get("password", "") == ADMIN_PASS:
            session["is_admin"] = True
            return redirect(url_for("bp.admin_dashboard"))
        flash("Senha incorreta.", "danger")
    return render_template("admin_login.html")

@bp.route("/admin/logout")
def admin_logout():
    session.pop("is_admin", None)
    return redirect(url_for("bp.public_index"))

# ======= Público =======
@bp.route("/")
def public_index():
    with get_db() as con:
        months = con.execute("SELECT id, ym FROM months ORDER BY ym DESC").fetchall()
    return render_template("public_index.html", months=months)

@bp.route("/report/<ym>")
def public_report(ym):
    ym = sanitize_ym(ym)
    order = (request.args.get("order") or "loja").lower()

    with get_db() as con:
        month = con.execute("SELECT id, ym FROM months WHERE ym=?", (ym,)).fetchone()
        if not month:
            flash("Sem dados consolidados para este mês.", "info")
            return render_template("public_report.html", ym=ym, rows=[], order=order)
        mid = month["id"]

        # Puxa do banco
        df = pd.read_sql_query("""
            SELECT store         AS Loja,
                   recebidas     AS Recebidas,
                   perdidas      AS Perdidas,
                   volume_total  AS Volume,
                   pct_perda     AS pct
              FROM metrics
             WHERE month_id=?
        """, con, params=(mid,))

    if df.empty:
        flash("Sem dados consolidados para este mês.", "info")
        return render_template("public_report.html", ym=ym, rows=[], order=order)

    # Tipos corretos
    df["Recebidas"] = pd.to_numeric(df["Recebidas"], errors="coerce").fillna(0).astype(int)
    df["Perdidas"]  = pd.to_numeric(df["Perdidas"],  errors="coerce").fillna(0).astype(int)
    df["Volume"]    = pd.to_numeric(df["Volume"],    errors="coerce").fillna(0).astype(int)
    df["pct"]       = pd.to_numeric(df["pct"],       errors="coerce").fillna(0.0)

  # % baseadas no Volume
    df["PctPerdidas"]  = (df["Perdidas"] / df["Volume"]).where(df["Volume"] > 0, 0.0) * 100.0
    df["PctAtendidas"] = ((df["Volume"] - df["Perdidas"]) / df["Volume"]).where(df["Volume"] > 0, 0.0) * 100.0

    # 🔁 Back-compat: alias para quem renderiza/ordena por 'pct'
    df["pct"] = df["PctPerdidas"]

    # 🔁 Normaliza nomes de ordenação vindos do template
    alias = {
        "pct_desc": "pct_perdidas_desc",
        "pct_asc": "pct_perdidas_asc",
        "pct_perda_desc": "pct_perdidas_desc",
        "pct_perda_asc": "pct_perdidas_asc",
    }
    order = alias.get(order, order)


    # --- Ordenação ---
    if order == "pct_perdidas_desc":
        df = df.sort_values(["PctPerdidas", "Loja"], ascending=[False, True])
    elif order == "pct_perdidas_asc":
        df = df.sort_values(["PctPerdidas", "Loja"], ascending=[True, True])
    elif order == "pct_atendidas_desc":
        df = df.sort_values(["PctAtendidas", "Loja"], ascending=[False, True])
    elif order == "pct_atendidas_asc":
        df = df.sort_values(["PctAtendidas", "Loja"], ascending=[True, True])
    elif order == "volume_desc":
        df = df.sort_values(["Volume", "Loja"], ascending=[False, True])
    elif order == "volume_asc":
        df = df.sort_values(["Volume", "Loja"], ascending=[True, True])
    elif order == "loja_desc":
        df["_n"] = df["Loja"].astype(str).str.extract(r"(\d+)$").astype(float)
        df = df.sort_values(["_n", "Loja"], ascending=[False, False]).drop(columns=["_n"])
    else:  # "loja" (padrão)
        df["_n"] = df["Loja"].astype(str).str.extract(r"(\d+)$").astype(float)
        df = df.sort_values(["_n", "Loja"], ascending=[True, True]).drop(columns=["_n"])

    rows = df.to_dict(orient="records")
    return render_template("public_report.html", ym=ym, rows=rows, order=order)


@bp.route("/report/<ym>/store/<store_slug>")
def public_store_detail(ym, store_slug):
    ym = sanitize_ym(ym)
    calls_file = _last_calls_file(ym)
    if not calls_file:
        flash("Arquivo de chamadas não encontrado para o mês.", "warning")
        return redirect(url_for("bp.public_report", ym=ym))

    calls = load_calls_only(calls_file)

    stores = sorted(calls["store"].dropna().unique().tolist())
    loja = _deslug(store_slug, stores)
    if not loja:
        flash("Loja não encontrada neste mês.", "warning")
        return redirect(url_for("bp.public_report", ym=ym))

    det = calls[calls["store"] == loja][["dt", "hr", "status"]].copy()

    counts = det["status"].value_counts(dropna=False).to_dict()
    perdidas_count = int(det["status"].isin(STATUS_LOST_ALL).sum())

    # Pega volume_total para calcular % real com base no que o admin digitou
    with get_db() as con:
        month = con.execute("SELECT id FROM months WHERE ym=?", (ym,)).fetchone()
        mid = month["id"] if month else None
        mt_row = (
            con.execute(
                "SELECT volume_total FROM metrics WHERE month_id=? AND store=?",
                (mid, loja),
            ).fetchone()
            if mid
            else None
        )

    volume_total = int(mt_row["volume_total"]) if (mt_row and mt_row["volume_total"] is not None) else 0
    pct_perda_real = (perdidas_count / volume_total * 100.0) if volume_total > 0 else 0.0

    # Filtro via querystring
    f = (request.args.get("f") or "").strip().lower()
    if f == "atendida":
        det = det[det["status"].isin(STATUS_HANDLED)]
    elif f == "expulsa":
        det = det[det["status"].isin(STATUS_EVICTED)]
    elif f == "Cliente desistiu":
        det = det[det["status"].isin(STATUS_ABANDONED)]
    elif f == "outros":
        det = det[~det["status"].isin(STATUS_HANDLED | STATUS_EVICTED | STATUS_ABANDONED)]

    det = det.sort_values(["dt", "hr"], na_position="last").reset_index(drop=True)
    rows = det.rename(columns={"dt": "Data", "hr": "Hora", "status": "Status"}).to_dict(orient="records")

    badges = {
        "atendida": counts.get("atendida", 0),
        "expulsa": counts.get("Televendas não atendeu", 0),
        "Cliente desistiu": counts.get("Cliente desistiu", 0),
        "outros": len(calls[calls["store"] == loja])
        - (
            counts.get("atendida", 0)
            + counts.get("Televendas não atendeu", 0)
            + counts.get("Cliente desistiu", 0)
        ),
    }

    return render_template(
        "public_store_detail.html",
        ym=ym,
        store=loja,
        rows=rows,
        filter_sel=f,
        badges=badges,
        volume_total=volume_total,
        perdidas=perdidas_count,
        pct_perda_real=pct_perda_real,
    )

@bp.route("/export/<ym>.xlsx")
def export_excel(ym):
    ym = sanitize_ym(ym)

    # Checa volumes pendentes antes de exportar
    with get_db() as con:
        month = con.execute("SELECT id FROM months WHERE ym=?", (ym,)).fetchone()
        if not month:
            flash("Mês não encontrado.", "danger")
            return redirect(url_for("bp.public_index"))
        pend = con.execute(
            "SELECT COUNT(*) AS c FROM metrics WHERE month_id=? AND volume_total=0",
            (month["id"],),
        ).fetchone()
        if pend and pend["c"] > 0:
            flash("Há lojas sem volume informado. Preencha os volumes antes de exportar.", "warning")
            return redirect(url_for("bp.admin_volumes", ym=ym))

    calls_file = _last_calls_file(ym)
    if not calls_file:
        flash("Sem arquivo para exportar.", "warning")
        return redirect(url_for("bp.public_report", ym=ym))

    calls = load_calls_only(calls_file)
    resumo = (
        agregados_por_loja(calls)
        .rename(columns={"store": "Loja", "recebidas": "Recebidas", "perdidas": "Perdidas"})
        .sort_values("Loja")
    )

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as xw:
        resumo.to_excel(xw, index=False, sheet_name="resumo")
        for loja in resumo["Loja"]:
            sub = detalhe_chamadas(calls, loja).rename(columns={"dt": "Data", "hr": "Hora", "status": "Status"})
            sub.to_excel(xw, index=False, sheet_name=("loja_" + _slug(loja))[:31])
    bio.seek(0)
    return send_file(
        bio,
        as_attachment=True,
        download_name=f"relatorio_televendas_{ym}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# ======= Admin =======
@bp.route("/admin")
def admin_dashboard():
    if not is_admin():
        return redirect(url_for("bp.admin_login"))
    with get_db() as con:
        months = con.execute(
            """
            SELECT m.id, m.ym,
              (SELECT COUNT(*) FROM uploads u WHERE u.month_id=m.id) AS uploads,
              (SELECT COUNT(*) FROM metrics t WHERE t.month_id=m.id) AS lojas
            FROM months m
            ORDER BY m.ym DESC
            """
        ).fetchall()
    return render_template("admin_dashboard.html", months=months)

@bp.route("/admin/upload", methods=["POST"])
def admin_upload():
    if not is_admin():
        return redirect(url_for("bp.admin_login"))

    ym = sanitize_ym(request.form.get("ym", ""))
    file = request.files.get("file")
    if not re.fullmatch(r"\d{4}-\d{2}", ym or "") or not file or not file.filename:
        flash("Informe mês AAAA-MM e selecione o arquivo.", "danger")
        return redirect(url_for("bp.admin_dashboard"))

    dest = CALLS_DIR / f"{ym}__{file.filename}"
    dest.parent.mkdir(parents=True, exist_ok=True)
    file.save(dest)

    try:
        consolidar_mes_apenas_calls(ym)
        flash("Upload concluído e mês consolidado. Agora informe o volume total por loja.", "success")
        return redirect(url_for("bp.admin_volumes", ym=ym))
    except Exception as e:
        flash(f"Upload salvo, mas falhou na consolidação: {e}", "warning")
        return redirect(url_for("bp.admin_dashboard"))

@bp.route("/admin/delete/<ym>", methods=["POST"])
def admin_delete(ym):
    if not is_admin():
        return redirect(url_for("bp.admin_login"))

    ym = sanitize_ym(ym)

    with get_db() as con:
        row = con.execute("SELECT id FROM months WHERE ym=?", (ym,)).fetchone()
        if row:
            mid = row["id"]
            con.execute("DELETE FROM metrics WHERE month_id=?", (mid,))
            con.execute("DELETE FROM uploads WHERE month_id=?", (mid,))
            con.execute("DELETE FROM months  WHERE id=?", (mid,))
            con.commit()

    try:
        for p in CALLS_DIR.glob(f"{ym}__*"):
            p.unlink(missing_ok=True)
    except Exception:
        pass

    flash(f"Mês {ym} removido.", "warning")
    return redirect(url_for("bp.admin_dashboard"))

def consolidar_mes_apenas_calls(ym: str):
    calls_file = _last_calls_file(ym)
    if not calls_file:
        raise RuntimeError("Envie a planilha de detalhamento de chamadas.")

    calls = load_calls_only(calls_file)
    df = agregados_por_loja(calls)  # store, recebidas, perdidas, pct_perda (inicial)

    with get_db() as con:
        mid = month_id_for(con, ym)
        con.execute("DELETE FROM metrics WHERE month_id=?", (mid,))
        con.execute("DELETE FROM uploads WHERE month_id=?", (mid,))
        con.execute("INSERT INTO uploads (month_id, filename) VALUES (?, ?)", (mid, calls_file.name))
        con.executemany(
            """
            INSERT INTO metrics (month_id, store, recebidas, perdidas, volume_total, pct_perda)
            VALUES (?, ?, ?, ?, 0, 0.0)
            """,
            [
                (mid, r["store"], int(r["recebidas"]), int(r["perdidas"]))
                for _, r in df.iterrows()
            ],
        )
        con.commit()

@bp.route("/admin/volumes/<ym>", methods=["GET", "POST"])
def admin_volumes(ym):
    if not is_admin():
        return redirect(url_for("bp.admin_login"))

    ym = sanitize_ym(ym)

    with get_db() as con:
        month = con.execute("SELECT id FROM months WHERE ym=?", (ym,)).fetchone()
        if not month:
            flash("Mês não encontrado.", "danger")
            return redirect(url_for("bp.admin_dashboard"))

        mid = month["id"]

        if request.method == "POST":
            # === Valida e coleta volumes ===
            items = []
            missing = []
            for key, val in request.form.items():
                if key.startswith("vol_"):
                    loja = key[4:]
                    raw = (val or "").strip()
                    if not raw:
                        missing.append(loja)
                        continue
                    try:
                        vol = int(raw)
                    except ValueError:
                        vol = 0
                    if vol <= 0:
                        missing.append(loja)
                    else:
                        items.append((vol, loja))

            if missing:
                flash(f"Informe um volume válido (≥1) para: {', '.join(missing)}.", "danger")
                df = pd.read_sql_query(
                    """
                    SELECT store, recebidas, perdidas, volume_total, pct_perda
                      FROM metrics WHERE month_id=? ORDER BY store
                    """,
                    con,
                    params=(mid,),
                )
                rows = df.to_dict(orient="records")
                return render_template("admin_volumes.html", ym=ym, rows=rows), 400

            # === Atualiza e recalcula % ===
            for vol, loja in items:
                con.execute(
                    """
                    UPDATE metrics
                       SET volume_total = ?,
                           pct_perda = CASE WHEN ? > 0
                                            THEN (CAST(perdidas AS REAL)/?) * 100.0
                                            ELSE 0.0 END
                     WHERE month_id=? AND store=?
                    """,
                    (vol, vol, vol, mid, loja),
                )
            con.commit()
            flash("Volumes atualizados e % de perda recalculada.", "success")
            return redirect(url_for("bp.public_report", ym=ym))

        # GET -> carrega lojas
        df = pd.read_sql_query(
            """
            SELECT store, recebidas, perdidas, volume_total, pct_perda
              FROM metrics WHERE month_id=?
            ORDER BY store
            """,
            con,
            params=(mid,),
        )

    rows = df.to_dict(orient="records")
    return render_template("admin_volumes.html", ym=ym, rows=rows)
