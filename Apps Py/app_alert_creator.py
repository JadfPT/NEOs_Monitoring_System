import json
import os
import threading
from datetime import datetime
import uuid
from typing import Dict, Optional, Tuple

import pyodbc
import tkinter as tk
from tkinter import ttk, messagebox

DEFAULT_CONFIG = "loader_config.json"


def save_config(cfg: Dict[str, str], path: str = DEFAULT_CONFIG) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


def load_config(path: str = DEFAULT_CONFIG) -> Optional[Dict[str, str]]:
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def build_conn_str(cfg: Dict[str, str]) -> str:
    server = cfg["server"].strip()
    port = cfg.get("port", "").strip()
    if port:
        server = f"{server},{port}"
    return (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={cfg['database'].strip()};"
        f"UID={cfg['user'].strip()};"
        f"PWD={cfg['password']};"
        "TrustServerCertificate=yes;"
    )


def connect(cfg: Dict[str, str]) -> pyodbc.Connection:
    conn = pyodbc.connect(build_conn_str(cfg))
    conn.autocommit = False
    return conn


def parse_datetime(text: str) -> Optional[datetime]:
    value = text.strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def main() -> None:
    root = tk.Tk()
    root.title("NEOs - Alert Creator")
    root.geometry("980x700")

    style = ttk.Style(root)
    if "clam" in style.theme_names():
        style.theme_use("clam")

    # --- Connection frame ---
    conn_frame = ttk.LabelFrame(root, text="Ligacao")
    conn_frame.pack(fill="x", padx=12, pady=10)

    fields = {
        "server": tk.StringVar(),
        "port": tk.StringVar(),
        "database": tk.StringVar(value="BD_PL2_06"),
        "user": tk.StringVar(),
        "password": tk.StringVar(),
    }

    ttk.Label(conn_frame, text="Servidor").grid(row=0, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(conn_frame, textvariable=fields["server"], width=28).grid(row=0, column=1, sticky="w", padx=6, pady=4)
    ttk.Label(conn_frame, text="Porta").grid(row=0, column=2, sticky="w", padx=6, pady=4)
    ttk.Entry(conn_frame, textvariable=fields["port"], width=10).grid(row=0, column=3, sticky="w", padx=6, pady=4)

    ttk.Label(conn_frame, text="Base de Dados").grid(row=1, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(conn_frame, textvariable=fields["database"], width=28).grid(row=1, column=1, sticky="w", padx=6, pady=4)
    ttk.Label(conn_frame, text="Utilizador").grid(row=1, column=2, sticky="w", padx=6, pady=4)
    ttk.Entry(conn_frame, textvariable=fields["user"], width=20).grid(row=1, column=3, sticky="w", padx=6, pady=4)

    ttk.Label(conn_frame, text="Password").grid(row=2, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(conn_frame, textvariable=fields["password"], show="*", width=28).grid(row=2, column=1, sticky="w", padx=6, pady=4)

    def read_cfg() -> Dict[str, str]:
        return {k: v.get().strip() for k, v in fields.items()}

    def apply_cfg(cfg: Dict[str, str]) -> None:
        for k, v in fields.items():
            if k in cfg:
                v.set(cfg[k])

    def on_load_cfg() -> None:
        cfg = load_config()
        if not cfg:
            messagebox.showwarning("Config", "Nao existe configuracao guardada.")
            return
        apply_cfg(cfg)

    def on_save_cfg() -> None:
        cfg = read_cfg()
        if not cfg.get("server") or not cfg.get("database") or not cfg.get("user"):
            messagebox.showwarning("Config", "Preenche servidor, base de dados e utilizador.")
            return
        save_config(cfg)
        messagebox.showinfo("Config", "Configuracao guardada.")

    def on_test() -> None:
        cfg = read_cfg()
        try:
            conn = connect(cfg)
            cur = conn.cursor()
            cur.execute("SELECT DB_NAME()")
            name = cur.fetchone()[0]
            cur.close()
            conn.close()
            messagebox.showinfo("Ligacao", f"OK: {name}")
        except Exception as ex:
            messagebox.showerror("Ligacao", f"Erro: {ex}")

    ttk.Button(conn_frame, text="Carregar Config", command=on_load_cfg).grid(row=0, column=4, padx=6, pady=4)
    ttk.Button(conn_frame, text="Guardar Config", command=on_save_cfg).grid(row=1, column=4, padx=6, pady=4)
    ttk.Button(conn_frame, text="Testar Ligacao", command=on_test).grid(row=2, column=4, padx=6, pady=4)

    # --- Alert form ---
    form_frame = ttk.LabelFrame(root, text="Criar Alert")
    form_frame.pack(fill="x", padx=12, pady=(0, 10))

    var_id_internal = tk.StringVar()
    var_criteria = tk.StringVar()
    var_priority = tk.StringVar()
    var_level = tk.StringVar()
    var_data_gen = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    ttk.Label(form_frame, text="ID Interno").grid(row=0, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(form_frame, textvariable=var_id_internal, width=16).grid(row=0, column=1, sticky="w", padx=6, pady=4)

    ttk.Label(form_frame, text="Criteria").grid(row=1, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(form_frame, textvariable=var_criteria, width=80).grid(row=1, column=1, columnspan=4, sticky="we", padx=6, pady=4)

    ttk.Label(form_frame, text="Regra").grid(row=4, column=0, sticky="w", padx=6, pady=4)
    var_rule = tk.StringVar()
    rule_combo = ttk.Combobox(form_frame, textvariable=var_rule, width=48, state="readonly")
    rule_combo.grid(row=4, column=1, columnspan=3, sticky="w", padx=6, pady=4)

    ttk.Label(form_frame, text="Priority").grid(row=2, column=0, sticky="w", padx=6, pady=4)
    priority_combo = ttk.Combobox(form_frame, textvariable=var_priority, width=18, state="readonly")
    priority_combo.grid(row=2, column=1, sticky="w", padx=6, pady=4)

    ttk.Label(form_frame, text="Level").grid(row=2, column=2, sticky="w", padx=6, pady=4)
    level_combo = ttk.Combobox(form_frame, textvariable=var_level, width=18, state="readonly")
    level_combo.grid(row=2, column=3, sticky="w", padx=6, pady=4)

    ttk.Label(form_frame, text="Data Geracao").grid(row=3, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(form_frame, textvariable=var_data_gen, width=24).grid(row=3, column=1, sticky="w", padx=6, pady=4)

    # --- Asteroid search ---
    search_frame = ttk.LabelFrame(root, text="Pesquisar Asteroide")
    search_frame.pack(fill="x", padx=12, pady=(0, 10))

    var_search = tk.StringVar()
    ttk.Label(search_frame, text="Nome / PDes").grid(row=0, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(search_frame, textvariable=var_search, width=40).grid(row=0, column=1, sticky="w", padx=6, pady=4)

    search_columns = (
        "id_internal",
        "spkid",
        "full_name",
        "pdes",
        "name",
        "prefix",
        "neo_flag",
        "pha_flag",
        "diameter",
        "absolute_magnitude",
        "albedo",
        "diameter_sigma",
        "created_at",
        "neo_id",
    )
    results = ttk.Treeview(search_frame, columns=search_columns, show="headings", height=6)
    headings = {
        "id_internal": ("ID", 70),
        "spkid": ("SPKID", 90),
        "full_name": ("Nome Completo", 220),
        "pdes": ("PDes", 120),
        "name": ("Nome", 140),
        "prefix": ("Prefixo", 80),
        "neo_flag": ("NEO", 60),
        "pha_flag": ("PHA", 60),
        "diameter": ("Diametro", 90),
        "absolute_magnitude": ("H", 80),
        "albedo": ("Albedo", 80),
        "diameter_sigma": ("Diametro Sigma", 120),
        "created_at": ("Criado Em", 150),
        "neo_id": ("NEO ID", 90),
    }
    for key, (title, width) in headings.items():
        results.heading(key, text=title)
        results.column(key, width=width, anchor="w", stretch=False)
    results.grid(row=1, column=0, columnspan=3, sticky="nsew", padx=6, pady=6)
    search_frame.grid_columnconfigure(1, weight=1)
    search_frame.grid_rowconfigure(1, weight=1)

    results_scroll_x = ttk.Scrollbar(search_frame, orient="horizontal", command=results.xview)
    results.configure(xscrollcommand=results_scroll_x.set)
    results_scroll_x.grid(row=2, column=0, columnspan=3, sticky="we", padx=6, pady=(0, 6))

    def load_reference_data() -> Tuple[Dict[str, int], Dict[str, int]]:
        cfg = read_cfg()
        priorities: Dict[str, int] = {}
        levels: Dict[str, int] = {}
        conn = connect(cfg)
        cur = conn.cursor()
        cur.execute("SELECT id_priority, name FROM Priority ORDER BY id_priority")
        for pid, name in cur.fetchall():
            priorities[f"{pid} - {name}"] = int(pid)
        cur.execute("SELECT id_level, description FROM Level ORDER BY id_level")
        for lid, desc in cur.fetchall():
            levels[f"{lid} - {desc}"] = int(lid)
        cur.close()
        conn.close()
        return priorities, levels

    priority_map: Dict[str, int] = {}
    level_map: Dict[str, int] = {}

    def refresh_refs() -> None:
        nonlocal priority_map, level_map
        try:
            priority_map, level_map = load_reference_data()
            priority_combo["values"] = list(priority_map.keys())
            level_combo["values"] = list(level_map.keys())
        except Exception as ex:
            messagebox.showerror("Erro", f"Nao foi possivel carregar prioridades/niveis: {ex}")

    def normalize_value(value) -> str:
        if value is None:
            return ""
        if isinstance(value, float):
            return f"{value:.6g}"
        text = str(value).strip()
        if text.upper() == "NULL":
            return ""
        return text

    def do_search() -> None:
        term = var_search.get().strip()
        if not term:
            messagebox.showwarning("Pesquisa", "Escreve algo para pesquisar.")
            return
        for item in results.get_children():
            results.delete(item)
        cfg = read_cfg()
        try:
            conn = connect(cfg)
            cur = conn.cursor()
            like = f"%{term}%"
            params = [like, like, like, like]
            sql = (
                "SELECT TOP 200 id_internal, spkid, full_name, pdes, name, prefix, "
                "neo_flag, pha_flag, diameter, absolute_magnitude, albedo, diameter_sigma, "
                "CONVERT(varchar(19), created_at, 120) AS created_at, neo_id "
                "FROM Asteroid "
                "WHERE pdes LIKE ? OR full_name LIKE ? OR name LIKE ? OR neo_id LIKE ?"
            )
            if term.isdigit():
                sql += " OR id_internal = ? OR spkid = ?"
                params.extend([int(term), int(term)])
            sql += " ORDER BY id_internal DESC"
            cur.execute(sql, params)
            for row in cur.fetchall():
                results.insert("", "end", values=[normalize_value(v) for v in row])
            cur.close()
            conn.close()
        except Exception as ex:
            messagebox.showerror("Pesquisa", f"Erro: {ex}")

    def on_select_result(event: tk.Event) -> None:
        sel = results.selection()
        if not sel:
            return
        values = results.item(sel[0], "values")
        if values:
            var_id_internal.set(values[0])

    results.bind("<Double-1>", on_select_result)
    ttk.Button(search_frame, text="Pesquisar", command=do_search).grid(row=0, column=2, padx=6, pady=4)

    def create_alert() -> None:
        cfg = read_cfg()
        if not var_id_internal.get().strip():
            messagebox.showwarning("Alert", "ID interno obrigatorio.")
            return
        try:
            id_internal = int(var_id_internal.get().strip())
        except ValueError:
            messagebox.showwarning("Alert", "ID interno invalido.")
            return
        criteria = var_criteria.get().strip()
        if not criteria:
            messagebox.showwarning("Alert", "Criteria obrigatorio.")
            return
        if not var_priority.get() or not var_level.get():
            messagebox.showwarning("Alert", "Seleciona Priority e Level.")
            return
        priority_id = priority_map.get(var_priority.get())
        level_id = level_map.get(var_level.get())
        if priority_id is None or level_id is None:
            messagebox.showwarning("Alert", "Priority/Level invalidos.")
            return
        data_gen = parse_datetime(var_data_gen.get())
        if data_gen is None:
            data_gen = datetime.now()

        try:
            conn = connect(cfg)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO Alert (id_alert, data_generation, criteria_trigger, id_internal, id_priority, id_level) "
                "VALUES (NEXT VALUE FOR dbo.seq_alert_id, ?, ?, ?, ?, ?)",
                data_gen, criteria, id_internal, priority_id, level_id,
            )
            conn.commit()
            cur.close()
            conn.close()
            messagebox.showinfo("Alert", "Alert criado.")
        except Exception as ex:
            messagebox.showerror("Alert", f"Erro: {ex}")

    def clear_form() -> None:
        var_criteria.set("")
        var_data_gen.set(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        var_rule.set("")

    ttk.Button(form_frame, text="Criar Alert", command=create_alert).grid(row=0, column=4, padx=6, pady=4)
    ttk.Button(form_frame, text="Limpar", command=clear_form).grid(row=3, column=4, padx=6, pady=4)
    ttk.Button(form_frame, text="Atualizar Listas", command=refresh_refs).grid(row=2, column=4, padx=6, pady=4)
    ttk.Button(form_frame, text="Aplicar Regra", command=lambda: apply_rule()).grid(row=4, column=4, padx=6, pady=4)
    ttk.Button(form_frame, text="Simular Alertas", command=lambda: open_simulation_dialog()).grid(row=5, column=4, padx=6, pady=6)

    rules = {
        "High: Close approach <1 LD in 7 days, diameter>10mm": (
            "Close approach < 1 LD within 7 days (diameter>10mm)",
            "1 - High",
            "4 - Critical",
        ),
        "High: PHA uncertain (diam>100, rms>0.8, moid_ld<20)": (
            "PHA uncertain: diameter>10m0, rms>0.8, moid_ld<20",
            "1 - High",
            "3 - High",
        ),
        "Medium: New large asteroid (diam>500, last month, moid_ld<50)": (
            "New large asteroid: diameter>50m0m, moid_ld<50, discovered last month",
            "2 - Medium",
            "2 - Medium",
        ),
        "Medium: Significant change (|Δe|>0.05 OR |Δi|>2)": (
            "Significant orbital change: |Δe|>0.05 OR |Δi|>2",
            "2 - Medium",
            "2 - Medium",
        ),
        "Low: Clustered approaches <10 LD in month": (
            "Clustered close approaches <10 LD in month",
            "3 - Low",
            "1 - Low",
        ),
        "Low: Anomaly (albedo>0.3, e>0.8, i>70, diam>200)": (
            "Anomaly: albedo>0.3 AND e>0.8 AND i>70 AND diameter>200m",
            "3 - Low",
            "1 - Low",
        ),
    }
    rule_combo["values"] = list(rules.keys())

    def apply_rule() -> None:
        rule = rules.get(var_rule.get())
        if not rule:
            messagebox.showwarning("Regra", "Seleciona uma regra.")
            return
        criteria, priority, level = rule
        var_criteria.set(criteria)
        if priority in priority_map:
            var_priority.set(priority)
        if level in level_map:
            var_level.set(level)

    def open_simulation_dialog() -> None:
        dialog = tk.Toplevel(root)
        dialog.title("Simular Alertas")
        dialog.transient(root)
        dialog.grab_set()

        choices = [
            ("close_7d", "Alta: <1 LD em 7 dias (diametro > 10m)"),
            ("pha_uncertain", "Alta: PHA incerto (diam>100, rms>0.8, moid_ld<20)"),
            ("new_large", "Media: novo asteroide grande (diam>500, ultimo mes, moid_ld<50)"),
            ("significant_change", "Media: mudanca orbital (|Δe|>0.05 ou |Δi|>2)"),
            ("cluster_month", "Baixa: >5 aproximacoes <10 LD no mesmo mes"),
            ("anomaly", "Baixa: anomalia (albedo>0.3, e>0.8, i>70, diam>200)"),
        ]
        vars_map = {key: tk.BooleanVar(value=True) for key, _ in choices}

        ttk.Label(dialog, text="Seleciona os cenarios a simular:").pack(anchor="w", padx=10, pady=(10, 4))
        for key, label in choices:
            ttk.Checkbutton(dialog, text=label, variable=vars_map[key]).pack(anchor="w", padx=12)

        def on_run() -> None:
            selected = [k for k, _ in choices if vars_map[k].get()]
            dialog.destroy()
            if not selected:
                messagebox.showwarning("Simular", "Seleciona pelo menos um cenario.")
                return
            simulate_alerts(selected)

        btns = ttk.Frame(dialog)
        btns.pack(fill="x", padx=10, pady=10)
        ttk.Button(btns, text="Simular", command=on_run).pack(side="right")
        ttk.Button(btns, text="Cancelar", command=dialog.destroy).pack(side="right", padx=(0, 6))

    def simulate_alerts(selected: list[str]) -> None:
        cfg = read_cfg()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                logs = []

                if "close_7d" in selected or "cluster_month" in selected:
                    cur.execute("""
                        SELECT TOP 1 id_internal
                        FROM Asteroid
                        WHERE diameter IS NOT NULL AND diameter > 0.01
                        ORDER BY diameter DESC;
                    """)
                    row = cur.fetchone()
                    if not row:
                        raise RuntimeError("Nao encontrei asteroide com diametro > 10.")
                    base_id = int(row[0])

                if "close_7d" in selected:
                    cur.execute("SELECT ISNULL(MAX(id_ca), 0) + 1 FROM Close_Approach;")
                    id_ca = int(cur.fetchone()[0])
                    cur.execute(
                        "INSERT INTO Close_Approach (id_ca, approach_date, rel_velocity_kms, dist_ld, id_internal) "
                        "VALUES (?, DATEADD(DAY, 3, CAST(GETDATE() AS date)), 12.3, 0.5, ?)",
                        id_ca,
                        base_id,
                    )
                    logs.append("Simulado: aproximacao <1 LD em 7 dias.")

                if "cluster_month" in selected:
                    cur.execute("SELECT ISNULL(MAX(id_ca), 0) FROM Close_Approach;")
                    base_ca = int(cur.fetchone()[0])
                    cur.execute("""
                        WITH nums AS (
                            SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3
                            UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6
                        )
                        INSERT INTO Close_Approach (id_ca, approach_date, rel_velocity_kms, dist_ld, id_internal)
                        SELECT
                            ? + n,
                            DATEADD(DAY, n, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)),
                            8.5,
                            5.0,
                            ?
                        FROM nums;
                    """, base_ca, base_id)
                    logs.append("Simulado: cluster mensal >5 aproximacoes <10 LD.")

                if "pha_uncertain" in selected:
                    cur.execute("""
                        SELECT TOP 1 id_internal
                        FROM Asteroid
                        WHERE pha_flag = 'Y' AND diameter IS NOT NULL AND diameter > 0.1
                        ORDER BY diameter DESC;
                    """)
                    row = cur.fetchone()
                    if row:
                        cur.execute("""
                            UPDATE TOP (1) Orbit
                            SET rms = 0.9, moid_ld = 10
                            WHERE id_internal = ?;
                        """, int(row[0]))
                        logs.append("Simulado: PHA incerto (rms>0.8, moid_ld<20).")

                if "new_large" in selected:
                    cur.execute("""
                        SELECT TOP 1 id_internal
                        FROM Asteroid
                        WHERE diameter IS NOT NULL AND diameter > 0.5
                          AND created_at >= DATEADD(MONTH, -1, SYSDATETIME())
                        ORDER BY diameter DESC;
                    """)
                    row = cur.fetchone()
                    if row:
                        cur.execute("""
                            UPDATE TOP (1) Orbit
                            SET moid_ld = 30
                            WHERE id_internal = ?;
                        """, int(row[0]))
                        logs.append("Simulado: novo asteroide grande (ultimo mes).")

                if "anomaly" in selected:
                    cur.execute("""
                        SELECT TOP 1 id_internal
                        FROM Asteroid
                        WHERE diameter IS NOT NULL AND diameter > 0.2
                          AND albedo IS NOT NULL AND albedo > 0.3
                        ORDER BY diameter DESC;
                    """)
                    row = cur.fetchone()
                    if row:
                        cur.execute("""
                            UPDATE TOP (1) Orbit
                            SET e = 0.85, i = 75
                            WHERE id_internal = ?;
                        """, int(row[0]))
                        logs.append("Simulado: anomalia (albedo/e/i/diametro).")

                if "significant_change" in selected:
                    cur.execute("""
                        SELECT TOP 1 id_internal
                        FROM Orbit
                        WHERE id_internal IS NOT NULL
                        ORDER BY epoch DESC;
                    """)
                    row = cur.fetchone()
                    if row:
                        orb_id = int(row[0])
                        new_id = f"SIM_{uuid.uuid4()}"
                        cur.execute("""
                            INSERT INTO Orbit (
                                id_orbita, epoch, rms, moid_ld, epoch_mjd, epoch_cal,
                                tp, tp_cal, per, per_y, equinox,
                                orbit_uncertainty, condition_code,
                                e, a, q, i, om, w, ma, ad, n, moid,
                                sigma_e, sigma_a, sigma_q, sigma_i, sigma_n, sigma_ma, sigma_om, sigma_w, sigma_ad, sigma_tp, sigma_per,
                                id_internal, class
                            )
                            SELECT
                                ?, o.epoch + 1, o.rms, o.moid_ld, o.epoch_mjd + 1,
                                DATEADD(DAY, 1, COALESCE(o.epoch_cal, CAST(GETDATE() AS date))),
                                o.tp + 1, DATEADD(DAY, 1, COALESCE(o.tp_cal, CAST(GETDATE() AS date))),
                                o.per, o.per_y, o.equinox,
                                o.orbit_uncertainty, o.condition_code,
                                o.e + 0.1, o.a, o.q, o.i + 3, o.om, o.w, o.ma, o.ad, o.n, o.moid,
                                o.sigma_e, o.sigma_a, o.sigma_q, o.sigma_i, o.sigma_n, o.sigma_ma, o.sigma_om, o.sigma_w, o.sigma_ad, o.sigma_tp, o.sigma_per,
                                o.id_internal, o.class
                            FROM Orbit o
                            WHERE o.id_internal = ?
                            ORDER BY o.epoch DESC;
                        """, new_id, orb_id)
                        logs.append("Simulado: mudanca orbital significativa (novo Orbit).")

                conn.commit()
                conn.close()
                msg = "Simulacao concluida:\\n- " + "\\n- ".join(logs) if logs else "Nada foi simulado."
                messagebox.showinfo("Simular", msg)
            except Exception as ex:
                messagebox.showerror("Simular", f"Erro: {ex}")

        threading.Thread(target=worker, daemon=True).start()

    cfg = load_config()
    if cfg:
        apply_cfg(cfg)

    refresh_refs()

    root.mainloop()


if __name__ == "__main__":
    main()
