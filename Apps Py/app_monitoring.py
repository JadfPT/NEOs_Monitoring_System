import json
import os
import threading
import queue
from typing import Any, Optional, Tuple

import pyodbc
import tkinter as tk
from tkinter import ttk, messagebox

DEFAULT_CONFIG = "loader_config.json"


def load_config(path: str = DEFAULT_CONFIG) -> Optional[dict]:
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def build_conn_str(cfg: dict) -> str:
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


def connect(cfg: dict) -> pyodbc.Connection:
    conn = pyodbc.connect(build_conn_str(cfg))
    conn.autocommit = False
    return conn


def draw_line_chart(canvas: tk.Canvas, labels: list, values: list) -> None:
    canvas.delete("all")
    w = canvas.winfo_width() or 400
    h = canvas.winfo_height() or 140
    pad = 28
    if not values:
        canvas.create_text(w / 2, h / 2, text="Sem dados", fill="#666666")
        return
    max_v = max(values) if values else 0
    min_v = min(values) if values else 0
    span = max_v - min_v if max_v != min_v else 1
    step_x = (w - 2 * pad) / max(1, len(values) - 1)
    points = []
    for i, v in enumerate(values):
        x = pad + i * step_x
        y = h - pad - ((v - min_v) / span) * (h - 2 * pad)
        points.append((x, y))
    canvas.create_line(pad, h - pad, w - pad, h - pad, fill="#bbbbbb")
    canvas.create_line(pad, pad, pad, h - pad, fill="#bbbbbb")
    for i in range(1, len(points)):
        canvas.create_line(points[i - 1][0], points[i - 1][1], points[i][0], points[i][1], fill="#2b6cb0", width=2)
    for x, y in points:
        canvas.create_oval(x - 2, y - 2, x + 2, y + 2, fill="#2b6cb0", outline="")
    canvas.create_text(pad, pad - 8, text=f"{max_v:.3f}" if isinstance(max_v, float) else str(max_v), anchor="w", fill="#666666")
    canvas.create_text(pad, h - pad + 12, text=f"{min_v:.3f}" if isinstance(min_v, float) else str(min_v), anchor="w", fill="#666666")
    if labels:
        canvas.create_text(pad, h - 6, text=str(labels[0]), anchor="w", fill="#666666")
        canvas.create_text(w - pad, h - 6, text=str(labels[-1]), anchor="e", fill="#666666")


def main() -> None:
    root = tk.Tk()
    root.title("NEOs - Monitorizacao")
    root.geometry("980x720")

    style = ttk.Style(root)
    if "clam" in style.theme_names():
        style.theme_use("clam")

    # --- Connection bar ---
    conn_frame = ttk.LabelFrame(root, text="Ligacao")
    conn_frame.pack(fill="x", padx=12, pady=10)

    var_server = tk.StringVar()
    var_port = tk.StringVar()
    var_user = tk.StringVar()
    var_password = tk.StringVar()
    var_database = tk.StringVar(value="NEOs")
    status_var = tk.StringVar(value="")

    ttk.Label(conn_frame, text="Servidor").grid(row=0, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(conn_frame, textvariable=var_server, width=28).grid(row=0, column=1, sticky="w", padx=6, pady=4)
    ttk.Label(conn_frame, text="Porta").grid(row=0, column=2, sticky="w", padx=6, pady=4)
    ttk.Entry(conn_frame, textvariable=var_port, width=10).grid(row=0, column=3, sticky="w", padx=6, pady=4)

    ttk.Label(conn_frame, text="BD").grid(row=1, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(conn_frame, textvariable=var_database, width=28).grid(row=1, column=1, sticky="w", padx=6, pady=4)
    ttk.Label(conn_frame, text="Utilizador").grid(row=1, column=2, sticky="w", padx=6, pady=4)
    ttk.Entry(conn_frame, textvariable=var_user, width=20).grid(row=1, column=3, sticky="w", padx=6, pady=4)

    ttk.Label(conn_frame, text="Password").grid(row=2, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(conn_frame, textvariable=var_password, show="*", width=28).grid(row=2, column=1, sticky="w", padx=6, pady=4)

    status_label = ttk.Label(conn_frame, textvariable=status_var, foreground="gray")
    status_label.grid(row=2, column=2, columnspan=2, sticky="w", padx=6, pady=4)

    def set_status(msg: str, ok: Optional[bool] = None) -> None:
        status_var.set(msg)
        if ok is True:
            status_label.configure(foreground="green")
        elif ok is False:
            status_label.configure(foreground="red")
        else:
            status_label.configure(foreground="gray")

    def cfg_from_fields() -> dict:
        return {
            "server": var_server.get().strip(),
            "port": var_port.get().strip(),
            "user": var_user.get().strip(),
            "password": var_password.get(),
            "database": var_database.get().strip(),
        }

    def on_load_cfg() -> None:
        cfg = load_config()
        if not cfg:
            messagebox.showwarning("Config", "Nao existe loader_config.json valido.")
            return
        var_server.set(cfg.get("server", ""))
        var_port.set(cfg.get("port", ""))
        var_user.set(cfg.get("user", "sa"))
        var_password.set(cfg.get("password", ""))
        var_database.set(cfg.get("database", "NEOs"))
        set_status("Configuracao carregada.", True)

    ttk.Button(conn_frame, text="Carregar Config", command=on_load_cfg).grid(row=0, column=4, padx=6, pady=4)

    # --- Monitorizacao ---
    monitor_canvas = tk.Canvas(root, highlightthickness=0)
    monitor_scroll = ttk.Scrollbar(root, orient="vertical", command=monitor_canvas.yview)
    monitor_canvas.configure(yscrollcommand=monitor_scroll.set)
    monitor_scroll.pack(side="right", fill="y")
    monitor_canvas.pack(side="left", fill="both", expand=True)

    body = ttk.Frame(monitor_canvas)
    body_window = monitor_canvas.create_window((0, 0), window=body, anchor="nw")

    def _sync_scroll(_event: tk.Event) -> None:
        monitor_canvas.configure(scrollregion=monitor_canvas.bbox("all"))

    def _sync_width(event: tk.Event) -> None:
        monitor_canvas.itemconfigure(body_window, width=event.width)

    body.bind("<Configure>", _sync_scroll)
    monitor_canvas.bind("<Configure>", _sync_width)

    stats_frame = ttk.LabelFrame(body, text="Resumo geral")
    stats_frame.pack(fill="x", pady=(0, 8))

    var_ast = tk.StringVar(value="-")
    var_orbit = tk.StringVar(value="-")
    var_alert = tk.StringVar(value="-")
    var_high = tk.StringVar(value="-")
    var_red = tk.StringVar(value="-")
    var_orange = tk.StringVar(value="-")
    var_pha_over = tk.StringVar(value="-")
    var_new_neos = tk.StringVar(value="-")
    var_next_critical = tk.StringVar(value="-")

    ttk.Label(stats_frame, text="Asteroides:").grid(row=0, column=0, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, textvariable=var_ast).grid(row=0, column=1, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, text="Orbits:").grid(row=0, column=2, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, textvariable=var_orbit).grid(row=0, column=3, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, text="Alertas:").grid(row=1, column=0, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, textvariable=var_alert).grid(row=1, column=1, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, text="Alertas High:").grid(row=1, column=2, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, textvariable=var_high).grid(row=1, column=3, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, text="Alertas Vermelhos:").grid(row=2, column=0, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, textvariable=var_red).grid(row=2, column=1, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, text="Alertas Laranja:").grid(row=2, column=2, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, textvariable=var_orange).grid(row=2, column=3, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, text="PHAs > 100m:").grid(row=3, column=0, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, textvariable=var_pha_over).grid(row=3, column=1, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, text="Novos NEOs (ultimo mes):").grid(row=3, column=2, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, textvariable=var_new_neos).grid(row=3, column=3, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, text="Proximo Evento <5 LD:").grid(row=4, column=0, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, textvariable=var_next_critical).grid(row=4, column=1, sticky="w", padx=6, pady=2)
    stats_frame.grid_columnconfigure(4, weight=1)

    trend_frame = ttk.LabelFrame(body, text="Tendencias")
    trend_frame.pack(fill="x", pady=(0, 8))
    trend_frame.grid_columnconfigure(0, weight=1, uniform="trend")
    trend_frame.grid_columnconfigure(1, weight=1, uniform="trend")

    precision_block = ttk.Frame(trend_frame)
    discovery_block = ttk.Frame(trend_frame)
    precision_block.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
    discovery_block.grid(row=0, column=1, sticky="nsew", padx=6, pady=6)
    precision_block.grid_columnconfigure(0, weight=1)
    discovery_block.grid_columnconfigure(0, weight=1)

    ttk.Label(precision_block, text="Precisao orbital (RMS medio por ano)").grid(row=0, column=0, sticky="w")
    ttk.Label(discovery_block, text="Novas descobertas (por mes)").grid(row=0, column=0, sticky="w")
    precision_canvas = tk.Canvas(precision_block, height=140, bg="white", highlightthickness=1, highlightbackground="#d0d0d0")
    discovery_canvas = tk.Canvas(discovery_block, height=140, bg="white", highlightthickness=1, highlightbackground="#d0d0d0")
    precision_canvas.grid(row=1, column=0, sticky="we", pady=(6, 0))
    discovery_canvas.grid(row=1, column=0, sticky="we", pady=(6, 0))

    tables_frame = ttk.Frame(body)
    tables_frame.pack(fill="both", expand=True)
    tables_frame.grid_columnconfigure(0, weight=1, uniform="tbl")
    tables_frame.grid_columnconfigure(1, weight=1, uniform="tbl")

    precision_box = ttk.LabelFrame(tables_frame, text="Detalhe: Precisao orbital (RMS medio por ano)")
    discovery_box = ttk.LabelFrame(tables_frame, text="Detalhe: Novas descobertas (por mes)")
    latest_box = ttk.LabelFrame(tables_frame, text="Ultimas descobertas")
    precision_box.grid(row=0, column=0, sticky="nsew", padx=(0, 6), pady=(0, 8))
    discovery_box.grid(row=0, column=1, sticky="nsew", padx=(6, 0), pady=(0, 8))
    latest_box.grid(row=1, column=0, columnspan=2, sticky="nsew")

    precision_tree = ttk.Treeview(
        precision_box,
        columns=("year", "count", "avg_rms"),
        show="headings",
        height=8,
    )
    precision_tree.heading("year", text="Ano")
    precision_tree.heading("count", text="Orbits")
    precision_tree.heading("avg_rms", text="RMS Medio")
    precision_tree.column("year", width=80, anchor="w")
    precision_tree.column("count", width=80, anchor="w")
    precision_tree.column("avg_rms", width=120, anchor="w")
    precision_tree.pack(fill="x", padx=6, pady=6)

    discovery_tree = ttk.Treeview(
        discovery_box,
        columns=("period", "count"),
        show="headings",
        height=8,
    )
    discovery_tree.heading("period", text="Periodo (YYYY-MM)")
    discovery_tree.heading("count", text="Novas Descobertas")
    discovery_tree.column("period", width=120, anchor="w")
    discovery_tree.column("count", width=150, anchor="w")
    discovery_tree.pack(fill="x", padx=6, pady=6)

    latest_tree = ttk.Treeview(
        latest_box,
        columns=("id_internal", "full_name", "created_at"),
        show="headings",
        height=8,
    )
    latest_tree.heading("id_internal", text="ID")
    latest_tree.heading("full_name", text="Nome")
    latest_tree.heading("created_at", text="Criado Em")
    latest_tree.column("id_internal", width=80, anchor="w")
    latest_tree.column("full_name", width=240, anchor="w")
    latest_tree.column("created_at", width=160, anchor="w")
    latest_tree.pack(fill="x", padx=6, pady=6)

    def clear_tree(tree: ttk.Treeview) -> None:
        for item in tree.get_children():
            tree.delete(item)

    trend_cache: dict = {"precision": ([], []), "discovery": ([], [])}

    def refresh_charts() -> None:
        draw_line_chart(precision_canvas, trend_cache["precision"][0], trend_cache["precision"][1])
        draw_line_chart(discovery_canvas, trend_cache["discovery"][0], trend_cache["discovery"][1])

    q = queue.Queue()

    def refresh_monitor() -> None:
        cfg = cfg_from_fields()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM Asteroid;")
                ast_count = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM Orbit;")
                orbit_count = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM Alert;")
                alert_count = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM Alert WHERE id_priority = 1;")
                high_count = int(cur.fetchone()[0])

                cur.execute("SELECT red_alerts, orange_alerts, pha_over_100 FROM vw_Alert_Stats;")
                row = cur.fetchone()
                red_count = int(row[0] if row and row[0] is not None else 0)
                orange_count = int(row[1] if row and row[1] is not None else 0)
                pha_over = int(row[2] if row and row[2] is not None else 0)

                cur.execute("SELECT next_close_approach_date FROM vw_Next_Critical_Event;")
                row = cur.fetchone()
                next_critical = row[0] if row else None

                cur.execute("SELECT new_neos_last_month FROM vw_New_NEOs_LastMonth;")
                row = cur.fetchone()
                new_neos = int(row[0] if row and row[0] is not None else 0)

                cur.execute("""
                    SELECT TOP 12 yr, cnt, avg_rms
                    FROM vw_RMS_Trend
                    ORDER BY yr DESC;
                """)
                precision_rows = cur.fetchall()

                cur.execute("""
                    SELECT TOP 12
                        FORMAT(created_at, 'yyyy-MM') AS period,
                        COUNT(*) AS cnt
                    FROM Asteroid
                    WHERE created_at IS NOT NULL
                    GROUP BY FORMAT(created_at, 'yyyy-MM')
                    ORDER BY period DESC;
                """)
                discovery_rows = cur.fetchall()

                cur.execute("""
                    SELECT TOP 10 id_internal, full_name, created_at
                    FROM Asteroid
                    ORDER BY created_at DESC;
                """)
                latest_rows = cur.fetchall()
                conn.close()

                q.put((
                    "stats",
                    (ast_count, orbit_count, alert_count, high_count, red_count, orange_count, pha_over, new_neos, next_critical, precision_rows, discovery_rows, latest_rows),
                ))
            except Exception as ex:
                q.put(("error", str(ex)))

        threading.Thread(target=worker, daemon=True).start()

    def update_ui(payload: Tuple[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]) -> None:
        (
            ast_count,
            orbit_count,
            alert_count,
            high_count,
            red_count,
            orange_count,
            pha_over,
            new_neos,
            next_critical,
            precision_rows,
            discovery_rows,
            latest_rows,
        ) = payload
        var_ast.set(str(ast_count))
        var_orbit.set(str(orbit_count))
        var_alert.set(str(alert_count))
        var_high.set(str(high_count))
        var_red.set(str(red_count))
        var_orange.set(str(orange_count))
        var_pha_over.set(str(pha_over))
        var_new_neos.set(str(new_neos))
        if next_critical:
            var_next_critical.set(
                next_critical.strftime("%Y-%m-%d") if hasattr(next_critical, "strftime") else str(next_critical)
            )
        else:
            var_next_critical.set("-")

        clear_tree(precision_tree)
        precision_labels = []
        precision_vals = []
        for row in precision_rows:
            yr, cnt, avg_rms = row
            avg_txt = f"{avg_rms:.4f}" if avg_rms is not None else "-"
            precision_tree.insert("", "end", values=(yr, cnt, avg_txt))
            precision_labels.append(str(yr))
            precision_vals.append(avg_rms if avg_rms is not None else 0.0)

        clear_tree(discovery_tree)
        discovery_labels = []
        discovery_vals = []
        for row in discovery_rows:
            discovery_tree.insert("", "end", values=(row[0], row[1]))
            discovery_labels.append(str(row[0]))
            discovery_vals.append(float(row[1]))

        clear_tree(latest_tree)
        for row in latest_rows:
            created = row[2]
            created_txt = created.strftime("%Y-%m-%d") if hasattr(created, "strftime") else str(created)
            latest_tree.insert("", "end", values=(row[0], row[1], created_txt))

        trend_cache["precision"] = (list(reversed(precision_labels)), list(reversed(precision_vals)))
        trend_cache["discovery"] = (list(reversed(discovery_labels)), list(reversed(discovery_vals)))
        refresh_charts()

    def poll_queue() -> None:
        try:
            while True:
                kind, payload = q.get_nowait()
                if kind == "stats":
                    update_ui(payload)
                    set_status("Atualizado.", True)
                else:
                    set_status(f"Erro: {payload}", False)
        except queue.Empty:
            pass
        root.after(200, poll_queue)

    ttk.Button(conn_frame, text="Atualizar", command=refresh_monitor).grid(row=1, column=4, padx=6, pady=4)
    precision_canvas.bind("<Configure>", lambda _e: refresh_charts())
    discovery_canvas.bind("<Configure>", lambda _e: refresh_charts())

    cfg = load_config()
    if cfg:
        var_server.set(cfg.get("server", ""))
        var_port.set(cfg.get("port", ""))
        var_user.set(cfg.get("user", "sa"))
        var_password.set(cfg.get("password", ""))
        var_database.set(cfg.get("database", "NEOs"))
        set_status("Configuracao carregada.", True)

    poll_queue()
    root.mainloop()


if __name__ == "__main__":
    main()
