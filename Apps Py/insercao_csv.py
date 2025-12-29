import os
import json
import csv
from tkinter import filedialog
import pyodbc
from datetime import datetime, date
from typing import Optional, Dict, Tuple
from tkinter import Tk
from tkinter.filedialog import askopenfilename

from mpcorb_loader import load_mpcorb

DEFAULT_NEO_HEADER = [
    "id", "spkid", "full_name", "pdes", "name", "prefix", "neo", "pha", "h",
    "diameter", "albedo", "diameter_sigma", "orbit_id", "epoch", "epoch_mjd",
    "epoch_cal", "equinox", "e", "a", "q", "i", "om", "w", "ma", "ad", "n",
    "tp", "tp_cal", "per", "per_y", "moid", "moid_ld", "sigma_e", "sigma_a",
    "sigma_q", "sigma_i", "sigma_om", "sigma_w", "sigma_ma", "sigma_ad",
    "sigma_n", "sigma_tp", "sigma_per", "class", "rms", "class_description"
]

# ----------------- Config paths -----------------
DEFAULT_LOADER_CONFIG = "loader_config.json"
POSSIBLE_STOR_CFG_NAMES = ["ultima_configuracao.cfg", "ultima_configuração.cfg"]

# ----------------- Helpers -----------------
def read_stor_cfg_lines(path: str) -> Optional[dict]:
    if not os.path.isfile(path):
        return None

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        # mantém linhas vazias para respeitar o "port" na linha 2
        lines = [ln.rstrip("\n\r") for ln in f.readlines()]

    # garantir pelo menos 5 linhas
    while len(lines) < 5:
        lines.append("")

    server = (lines[0] or "").strip()
    port = (lines[1] or "").strip()
    user = (lines[2] or "").strip()
    password = (lines[3] or "").strip()
    database = (lines[4] or "").strip()

    if not server or not user or not database:
        return None

    return {
        "server": server,
        "port": port,
        "user": user,
        "password": password,
        "database": database
    }

def try_read_stor_cfg(path: str) -> Optional[dict]:
    # 1) formato do stor: 1 valor por linha
    cfg = read_stor_cfg_lines(path)
    if cfg:
        return cfg

    # 2) fallback (se algum dia mudar formato): JSON
    try:
        with open(path, "r", encoding="utf-8") as f:
            txt = f.read().strip()
        if txt.startswith("{") and txt.endswith("}"):
            data = json.loads(txt)
            return normalize_cfg_keys(data)
    except:
        pass

    # 3) fallback: key=value
    try:
        data = {}
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = [ln.strip() for ln in f if ln.strip()]
        for ln in lines:
            if "=" in ln:
                k, v = ln.split("=", 1)
                data[k.strip()] = v.strip()
        if data:
            return normalize_cfg_keys(data)
    except:
        pass

    return None

def normalize_cfg_keys(d: dict) -> dict:
    """
    Mapeia chaves para um formato consistente:
      server, port, user, password, database
    """
    keymap = {
        "Servidor": "server",
        "Servidor (IP/Nome)": "server",
        "server": "server",
        "host": "server",

        "Porta": "port",
        "Porta (opcional)": "port",
        "port": "port",

        "Utilizador": "user",
        "username": "user",
        "user": "user",

        "Password": "password",
        "password": "password",
        "pwd": "password",

        "Base de Dados": "database",
        "database": "database",
        "db": "database",
    }
    out = {"server": "", "port": "", "user": "", "password": "", "database": ""}
    for k, v in d.items():
        nk = keymap.get(k, None)
        if nk:
            out[nk] = str(v)
    return out

def save_loader_config(cfg: dict, path: str = DEFAULT_LOADER_CONFIG) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)

def load_loader_config(path: str = DEFAULT_LOADER_CONFIG) -> Optional[dict]:
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return None

def build_conn_str(cfg: dict) -> str:
    # SQL Auth (como tu estás a usar 'sa')
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

def safe_input(prompt: str, default: str = "") -> str:
    if default:
        v = input(f"{prompt} [{default}]: ").strip()
        return v if v else default
    return input(f"{prompt}: ").strip()

# ----------------- DB utility (same as before) -----------------
def table_exists(cur, name: str) -> bool:
    cur.execute("SELECT 1 FROM sys.tables WHERE name = ?", name)
    return cur.fetchone() is not None

def ensure_reference_data(cur):
    cur.execute("""
    IF NOT EXISTS (SELECT 1 FROM Priority)
    INSERT INTO Priority(id_priority, name) VALUES (1,'High'),(2,'Medium'),(3,'Low');
    """)
    cur.execute("""
    IF NOT EXISTS (SELECT 1 FROM Level)
    INSERT INTO Level(id_level, color, description)
    VALUES (1,'G','Green'),(2,'Y','Yellow'),(3,'O','Orange'),(4,'R','Red');
    """)

def log_error(cur, source_file: str, row_number: int, entity: str, msg: str, raw: str):
    if not table_exists(cur, "Load_Error"):
        return
    cur.execute(
        "INSERT INTO Load_Error(source_file, row_number, entity, error_message, raw_data) VALUES (?, ?, ?, ?, ?)",
        source_file, row_number, entity, msg, raw[:4000]
    )

def parse_float(x: str) -> Optional[float]:
    x = (x or "").strip()
    if x == "" or x.upper() == "NULL":
        return None
    try:
        return float(x)
    except:
        return None

def parse_int(x: str) -> Optional[int]:
    x = (x or "").strip()
    if x == "" or x.upper() == "NULL":
        return None
    try:
        return int(float(x))
    except:
        return None

def parse_date(x: str) -> Optional[date]:
    x = (x or "").strip()
    if x == "" or x.upper() == "NULL":
        return None

    # aceita YYYYMMDD ou YYYYMMDD.xxx
    if len(x) >= 8 and x[:8].isdigit():
        try:
            return datetime.strptime(x[:8], "%Y%m%d").date()
        except:
            return None

    # fallback
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(x, fmt).date()
        except:
            pass

    return None

def detect_encoding(path: str) -> str:
    with open(path, "rb") as f:
        raw = f.read(4096)
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return "utf-16"
    if raw.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    if b"\x00" in raw:
        return "utf-16"
    return "utf-8"

def read_header_line(path: str, encoding: str) -> str:
    with open(path, "r", encoding=encoding, errors="ignore", newline="") as f:
        for line in f:
            if line.strip():
                return line.rstrip("\n\r")
    return ""

def parse_header_fields(header_line: str, delim: str) -> list:
    return [c.strip().lower().lstrip("\ufeff") for c in header_line.split(delim)]

def detect_delimiter_from_header(path: str, encoding: str) -> Tuple[Optional[str], Optional[list]]:
    header = read_header_line(path, encoding)
    if not header:
        return None, None
    for delim in ("\t", ";", ",", "|"):
        cols = parse_header_fields(header, delim)
        if "id" in cols and "spkid" in cols:
            return delim, cols
    return None, None

def detect_delimiter(path: str, encoding: str) -> str:
    """Tenta detectar o delimitador de forma mais determinística."""
    with open(path, "r", encoding=encoding, errors="ignore", newline="") as f:
        lines = [ln.rstrip("\n\r") for ln in f.readlines()[:5] if ln.strip()]

    if not lines:
        return ";"  # default

    first = lines[0]

    # heurísticas rápidas
    if ";" in first and first.count(";") >= first.count(","):
        return ";"
    if "\t" in first:
        return "\t"
    if "," in first:
        return ","

    # fallback: conta em até 5 linhas
    joined = "\n".join(lines)
    counts: Dict[str, int] = {
        ";": joined.count(";"),
        ",": joined.count(","),
        "\t": joined.count("\t"),
        "|": joined.count("|"),
    }
    best = max(counts.items(), key=lambda kv: kv[1])[0]
    return best or ";"

def normalize_row_keys(row: dict) -> dict:
    out = {}
    for k, v in row.items():
        if k is None:
            continue
        nk = str(k).strip().lower().lstrip("\ufeff")  # remove BOM
        out[nk] = v
    return out

def get_next_id_internal(cur) -> int:
    cur.execute("SELECT ISNULL(MAX(id_internal), 0) FROM Asteroid;")
    return int(cur.fetchone()[0]) + 1

def load_existing_maps(cur) -> Tuple[Dict[str,int], Dict[int,int]]:
    neo_map: Dict[str,int] = {}
    spk_map: Dict[int,int] = {}
    cur.execute("SELECT id_internal, neo_id, spkid FROM Asteroid;")
    for id_internal, neo_id, spkid in cur.fetchall():
        if neo_id is not None:
            neo_map[str(neo_id).strip().lower()] = int(id_internal)
        if spkid is not None:
            spk_map[int(spkid)] = int(id_internal)
    return neo_map, spk_map

def upsert_class(cur, cls: str, desc: str):
    if not cls:
        return
    cur.execute("""
        IF NOT EXISTS (SELECT 1 FROM Class_Orbital WHERE class = ?)
        INSERT INTO Class_Orbital(class, class_description) VALUES (?, ?);
    """, cls, cls, desc or cls)

def upsert_asteroid(cur, id_internal: int, neo_id: str, spkid: int,
                    full_name: str, pdes: str, name: Optional[str], prefix: str,
                    neo_flag: str, pha_flag: str,
                    diameter: Optional[float], h: float,
                    albedo: Optional[float], diameter_sigma: Optional[float]) -> str:

    # 1) Se já existe por spkid, atualiza esse
    cur.execute("SELECT id_internal FROM Asteroid WHERE spkid = ?", spkid)
    row = cur.fetchone()
    if row:
        cur.execute("""
            UPDATE Asteroid
            SET neo_id = COALESCE(neo_id, ?),
                full_name = ?, pdes = ?, name = ?, prefix = ?,
                neo_flag = ?, pha_flag = ?,
                diameter = ?, absolute_magnitude = ?, albedo = ?, diameter_sigma = ?
            WHERE spkid = ?;
        """, neo_id, full_name, pdes, name, prefix,
             neo_flag, pha_flag,
             diameter, h, albedo, diameter_sigma,
             spkid)
        return "update"

    # 2) Se não existe por spkid, mas já existe por neo_id (UNIQUE), atualiza esse
    cur.execute("SELECT id_internal FROM Asteroid WHERE neo_id = ?", neo_id)
    row = cur.fetchone()
    if row:
        cur.execute("""
            UPDATE Asteroid
            SET spkid = COALESCE(spkid, ?),
                full_name = ?, pdes = ?, name = ?, prefix = ?,
                neo_flag = ?, pha_flag = ?,
                diameter = ?, absolute_magnitude = ?, albedo = ?, diameter_sigma = ?
            WHERE neo_id = ?;
        """, spkid, full_name, pdes, name, prefix,
             neo_flag, pha_flag,
             diameter, h, albedo, diameter_sigma,
             neo_id)
        return "update"

    # 3) Inserir novo
    cur.execute("""
        INSERT INTO Asteroid(
          id_internal, spkid, full_name, pdes, name, prefix,
          neo_flag, pha_flag, diameter, absolute_magnitude, albedo, diameter_sigma,
          created_at, neo_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSDATETIME(), ?);
    """, id_internal, spkid, full_name, pdes, name, prefix,
         neo_flag, pha_flag, diameter, h, albedo, diameter_sigma,
         neo_id)
    return "insert"


def insert_orbit_if_new(cur, orbit_id: str, id_internal: int, cls: str,
                        epoch: Optional[float], epoch_mjd: Optional[float], epoch_cal: Optional[date], equinox: str,
                        rms: Optional[float], moid_ld: Optional[float], moid: Optional[float],
                        e: Optional[float], a: Optional[float], q: Optional[float], inc: Optional[float],
                        om: Optional[float], w: Optional[float], ma: Optional[float], ad: Optional[float],
                        n: Optional[float], tp: Optional[float], tp_cal: Optional[date],
                        per: Optional[float], per_y: Optional[float],
                        sigma_e: Optional[float], sigma_a: Optional[float], sigma_q: Optional[float], sigma_i: Optional[float],
                        sigma_om: Optional[float], sigma_w: Optional[float], sigma_ma: Optional[float], sigma_ad: Optional[float],
                        sigma_n: Optional[float], sigma_tp: Optional[float], sigma_per: Optional[float]) -> bool:
    cur.execute("SELECT 1 FROM Orbit WHERE id_orbita = ?", orbit_id)
    epoch_val = epoch if epoch is not None else (epoch_mjd if epoch_mjd is not None else 0.0)
    if cur.fetchone() is not None:
        cur.execute("""
            UPDATE Orbit
            SET epoch = ?,
                rms = ?,
                moid_ld = ?,
                epoch_mjd = ?,
                epoch_cal = ?,
                tp = ?,
                tp_cal = ?,
                per = ?,
                per_y = ?,
                equinox = ?,
                orbit_uncertainty = NULL,
                condition_code = NULL,
                e = ?,
                a = ?,
                q = ?,
                i = ?,
                om = ?,
                w = ?,
                ma = ?,
                ad = ?,
                n = ?,
                moid = ?,
                sigma_e = ?,
                sigma_a = ?,
                sigma_q = ?,
                sigma_i = ?,
                sigma_n = ?,
                sigma_ma = ?,
                sigma_om = ?,
                sigma_w = ?,
                sigma_ad = ?,
                sigma_tp = ?,
                sigma_per = ?,
                id_internal = ?,
                class = ?
            WHERE id_orbita = ?;
        """,
        epoch_val,
        rms or 0.0,
        moid_ld or 0.0,
        epoch_mjd,
        epoch_cal,
        tp or 0.0,
        tp_cal,
        per or 0.0,
        per_y or 0.0,
        equinox or "J2000",
        e or 0.0, a or 0.0, q or 0.0, inc or 0.0, om or 0.0, w or 0.0, ma or 0.0,
        ad or 0.0, n or 0.0, moid or 0.0,
        sigma_e, sigma_a, sigma_q, sigma_i, sigma_n, sigma_ma, sigma_om, sigma_w, sigma_ad, sigma_tp, sigma_per,
        id_internal, cls,
        orbit_id
        )
        return False

    if not cls:
        cls = "NEA"
        upsert_class(cur, cls, "Near Earth Asteroid")

    if tp_cal is None:
        tp_cal = epoch_cal if epoch_cal is not None else date.today()

    cur.execute("""
        INSERT INTO Orbit(
          id_orbita, epoch, rms, moid_ld, epoch_mjd, epoch_cal,
          tp, tp_cal, per, per_y, equinox,
          orbit_uncertainty, condition_code,
          e, a, q, i, om, w, ma, ad, n, moid,
          sigma_e, sigma_a, sigma_q, sigma_i, sigma_n, sigma_ma, sigma_om, sigma_w, sigma_ad, sigma_tp, sigma_per,
          id_internal, class
        )
        VALUES (
          ?, ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?,
          NULL, NULL,
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
          ?, ?
        );
    """,
    orbit_id, epoch_val, rms or 0.0, moid_ld or 0.0, epoch_mjd, epoch_cal,
    tp or 0.0, tp_cal, per or 0.0, per_y or 0.0, equinox or "J2000",
    e or 0.0, a or 0.0, q or 0.0, inc or 0.0, om or 0.0, w or 0.0, ma or 0.0, ad or 0.0, n or 0.0, moid or 0.0,
    sigma_e, sigma_a, sigma_q, sigma_i, sigma_n, sigma_ma, sigma_om, sigma_w, sigma_ad, sigma_tp, sigma_per,
    id_internal, cls
    )
    return True

def pick_csv_file() -> str:
    root = Tk()
    root.withdraw()           # não mostrar janela principal
    root.attributes("-topmost", True)
    path = askopenfilename(
        title="Seleciona um ficheiro CSV",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
    )
    root.destroy()
    return path

def load_neo_csv(conn: pyodbc.Connection, path: str) -> None:
    encoding = detect_encoding(path)
    header_line = read_header_line(path, encoding)
    delim, header_fields = detect_delimiter_from_header(path, encoding)
    if not header_line:
        print("[ERRO] CSV vazio ou sem header legivel.")
        return
    if not delim:
        delim = detect_delimiter(path, encoding)
        header_fields = parse_header_fields(header_line, delim)
    has_header = True
    if not header_fields or "id" not in header_fields or "spkid" not in header_fields:
        has_header = False
        header_fields = DEFAULT_NEO_HEADER
        print("[WARN] Header nao identificado. A usar cabecalho pre-definido.")
        print("[DEBUG] Primeira linha lida:", header_line[:200])
    cur = conn.cursor()
    ensure_reference_data(cur)

    neo_map, spk_map = load_existing_maps(cur)
    next_id = get_next_id_internal(cur)

    inserted_ast = updated_ast = inserted_orb = 0
    errors = 0

    with open(path, "r", encoding=encoding, errors="ignore", newline="") as f:
        if has_header:
            for line in f:
                if line.strip():
                    break
            start_line_no = 2
        else:
            start_line_no = 1
        reader = csv.DictReader(f, delimiter=delim, fieldnames=header_fields)

        for line_no, row in enumerate(reader, start=start_line_no):
            if not isinstance(row, dict):
                print("DEBUG row type:", type(row), row[:5] if isinstance(row, list) else row)
                continue

            row = normalize_row_keys(row)

            try:
                neo_id = (row.get("id") or "").strip()
                neo_key = neo_id.lower()
                spkid = parse_int(row.get("spkid") or "")
                orbit_id = (row.get("orbit_id") or "").strip()

                if not neo_id or spkid is None:
                    print(
                        "DEBUG missing:",
                        "neo_id=", neo_id,
                        "spkid_raw=", row.get("spkid"),
                        "keys=", list(row.keys())[:10]
                    )
                    errors += 1
                    log_error(cur, path, line_no, "Asteroid", "Missing id or spkid", str(row))
                    continue

                cls = (row.get("class") or "").strip()
                cls_desc = (row.get("class_description") or cls).strip()
                upsert_class(cur, cls, cls_desc)
                if neo_key in neo_map:
                    id_internal = neo_map[neo_key]
                elif spkid in spk_map:
                    id_internal = spk_map[spkid]
                    neo_map[neo_key] = id_internal
                else:
                    id_internal = next_id
                    next_id += 1
                    neo_map[neo_key] = id_internal
                    spk_map[spkid] = id_internal
                neo_flag = ((row.get("neo") or "N").strip().upper()[:1] or "N")
                pha_flag = ((row.get("pha") or "N").strip().upper()[:1] or "N")
                if neo_flag not in ("Y","N"): neo_flag = "N"
                if pha_flag not in ("Y","N"): pha_flag = "N"
                full_name = (row.get("full_name") or "").strip()[:100]
                pdes = (row.get("pdes") or "").strip()[:50]
                name = (row.get("name") or "").strip()[:100] or None
                prefix = (row.get("prefix") or "").strip()[:10] or ""
                h = parse_float(row.get("h") or "") or 0.0
                diameter = parse_float(row.get("diameter") or "")
                albedo = parse_float(row.get("albedo") or "")
                diameter_sigma = parse_float(row.get("diameter_sigma") or "")
                action = upsert_asteroid(
                    cur, id_internal, neo_id, spkid,
                    full_name, pdes, name, prefix,
                    neo_flag, pha_flag,
                    diameter, h, albedo, diameter_sigma
                )
                if action == "insert":
                    inserted_ast += 1
                else:
                    updated_ast += 1
                if orbit_id and (row.get("epoch_mjd") or row.get("epoch_cal") or row.get("tp_cal")):
                    epoch = parse_float(row.get("epoch") or "")
                    epoch_mjd = parse_float(row.get("epoch_mjd") or "")
                    epoch_cal = parse_date(row.get("epoch_cal") or "")
                    equinox = (row.get("equinox") or "J2000").strip()
                    rms = parse_float(row.get("rms") or "")
                    moid_ld = parse_float(row.get("moid_ld") or "")
                    moid = parse_float(row.get("moid") or "")
                    e = parse_float(row.get("e") or "")
                    a = parse_float(row.get("a") or "")
                    q = parse_float(row.get("q") or "")
                    inc = parse_float(row.get("i") or "")
                    om = parse_float(row.get("om") or "")
                    w = parse_float(row.get("w") or "")
                    ma = parse_float(row.get("ma") or "")
                    ad = parse_float(row.get("ad") or "")
                    n = parse_float(row.get("n") or "")
                    tp = parse_float(row.get("tp") or "")
                    tp_cal = parse_date(row.get("tp_cal") or "")
                    per = parse_float(row.get("per") or "")
                    per_y = parse_float(row.get("per_y") or "")
                    sigma_e = parse_float(row.get("sigma_e") or "")
                    sigma_a = parse_float(row.get("sigma_a") or "")
                    sigma_q = parse_float(row.get("sigma_q") or "")
                    sigma_i = parse_float(row.get("sigma_i") or "")
                    sigma_om = parse_float(row.get("sigma_om") or "")
                    sigma_w = parse_float(row.get("sigma_w") or "")
                    sigma_ma = parse_float(row.get("sigma_ma") or "")
                    sigma_ad = parse_float(row.get("sigma_ad") or "")
                    sigma_n = parse_float(row.get("sigma_n") or "")
                    sigma_tp = parse_float(row.get("sigma_tp") or "")
                    sigma_per = parse_float(row.get("sigma_per") or "")
                    if tp_cal is None:
                        tp_cal = epoch_cal if epoch_cal is not None else datetime.today().date()
                    inserted = insert_orbit_if_new(
                        cur, orbit_id, id_internal, cls,
                        epoch, epoch_mjd, epoch_cal, equinox,
                        rms, moid_ld, moid,
                        e, a, q, inc, om, w, ma, ad, n,
                        tp, tp_cal, per, per_y,
                        sigma_e, sigma_a, sigma_q, sigma_i,
                        sigma_om, sigma_w, sigma_ma, sigma_ad,
                        sigma_n, sigma_tp, sigma_per
                    )
                    if inserted:
                        inserted_orb += 1
                        
            except Exception as ex:
                errors += 1
                log_error(cur, path, line_no, "Loader", f"Unhandled error: {ex}", str(row))
                
            if (line_no % 1000) == 0:
                conn.commit()

    conn.commit()
    cur.close()

    print("\n=== RESULTADO (NEO CSV) ===")
    print(f"Asteroids inseridos:   {inserted_ast}")
    print(f"Asteroids atualizados: {updated_ast}")
    print(f"Orbits inseridas:      {inserted_orb}")
    print(f"Erros:                 {errors}")
    print("==========================\n")

# ----------------- Menu app -----------------
def find_stor_cfg_nearby() -> Optional[str]:
    # procura no diretório atual
    cwd = os.getcwd()
    for name in POSSIBLE_STOR_CFG_NAMES:
        p = os.path.join(cwd, name)
        if os.path.isfile(p):
            return p
    return None

def prompt_connection_cfg(existing: Optional[dict] = None) -> dict:
    existing = existing or {"server":"", "port":"", "user":"", "password":"", "database":""}
    cfg = {}
    cfg["server"] = safe_input("Servidor (IP/Nome)", existing.get("server",""))
    cfg["port"] = safe_input("Porta (opcional)", existing.get("port",""))
    cfg["user"] = safe_input("Utilizador", existing.get("user","sa") or "sa")
    cfg["password"] = safe_input("Password", existing.get("password",""))
    cfg["database"] = safe_input("Base de Dados", existing.get("database","NEOs") or "NEOs")
    return cfg

def test_connection(cfg: dict) -> bool:
    try:
        conn = connect(cfg)
        cur = conn.cursor()

        cur.execute("SELECT DB_NAME()")
        row = cur.fetchone()
        db = row[0] if row else "<desconhecida>"

        cur.close()
        conn.close()

        print(f"[OK] ligação bem-sucedida à BD: {db}")
        return True

    except Exception as ex:
        print(f"[ERRO] Falha na ligação: {ex}")
        return False

def main():
    # tenta carregar config do loader
    loader_cfg = load_loader_config(DEFAULT_LOADER_CONFIG)

    # tenta também ler config do stor (se existir)
    stor_path = find_stor_cfg_nearby()
    stor_cfg = try_read_stor_cfg(stor_path) if stor_path else None

    active_cfg = loader_cfg or stor_cfg

    while True:
        print("==== App Inserção CSV (NEOs) ====")
        print("1) Ligar / Configurar ligação")
        print("2) Guardar configuração (loader_config.json)")
        print("3) Carregar configuração (loader_config.json)")
        if stor_path:
            print(f"4) Tentar usar config do stor ({os.path.basename(stor_path)})")
        print("5) Testar ligação")
        print("6) Carregar CSV NEO (asteroids + orbits)")
        print("7) Carregar mpcorb.csv (orbits MPC)")
        print("0) Sair")
        op = input("Escolha: ").strip()

        if op == "0":
            break

        elif op == "1":
            active_cfg = prompt_connection_cfg(active_cfg)
            print("[INFO] Configuração atual pronta.")

        elif op == "2":
            if not active_cfg:
                print("[ERRO] Ainda não tens configuração.")
                continue
            save_loader_config(active_cfg, DEFAULT_LOADER_CONFIG)
            print(f"[OK] Guardado em {DEFAULT_LOADER_CONFIG}")

        elif op == "3":
            c = load_loader_config(DEFAULT_LOADER_CONFIG)
            if not c:
                print("[ERRO] Não existe loader_config.json ou está inválido.")
            else:
                active_cfg = c
                print("[OK] Configuração carregada.")

        elif op == "4" and stor_path:
            c = try_read_stor_cfg(stor_path)
            if not c:
                print("[ERRO] Não consegui ler/interpretar a config do stor (pode ser binária).")
                print("Sugestão: usa a opção 1 e depois guarda em loader_config.json.")
            else:
                active_cfg = c
                print("[OK] Config do stor aplicada (se as chaves eram legíveis).")

        elif op == "5":
            if not active_cfg:
                print("[ERRO] Primeiro configura a ligação (opção 1).")
                continue
            test_connection(active_cfg)

        elif op == "6":
            if not active_cfg:
                print("[ERRO] Primeiro configura a ligação (opção 1).")
                continue
            csv_path = pick_csv_file()
            if not csv_path:
                print("[INFO] Nenhum ficheiro escolhido.")
                continue
            if not os.path.isfile(csv_path):
                print("[ERRO] Ficheiro não existe.")
                continue
            # liga e carrega
            conn = connect(active_cfg)
            try:
                load_neo_csv(conn, csv_path)
                print("[OK] CSV carregado.")
            finally:
                conn.close()
        
        elif op == "7":
            caminho = pick_csv_file()
            if not caminho:
                print("Cancelado.")
            else:
                load_mpcorb(caminho)

if __name__ == "__main__":
    main()
