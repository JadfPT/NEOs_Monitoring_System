import os
import json
import csv
import io
import threading
import queue
from contextlib import redirect_stdout
from tkinter import filedialog
import pyodbc
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Tuple, Any, cast
import tkinter as tk
from tkinter import Tk
from tkinter import ttk, messagebox, font as tkfont

import generate_sql as gen_sql
DEFAULT_MERGED_HEADER = [
    "id", "spkid", "full_name", "pdes", "name", "prefix", "neo", "pha", "h",
    "diameter", "albedo", "diameter_sigma", "orbit_id", "epoch", "epoch_mjd",
    "epoch_cal", "equinox", "e", "a", "q", "i", "om", "w", "ma", "ad", "n",
    "tp", "tp_cal", "per", "per_y", "moid", "moid_ld", "sigma_e", "sigma_a",
    "sigma_q", "sigma_i", "sigma_om", "sigma_w", "sigma_ma", "sigma_ad",
    "sigma_n", "sigma_tp", "sigma_per", "class", "rms", "class_description",
    "abs_mag", "slope_param", "epoch_mpc", "mean_anomaly", "arg_perihelion",
    "long_asc_node", "inclination", "eccentricity", "mean_motion",
    "semi_major_axis", "uncertainty", "reference", "num_observations",
    "num_oppositions", "first_obs", "separator", "last_obs", "rms_residual",
    "coarse_perturbers", "precise_perturbers", "computer", "hex_flags",
    "designation_full", "last_obs_date", "orbit_type", "is_neo"
]

# ----------------- Config paths -----------------
DEFAULT_LOADER_CONFIG = "loader_config.json"
DEFAULT_FINAL_CSV = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "Ficheiros .csv", "neo_mpcorb_final.csv")
)

# ----------------- Helpers -----------------
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

def test_connection(cfg: dict) -> bool:
    try:
        conn = connect(cfg)
        cur = conn.cursor()
        cur.execute("SELECT DB_NAME()")
        row = cur.fetchone()
        db = row[0] if row else "<desconhecida>"
        cur.close()
        conn.close()
        print(f"[OK] ligacao bem-sucedida - BD: {db}")
        return True
    except Exception as ex:
        print(f"[ERRO] Falha na ligacao: {ex}")
        return False

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

def mpc_packed_to_date(packed: str) -> Optional[date]:
    packed = (packed or "").strip()
    if len(packed) != 5:
        return None
    century_map = {"I": 1800, "J": 1900, "K": 2000}
    c = packed[0]
    if c not in century_map or not packed[1:3].isdigit():
        return None
    year = century_map[c] + int(packed[1:3])

    def decode_md(ch: str) -> Optional[int]:
        if ch.isdigit():
            v = int(ch)
            return v if 1 <= v <= 9 else None
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        if ch in alphabet:
            return 10 + alphabet.index(ch)
        return None

    month = decode_md(packed[3])
    day = decode_md(packed[4])
    if not month or not day:
        return None
    try:
        return date(year, month, day)
    except Exception:
        return None

def date_to_mjd(d: date) -> float:
    mjd0 = date(1858, 11, 17)
    return float((d - mjd0).days)

def mjd_to_date(mjd: float) -> date:
    mjd0 = date(1858, 11, 17)
    return mjd0 + timedelta(days=int(mjd))

def norm_text(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    v = str(x).strip()
    if v == "" or v.upper() == "NULL":
        return None
    return v

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

def ensure_unique_header_fields(fields: list) -> list:
    counts = {}
    out = []
    for c in fields:
        if c in counts:
            counts[c] += 1
            if c == "epoch":
                out.append("epoch_mpc")
            else:
                out.append(f"{c}_dup{counts[c]}")
        else:
            counts[c] = 1
            out.append(c)
    return out

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
    row = cur.fetchone()
    return int(row[0] if row else 0) + 1

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

def get_next_mpc_seq(cur) -> int:
    cur.execute("""
        SELECT ISNULL(MAX(TRY_CONVERT(int, SUBSTRING(id_orbita, 4, 50))), 0)
        FROM Orbit
        WHERE id_orbita LIKE 'MPC%';
    """)
    row = cur.fetchone()
    return int(row[0] or 0) + 1

def upsert_class(cur, cls: str, desc: str):
    if not cls:
        return
    cur.execute("""
        IF NOT EXISTS (SELECT 1 FROM Class_Orbital WHERE class = ?)
        INSERT INTO Class_Orbital(class, class_description) VALUES (?, ?);
    """, cls, cls, desc or cls)

def upsert_asteroid(cur, id_internal: int, neo_id: Optional[str], spkid: Optional[int],
                    full_name: str, pdes: str, name: Optional[str], prefix: str,
                    neo_flag: str, pha_flag: str,
                    diameter: Optional[float], h: Optional[float],
                    albedo: Optional[float], diameter_sigma: Optional[float]) -> str:

    # 1) Se já existe por spkid, atualiza esse
    cur.execute("SELECT id_internal FROM Asteroid WHERE spkid = ?", spkid)
    row = cur.fetchone()
    if row:
        cur.execute("""
            UPDATE Asteroid
            SET neo_id = COALESCE(neo_id, ?),
                full_name = CASE WHEN full_name IS NULL OR full_name = '' THEN NULLIF(?, '') ELSE full_name END,
                pdes = CASE WHEN pdes IS NULL OR pdes = '' THEN NULLIF(?, '') ELSE pdes END,
                name = CASE WHEN name IS NULL OR name = '' THEN NULLIF(?, '') ELSE name END,
                prefix = CASE WHEN prefix IS NULL OR prefix = '' THEN COALESCE(NULLIF(?, ''), '') ELSE prefix END,
                neo_flag = CASE WHEN neo_flag IS NULL OR neo_flag = '' THEN NULLIF(?, '') ELSE neo_flag END,
                pha_flag = CASE WHEN pha_flag IS NULL OR pha_flag = '' THEN NULLIF(?, '') ELSE pha_flag END,
                diameter = COALESCE(diameter, ?),
                absolute_magnitude = COALESCE(absolute_magnitude, ?),
                albedo = COALESCE(albedo, ?),
                diameter_sigma = COALESCE(diameter_sigma, ?)
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
                full_name = CASE WHEN full_name IS NULL OR full_name = '' THEN NULLIF(?, '') ELSE full_name END,
                pdes = CASE WHEN pdes IS NULL OR pdes = '' THEN NULLIF(?, '') ELSE pdes END,
                name = CASE WHEN name IS NULL OR name = '' THEN NULLIF(?, '') ELSE name END,
                prefix = CASE WHEN prefix IS NULL OR prefix = '' THEN COALESCE(NULLIF(?, ''), '') ELSE prefix END,
                neo_flag = CASE WHEN neo_flag IS NULL OR neo_flag = '' THEN NULLIF(?, '') ELSE neo_flag END,
                pha_flag = CASE WHEN pha_flag IS NULL OR pha_flag = '' THEN NULLIF(?, '') ELSE pha_flag END,
                diameter = COALESCE(diameter, ?),
                absolute_magnitude = COALESCE(absolute_magnitude, ?),
                albedo = COALESCE(albedo, ?),
                diameter_sigma = COALESCE(diameter_sigma, ?)
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
         neo_flag, pha_flag, diameter, (h if h is not None else 0.0), albedo, diameter_sigma,
         neo_id)
    return "insert"


def insert_orbit_if_new(cur, orbit_id: str, id_internal: Optional[int], cls: str,
                        epoch: Optional[float], epoch_mjd: Optional[float], epoch_cal: Optional[date], equinox: str,
                        rms: Optional[float], moid_ld: Optional[float], moid: Optional[float],
                        e: Optional[float], a: Optional[float], q: Optional[float], inc: Optional[float],
                        om: Optional[float], w: Optional[float], ma: Optional[float], ad: Optional[float],
                        n: Optional[float], tp: Optional[float], tp_cal: Optional[date],
                        per: Optional[float], per_y: Optional[float],
                        sigma_e: Optional[float], sigma_a: Optional[float], sigma_q: Optional[float], sigma_i: Optional[float],
                        sigma_om: Optional[float], sigma_w: Optional[float], sigma_ma: Optional[float], sigma_ad: Optional[float],
                        sigma_n: Optional[float], sigma_tp: Optional[float], sigma_per: Optional[float],
                        orbit_uncertainty: Optional[int], condition_code: Optional[int]) -> bool:
    cur.execute("SELECT id_internal FROM Orbit WHERE id_orbita = ?", orbit_id)
    epoch_val = epoch if epoch is not None else (epoch_mjd if epoch_mjd is not None else None)
    row = cur.fetchone()
    if row is not None:
        existing_id = row[0]
        if existing_id is not None and id_internal is not None and int(existing_id) != int(id_internal):
            print(f"[WARN] Orbit id {orbit_id} pertence a id_internal={existing_id}, skip update.")
            return False
        cur.execute("""
            UPDATE Orbit
            SET epoch = COALESCE(epoch, ?),
                rms = COALESCE(rms, ?),
                moid_ld = COALESCE(moid_ld, ?),
                epoch_mjd = COALESCE(epoch_mjd, ?),
                epoch_cal = COALESCE(epoch_cal, ?),
                tp = COALESCE(tp, ?),
                tp_cal = COALESCE(tp_cal, ?),
                per = COALESCE(per, ?),
                per_y = COALESCE(per_y, ?),
                equinox = CASE WHEN equinox IS NULL OR equinox = '' THEN NULLIF(?, '') ELSE equinox END,
                orbit_uncertainty = COALESCE(orbit_uncertainty, ?),
                condition_code = COALESCE(condition_code, ?),
                e = COALESCE(e, ?),
                a = COALESCE(a, ?),
                q = COALESCE(q, ?),
                i = COALESCE(i, ?),
                om = COALESCE(om, ?),
                w = COALESCE(w, ?),
                ma = COALESCE(ma, ?),
                ad = COALESCE(ad, ?),
                n = COALESCE(n, ?),
                moid = COALESCE(moid, ?),
                sigma_e = COALESCE(sigma_e, ?),
                sigma_a = COALESCE(sigma_a, ?),
                sigma_q = COALESCE(sigma_q, ?),
                sigma_i = COALESCE(sigma_i, ?),
                sigma_n = COALESCE(sigma_n, ?),
                sigma_ma = COALESCE(sigma_ma, ?),
                sigma_om = COALESCE(sigma_om, ?),
                sigma_w = COALESCE(sigma_w, ?),
                sigma_ad = COALESCE(sigma_ad, ?),
                sigma_tp = COALESCE(sigma_tp, ?),
                sigma_per = COALESCE(sigma_per, ?),
                id_internal = COALESCE(id_internal, ?),
                class = CASE WHEN class IS NULL OR class = '' THEN NULLIF(?, '') ELSE class END
            WHERE id_orbita = ?;
        """,
        epoch_val,
        rms,
        moid_ld,
        epoch_mjd,
        epoch_cal,
        tp,
        tp_cal,
        per,
        per_y,
        equinox,
        orbit_uncertainty,
        condition_code,
        e, a, q, inc, om, w, ma,
        ad, n, moid,
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
          ?, ?,
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
          ?, ?
        );
    """,
    orbit_id, (epoch_val if epoch_val is not None else 0.0), rms or 0.0, moid_ld or 0.0, epoch_mjd, epoch_cal,
    tp or 0.0, tp_cal, per or 0.0, per_y or 0.0, equinox or "J2000",
    orbit_uncertainty, condition_code,
    e or 0.0, a or 0.0, q or 0.0, inc or 0.0, om or 0.0, w or 0.0, ma or 0.0, ad or 0.0, n or 0.0, moid or 0.0,
    sigma_e, sigma_a, sigma_q, sigma_i, sigma_n, sigma_ma, sigma_om, sigma_w, sigma_ad, sigma_tp, sigma_per,
    id_internal, cls
    )
    return True

def load_neo_mpcorb_csv(conn: pyodbc.Connection, path: str) -> None:
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
        header_fields = DEFAULT_MERGED_HEADER
        print("[WARN] Header nao identificado. A usar cabecalho pre-definido.")
        print("[DEBUG] Primeira linha lida:", header_line[:200])
    header_fields = ensure_unique_header_fields(header_fields)

    cur = conn.cursor()
    ensure_reference_data(cur)

    neo_map, spk_map = load_existing_maps(cur)
    next_id = get_next_id_internal(cur)
    mpc_seq = get_next_mpc_seq(cur)

    inserted_ast = updated_ast = inserted_orb = 0
    errors = 0
    missing_keys = 0
    error_counts: Dict[str, int] = {}
    error_samples = []

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
                continue

            row = normalize_row_keys(row)

            try:
                neo_id = norm_text(row.get("id"))
                spkid = parse_int(row.get("spkid") or "")
                orbit_id = norm_text(row.get("orbit_id"))
                neo_key = neo_id.lower() if neo_id else None

                if not neo_id and spkid is None:
                    missing_keys += 1

                cls = (row.get("class") or "").strip()
                orbit_type = (row.get("orbit_type") or "").strip()
                if not cls and orbit_type:
                    cls = orbit_type[:20]
                cls_desc = (row.get("class_description") or orbit_type or cls).strip()
                upsert_class(cur, cls, cls_desc)

                id_internal = None
                if neo_key:
                    if neo_key in neo_map:
                        id_internal = neo_map[neo_key]
                    elif spkid is not None and spkid in spk_map:
                        id_internal = spk_map[spkid]
                        neo_map[neo_key] = id_internal
                    else:
                        id_internal = next_id
                        next_id += 1
                        neo_map[neo_key] = id_internal
                        if spkid is not None:
                            spk_map[spkid] = id_internal
                elif spkid is not None:
                    if spkid in spk_map:
                        id_internal = spk_map[spkid]
                    else:
                        id_internal = next_id
                        next_id += 1
                        spk_map[spkid] = id_internal

                neo_flag = ((row.get("neo") or "N").strip().upper()[:1] or "N")
                pha_flag = ((row.get("pha") or "N").strip().upper()[:1] or "N")
                if neo_flag not in ("Y", "N"):
                    neo_flag = "N"
                if pha_flag not in ("Y", "N"):
                    pha_flag = "N"

                designation = norm_text(row.get("designation")) or ""
                designation_full = norm_text(row.get("designation_full")) or ""
                full_name = norm_text(row.get("full_name"))
                if not full_name:
                    full_name = (designation_full or designation or "UNKNOWN")[:100]
                else:
                    full_name = full_name[:100]
                pdes = norm_text(row.get("pdes"))
                if not pdes:
                    pdes = (designation or designation_full or "UNKNOWN")[:50]
                else:
                    pdes = pdes[:50]
                name = norm_text(row.get("name"))
                if name:
                    name = name[:100]
                prefix = (norm_text(row.get("prefix")) or "")[:10]
                h = parse_float(row.get("h") or "")
                if h is None:
                    h = parse_float(row.get("abs_mag") or "")
                diameter = parse_float(row.get("diameter") or "")
                albedo = parse_float(row.get("albedo") or "")
                diameter_sigma = parse_float(row.get("diameter_sigma") or "")

                if id_internal is not None:
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

                if not orbit_id:
                    orbit_id = f"MPC{mpc_seq}"
                    mpc_seq += 1

                if orbit_id:
                    epoch = parse_float(row.get("epoch") or "")
                    epoch_mjd = parse_float(row.get("epoch_mjd") or "")
                    epoch_cal = parse_date(row.get("epoch_cal") or "")
                    equinox = (row.get("equinox") or "J2000").strip()

                    epoch_mpc = (row.get("epoch_mpc") or "").strip()
                    if not epoch and not epoch_mjd and not epoch_cal and epoch_mpc:
                        epoch_cal = mpc_packed_to_date(epoch_mpc)
                        if epoch_cal is not None:
                            epoch_mjd = date_to_mjd(epoch_cal)
                            epoch = epoch_mjd + 2400000.5

                    rms = parse_float(row.get("rms") or "")
                    if rms is None:
                        rms = parse_float(row.get("rms_residual") or "")
                    moid_ld = parse_float(row.get("moid_ld") or "")
                    moid = parse_float(row.get("moid") or "")
                    e = parse_float(row.get("e") or "")
                    if e is None:
                        e = parse_float(row.get("eccentricity") or "")
                    a = parse_float(row.get("a") or "")
                    if a is None:
                        a = parse_float(row.get("semi_major_axis") or "")
                    q = parse_float(row.get("q") or "")
                    if q is None and a is not None and e is not None:
                        q = a * (1.0 - e)
                    inc = parse_float(row.get("i") or "")
                    if inc is None:
                        inc = parse_float(row.get("inclination") or "")
                    om = parse_float(row.get("om") or "")
                    if om is None:
                        om = parse_float(row.get("long_asc_node") or "")
                    w = parse_float(row.get("w") or "")
                    if w is None:
                        w = parse_float(row.get("arg_perihelion") or "")
                    ma = parse_float(row.get("ma") or "")
                    if ma is None:
                        ma = parse_float(row.get("mean_anomaly") or "")
                    ad = parse_float(row.get("ad") or "")
                    if ad is None and a is not None and e is not None:
                        ad = a * (1.0 + e)
                    n = parse_float(row.get("n") or "")
                    if n is None:
                        n = parse_float(row.get("mean_motion") or "")
                    tp = parse_float(row.get("tp") or "")
                    tp_cal = parse_date(row.get("tp_cal") or "")
                    per = parse_float(row.get("per") or "")
                    per_y = parse_float(row.get("per_y") or "")
                    if per is None and n:
                        per = 360.0 / n
                        per_y = per / 365.25 if per else None

                    if tp is None and epoch and n and ma is not None:
                        tp_jd = epoch - (ma / n)
                        tp = tp_jd
                        tp_mjd = tp_jd - 2400000.5
                        tp_cal = mjd_to_date(tp_mjd)

                    if tp_cal is None:
                        tp_cal = epoch_cal if epoch_cal is not None else datetime.today().date()

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

                    orbit_uncertainty = parse_int(row.get("uncertainty") or "")

                    inserted = insert_orbit_if_new(
                        cur, orbit_id, id_internal, cls,
                        epoch, epoch_mjd, epoch_cal, equinox,
                        rms, moid_ld, moid,
                        e, a, q, inc, om, w, ma, ad, n,
                        tp, tp_cal, per, per_y,
                        sigma_e, sigma_a, sigma_q, sigma_i,
                        sigma_om, sigma_w, sigma_ma, sigma_ad,
                        sigma_n, sigma_tp, sigma_per,
                        orbit_uncertainty, None
                    )
                    if inserted:
                        inserted_orb += 1

            except Exception as ex:
                errors += 1
                msg = str(ex)
                error_counts[msg] = error_counts.get(msg, 0) + 1
                if len(error_samples) < 5:
                    error_samples.append((line_no, row.get("id"), row.get("spkid"), row.get("orbit_id"), msg))
                log_error(cur, path, line_no, "Loader", f"Unhandled error: {ex}", str(row))

            if (line_no % 1000) == 0:
                conn.commit()

    conn.commit()
    cur.close()

    print("\n=== RESULTADO (NEO+MPCORB CSV) ===")
    print(f"Asteroids inseridos:   {inserted_ast}")
    print(f"Asteroids atualizados: {updated_ast}")
    print(f"Orbits inseridas:      {inserted_orb}")
    print(f"Erros:                 {errors}")
    print(f"Linhas sem id/spkid:   {missing_keys}")
    if error_samples:
        print("Exemplos de erro (linha, id, spkid, orbit_id, erro):")
        for ln, rid, rspk, rorb, emsg in error_samples:
            print(f"  {ln} | id={rid} | spkid={rspk} | orbit_id={rorb} | {emsg}")
    if error_counts:
        top = sorted(error_counts.items(), key=lambda kv: kv[1], reverse=True)[:5]
        print("Erros mais frequentes:")
        for emsg, cnt in top:
            print(f"  {cnt}x | {emsg}")
    print("===============================\n")

class QueueWriter(io.TextIOBase):
    def __init__(self, q: "queue.Queue[str]") -> None:
        self.q = q

    def write(self, s: str) -> int:
        if s:
            self.q.put(s)
        return len(s)

    def flush(self) -> None:
        return None

def run_gui() -> None:
    root = Tk()
    root.title("Aplicacao Cliente para SQL Server")
    root.geometry("880x620")

    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass
    root.configure(bg="#f6f7f9")
    default_font = tkfont.nametofont("TkDefaultFont")
    default_font.configure(family="Segoe UI", size=10)
    style.configure("TFrame", background="#f6f7f9")
    style.configure("TNotebook", background="#f6f7f9", borderwidth=0)
    style.configure("TNotebook.Tab", padding=(12, 6))
    style.configure("TLabel", background="#f6f7f9")
    style.configure("TButton", padding=(10, 4))
    style.configure("TLabelframe", background="#f6f7f9")
    style.configure("TLabelframe.Label", background="#f6f7f9", font=("Segoe UI", 10, "bold"))
    style.configure("Header.TLabel", font=("Segoe UI", 11, "bold"), background="#f6f7f9")
    style.configure("Muted.TLabel", foreground="#666666", background="#f6f7f9")
    style.configure("Treeview", rowheight=22)

    q: "queue.Queue[str]" = queue.Queue()
    q_alerts: "queue.Queue[tuple[str, list]]" = queue.Queue()
    q_gen: "queue.Queue[tuple[str, str]]" = queue.Queue()
    q_monitor: "queue.Queue[tuple[str, object]]" = queue.Queue()
    q_obs: "queue.Queue[tuple[str, Any]]" = queue.Queue()
    writer = QueueWriter(q)

    var_server = tk.StringVar(value="")
    var_port = tk.StringVar(value="")
    var_user = tk.StringVar(value="")
    var_password = tk.StringVar(value="")
    var_database = tk.StringVar(value="")
    status_var = tk.StringVar(value="Nao ligado.")

    csv_default = DEFAULT_FINAL_CSV if os.path.isfile(DEFAULT_FINAL_CSV) else ""
    var_csv = tk.StringVar(value=csv_default)
    var_notify_high = tk.BooleanVar(value=False)

    base_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    template_default = os.path.join(base_root, gen_sql.TEMPLATE_SQL)
    output_default = os.path.join(base_root, gen_sql.OUTPUT_SQL)
    var_template = tk.StringVar(value=template_default)
    var_gen_csv = tk.StringVar(value=csv_default)
    var_output = tk.StringVar(value=output_default)

    notebook = ttk.Notebook(root)
    tab_conn = ttk.Frame(notebook)
    tab_load = ttk.Frame(notebook)
    tab_gen = ttk.Frame(notebook)
    tab_obs = ttk.Frame(notebook)
    tab_monitor = ttk.Frame(notebook)
    tab_alert = ttk.Frame(notebook)
    notebook.add(tab_conn, text="Ligar")
    notebook.add(tab_gen, text="Gerar SQL")
    notebook.add(tab_load, text="Atualizar BD")
    notebook.add(tab_obs, text="Observacoes")
    notebook.add(tab_monitor, text="Monitorizacao")
    notebook.add(tab_alert, text="Alertas")
    notebook.pack(fill="both", expand=True, padx=8, pady=8)

    # --- Tab Ligar ---
    conn_form = ttk.Frame(tab_conn)
    conn_form.pack(fill="x", padx=10, pady=10)

    def add_row(row: int, label: str, var: tk.StringVar, show: Optional[str] = None) -> None:
        ttk.Label(conn_form, text=label).grid(row=row, column=0, sticky="w", padx=6, pady=4)
        entry = ttk.Entry(conn_form, textvariable=var, show=show or "")
        entry.grid(row=row, column=1, sticky="we", padx=6, pady=4)
        conn_form.grid_columnconfigure(1, weight=1)

    add_row(0, "Servidor (IP/Nome):", var_server)
    add_row(1, "Porta (opcional):", var_port)
    add_row(2, "Utilizador:", var_user)
    add_row(3, "Password:", var_password, show="*")
    add_row(4, "Base de Dados:", var_database)

    status_label = ttk.Label(tab_conn, textvariable=status_var, foreground="gray")
    status_label.pack(anchor="w", padx=16, pady=(0, 8))

    btn_frame = ttk.Frame(tab_conn)
    btn_frame.pack(anchor="w", padx=16, pady=6)

    def cfg_from_fields() -> dict:
        return {
            "server": var_server.get().strip(),
            "port": var_port.get().strip(),
            "user": var_user.get().strip(),
            "password": var_password.get(),
            "database": var_database.get().strip(),
        }

    def set_status(msg: str, ok: Optional[bool] = None) -> None:
        status_var.set(msg)
        if ok is True:
            status_label.configure(foreground="green")
        elif ok is False:
            status_label.configure(foreground="red")
        else:
            status_label.configure(foreground="gray")

    def set_tabs_enabled(connected: bool) -> None:
        state = "normal" if connected else "disabled"
        notebook.tab(2, state=state)  # Atualizar BD
        notebook.tab(3, state=state)  # Observacoes
        notebook.tab(4, state=state)  # Monitorizacao
        notebook.tab(5, state=state)  # Alertas

    def on_test_connection() -> None:
        cfg = cfg_from_fields()
        try:
            ok = test_connection(cfg)
            if ok:
                set_status("Ligacao bem-sucedida!", True)
                set_tabs_enabled(True)
            else:
                set_status("Falha na ligacao.", False)
                set_tabs_enabled(False)
        except Exception as ex:
            set_status(f"Erro: {ex}", False)
            set_tabs_enabled(False)

    def on_save_cfg() -> None:
        cfg = cfg_from_fields()
        cfg["notify_high"] = bool(var_notify_high.get())
        if os.path.isfile(DEFAULT_LOADER_CONFIG):
            ok = messagebox.askyesno(
                "Config",
                "Ja existe uma configuracao guardada. Queres substituir?"
            )
            if not ok:
                return
        save_loader_config(cfg, DEFAULT_LOADER_CONFIG)
        set_status("Configuracao guardada.", True)

    def on_load_cfg() -> None:
        cfg = load_loader_config(DEFAULT_LOADER_CONFIG)
        if not cfg:
            messagebox.showwarning("Config", "Nao existe loader_config.json valido.")
            return
        var_server.set(cfg.get("server", ""))
        var_port.set(cfg.get("port", ""))
        var_user.set(cfg.get("user", "sa"))
        var_password.set(cfg.get("password", ""))
        var_database.set(cfg.get("database", "NEOs"))
        var_notify_high.set(bool(cfg.get("notify_high", False)))
        set_status("Configuracao carregada.", True)
        set_tabs_enabled(False)

    ttk.Button(btn_frame, text="Ligar a BD", command=on_test_connection).grid(row=0, column=0, padx=4, pady=4)
    ttk.Button(btn_frame, text="Carregar Configuracao", command=on_load_cfg).grid(row=0, column=1, padx=4, pady=4)
    ttk.Button(btn_frame, text="Guardar Configuracao", command=on_save_cfg).grid(row=0, column=2, padx=4, pady=4)
    set_tabs_enabled(False)

    cfg = load_loader_config(DEFAULT_LOADER_CONFIG)
    if cfg:
        var_server.set(cfg.get("server", ""))
        var_port.set(cfg.get("port", ""))
        var_user.set(cfg.get("user", "sa"))
        var_password.set(cfg.get("password", ""))
        var_database.set(cfg.get("database", "NEOs"))
        var_notify_high.set(bool(cfg.get("notify_high", False)))
        set_status("Configuracao carregada.", True)

    # --- Tab Atualizar BD ---
    load_top = ttk.Frame(tab_load)
    load_top.pack(fill="x", padx=10, pady=8)
    ttk.Label(load_top, text="CSV:").grid(row=0, column=0, sticky="w", padx=4, pady=4)
    csv_entry = ttk.Entry(load_top, textvariable=var_csv)
    csv_entry.grid(row=0, column=1, sticky="we", padx=4, pady=4)
    load_top.grid_columnconfigure(1, weight=1)

    def on_browse() -> None:
        path = filedialog.askopenfilename(
            title="Seleciona o CSV",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if path:
            var_csv.set(path)

    def on_run() -> None:
        cfg = cfg_from_fields()
        csv_path = var_csv.get().strip()
        if not csv_path:
            messagebox.showwarning("CSV", "Seleciona um ficheiro CSV.")
            return
        if not os.path.isfile(csv_path):
            messagebox.showerror("CSV", "Ficheiro nao existe.")
            return
        run_button.configure(state="disabled")
        set_status("A carregar CSV...", None)
        output_text.configure(state="normal")
        output_text.insert("end", f"[INFO] A iniciar carregamento: {csv_path}\n")
        output_text.configure(state="disabled")

        def worker() -> None:
            conn = None
            try:
                conn = connect(cfg)
                with redirect_stdout(writer):  # type: ignore[arg-type]
                    load_neo_mpcorb_csv(conn, csv_path)
            except Exception as ex:
                q.put(f"[ERRO] {ex}\n")
            finally:
                if conn:
                    conn.close()
                q.put("__DONE__")

        threading.Thread(target=worker, daemon=True).start()

    def poll_queue() -> None:
        try:
            while True:
                msg = q.get_nowait()
                if msg == "__DONE__":
                    run_button.configure(state="normal")
                    set_status("Processo concluido.", True)
                else:
                    output_text.configure(state="normal")
                    output_text.insert("end", msg)
                    output_text.see("end")
                    output_text.configure(state="disabled")
        except queue.Empty:
            pass
        root.after(100, poll_queue)

    ttk.Button(load_top, text="Escolher CSV", command=on_browse).grid(row=0, column=2, padx=4, pady=4)
    run_button = ttk.Button(load_top, text="Atualizar BD", command=on_run)
    run_button.grid(row=0, column=3, padx=4, pady=4)

    output_frame = ttk.Frame(tab_load)
    output_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
    output_text = tk.Text(output_frame, height=16, wrap="word")
    output_text.pack(side="left", fill="both", expand=True)
    output_text.configure(state="disabled")
    scroll = ttk.Scrollbar(output_frame, orient="vertical", command=output_text.yview)
    scroll.pack(side="right", fill="y")
    output_text.configure(yscrollcommand=scroll.set)

    # --- Tab Gerar SQL ---
    gen_top = ttk.Frame(tab_gen)
    gen_top.pack(fill="x", padx=10, pady=8)
    gen_top.grid_columnconfigure(1, weight=1)

    ttk.Label(gen_top, text="Template SQL:").grid(row=0, column=0, sticky="w", padx=4, pady=4)
    ttk.Entry(gen_top, textvariable=var_template).grid(row=0, column=1, sticky="we", padx=4, pady=4)
    ttk.Label(gen_top, text="CSV:").grid(row=1, column=0, sticky="w", padx=4, pady=4)
    ttk.Entry(gen_top, textvariable=var_gen_csv).grid(row=1, column=1, sticky="we", padx=4, pady=4)
    ttk.Label(gen_top, text="Output SQL:").grid(row=2, column=0, sticky="w", padx=4, pady=4)
    ttk.Entry(gen_top, textvariable=var_output).grid(row=2, column=1, sticky="we", padx=4, pady=4)

    def browse_template() -> None:
        path = filedialog.askopenfilename(
            title="Seleciona o template SQL",
            filetypes=[("SQL files", "*.sql"), ("All files", "*.*")]
        )
        if path:
            var_template.set(path)

    def browse_gen_csv() -> None:
        path = filedialog.askopenfilename(
            title="Seleciona o CSV",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if path:
            var_gen_csv.set(path)

    def browse_output() -> None:
        path = filedialog.asksaveasfilename(
            title="Guardar SQL",
            defaultextension=".sql",
            filetypes=[("SQL files", "*.sql"), ("All files", "*.*")]
        )
        if path:
            var_output.set(path)

    ttk.Button(gen_top, text="Escolher", command=browse_template).grid(row=0, column=2, padx=4, pady=4)
    ttk.Button(gen_top, text="Escolher", command=browse_gen_csv).grid(row=1, column=2, padx=4, pady=4)
    ttk.Button(gen_top, text="Guardar Como", command=browse_output).grid(row=2, column=2, padx=4, pady=4)

    gen_log = tk.Text(tab_gen, height=16, wrap="word")
    gen_log.pack(fill="both", expand=True, padx=10, pady=(0, 10))
    gen_log.configure(state="disabled")

    def log_gen(msg: str) -> None:
        gen_log.configure(state="normal")
        gen_log.insert("end", msg + "\n")
        gen_log.see("end")
        gen_log.configure(state="disabled")

    gen_button = ttk.Button(gen_top, text="Gerar SQL")
    gen_button.grid(row=3, column=2, padx=4, pady=8, sticky="e")

    def run_generate_sql() -> None:
        template_path = var_template.get().strip()
        csv_path = var_gen_csv.get().strip()
        output_path = var_output.get().strip()
        if not os.path.isfile(template_path):
            messagebox.showerror("Template", "Template SQL nao existe.")
            return
        if not os.path.isfile(csv_path):
            messagebox.showerror("CSV", "Ficheiro CSV nao existe.")
            return
        if not output_path:
            messagebox.showwarning("Output", "Define o ficheiro de output.")
            return

        gen_button.configure(state="disabled")
        log_gen(f"[INFO] A gerar SQL a partir de {csv_path}")

        def worker() -> None:
            try:
                class_map, asteroids, orbits = gen_sql.build_data_from_csv(csv_path)
                q_gen.put(("log", f"Classes: {len(class_map)} | Asteroides: {len(asteroids)} | Orbits: {len(orbits)}"))
                class_lines, asteroid_lines, orbit_lines = gen_sql.build_insert_blocks(class_map, asteroids, orbits)
                gen_sql.write_sql(template_path, output_path, class_lines, asteroid_lines, orbit_lines)
                q_gen.put(("done", output_path))
            except Exception as ex:
                q_gen.put(("error", str(ex)))

        threading.Thread(target=worker, daemon=True).start()

    gen_button.configure(command=run_generate_sql)

    # --- Tab Observacoes ---
    obs_status_var = tk.StringVar(value="Pronto.")
    ttk.Label(tab_obs, textvariable=obs_status_var, style="Muted.TLabel").pack(anchor="w", padx=10, pady=(8, 0))

    obs_notebook = ttk.Notebook(tab_obs)
    obs_notebook.pack(fill="both", expand=True, padx=8, pady=8)

    tab_center = ttk.Frame(obs_notebook)
    tab_equipment = ttk.Frame(obs_notebook)
    tab_software = ttk.Frame(obs_notebook)
    tab_astronomer = ttk.Frame(obs_notebook)
    tab_observation = ttk.Frame(obs_notebook)
    obs_notebook.add(tab_center, text="Centros")
    obs_notebook.add(tab_equipment, text="Equipamentos")
    obs_notebook.add(tab_software, text="Software")
    obs_notebook.add(tab_astronomer, text="Astronomos")
    obs_notebook.add(tab_observation, text="Observacoes")

    def parse_combo_id(value: str) -> Optional[int]:
        if not value:
            return None
        try:
            return int(value.split(" - ", 1)[0])
        except ValueError:
            return None

    def parse_datetime_text(value: str) -> Optional[datetime]:
        v = value.strip()
        if not v:
            return None
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(v, fmt)
            except ValueError:
                pass
        return None

    # ---- Centros ----
    var_center_name = tk.StringVar(value="")
    var_center_location = tk.StringVar(value="")

    center_form = ttk.LabelFrame(tab_center, text="Novo Centro")
    center_form.pack(fill="x", padx=10, pady=10)
    center_form.grid_columnconfigure(1, weight=1)
    ttk.Label(center_form, text="Nome:").grid(row=0, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(center_form, textvariable=var_center_name).grid(row=0, column=1, sticky="we", padx=6, pady=4)
    ttk.Label(center_form, text="Localizacao:").grid(row=1, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(center_form, textvariable=var_center_location).grid(row=1, column=1, sticky="we", padx=6, pady=4)

    center_btns = ttk.Frame(center_form)
    center_btns.grid(row=0, column=2, rowspan=2, padx=6, pady=4, sticky="n")

    center_tree = ttk.Treeview(tab_center, columns=("id_center", "name", "location"), show="headings", height=10)
    center_tree.heading("id_center", text="ID")
    center_tree.heading("name", text="Nome")
    center_tree.heading("location", text="Localizacao")
    center_tree.column("id_center", width=80, anchor="w")
    center_tree.column("name", width=240, anchor="w")
    center_tree.column("location", width=240, anchor="w")
    center_tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    center_scroll = ttk.Scrollbar(tab_center, orient="vertical", command=center_tree.yview)
    center_tree.configure(yscrollcommand=center_scroll.set)
    center_scroll.place(in_=center_tree, relx=1.0, rely=0, relheight=1.0, anchor="ne")

    # ---- Equipamentos ----
    var_equipment_tipo = tk.StringVar(value="")
    var_equipment_modelo = tk.StringVar(value="")
    var_equipment_center = tk.StringVar(value="")

    equipment_form = ttk.LabelFrame(tab_equipment, text="Novo Equipamento")
    equipment_form.pack(fill="x", padx=10, pady=10)
    equipment_form.grid_columnconfigure(1, weight=1)
    ttk.Label(equipment_form, text="Tipo:").grid(row=0, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(equipment_form, textvariable=var_equipment_tipo).grid(row=0, column=1, sticky="we", padx=6, pady=4)
    ttk.Label(equipment_form, text="Modelo:").grid(row=1, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(equipment_form, textvariable=var_equipment_modelo).grid(row=1, column=1, sticky="we", padx=6, pady=4)
    ttk.Label(equipment_form, text="Centro:").grid(row=2, column=0, sticky="w", padx=6, pady=4)
    equipment_center_combo = ttk.Combobox(equipment_form, textvariable=var_equipment_center, state="readonly", width=28)
    equipment_center_combo.grid(row=2, column=1, sticky="w", padx=6, pady=4)

    equipment_btns = ttk.Frame(equipment_form)
    equipment_btns.grid(row=0, column=2, rowspan=3, padx=6, pady=4, sticky="n")

    equipment_tree = ttk.Treeview(
        tab_equipment,
        columns=("id_equipment", "tipo", "modelo", "center"),
        show="headings",
        height=10,
    )
    equipment_tree.heading("id_equipment", text="ID")
    equipment_tree.heading("tipo", text="Tipo")
    equipment_tree.heading("modelo", text="Modelo")
    equipment_tree.heading("center", text="Centro")
    equipment_tree.column("id_equipment", width=80, anchor="w")
    equipment_tree.column("tipo", width=160, anchor="w")
    equipment_tree.column("modelo", width=200, anchor="w")
    equipment_tree.column("center", width=220, anchor="w")
    equipment_tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    equipment_scroll = ttk.Scrollbar(tab_equipment, orient="vertical", command=equipment_tree.yview)
    equipment_tree.configure(yscrollcommand=equipment_scroll.set)
    equipment_scroll.place(in_=equipment_tree, relx=1.0, rely=0, relheight=1.0, anchor="ne")

    # ---- Software ----
    var_software_version = tk.StringVar(value="")

    software_form = ttk.LabelFrame(tab_software, text="Novo Software")
    software_form.pack(fill="x", padx=10, pady=10)
    software_form.grid_columnconfigure(1, weight=1)
    ttk.Label(software_form, text="Versao:").grid(row=0, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(software_form, textvariable=var_software_version).grid(row=0, column=1, sticky="we", padx=6, pady=4)

    software_btns = ttk.Frame(software_form)
    software_btns.grid(row=0, column=2, padx=6, pady=4, sticky="n")

    software_tree = ttk.Treeview(tab_software, columns=("id_software", "version"), show="headings", height=10)
    software_tree.heading("id_software", text="ID")
    software_tree.heading("version", text="Versao")
    software_tree.column("id_software", width=80, anchor="w")
    software_tree.column("version", width=260, anchor="w")
    software_tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    software_scroll = ttk.Scrollbar(tab_software, orient="vertical", command=software_tree.yview)
    software_tree.configure(yscrollcommand=software_scroll.set)
    software_scroll.place(in_=software_tree, relx=1.0, rely=0, relheight=1.0, anchor="ne")

    # ---- Astronomos ----
    var_astronomer_name = tk.StringVar(value="")
    var_astronomer_aff = tk.StringVar(value="")

    astronomer_form = ttk.LabelFrame(tab_astronomer, text="Novo Astronomo")
    astronomer_form.pack(fill="x", padx=10, pady=10)
    astronomer_form.grid_columnconfigure(1, weight=1)
    ttk.Label(astronomer_form, text="Nome:").grid(row=0, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(astronomer_form, textvariable=var_astronomer_name).grid(row=0, column=1, sticky="we", padx=6, pady=4)
    ttk.Label(astronomer_form, text="Afiliacao:").grid(row=1, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(astronomer_form, textvariable=var_astronomer_aff).grid(row=1, column=1, sticky="we", padx=6, pady=4)

    astronomer_btns = ttk.Frame(astronomer_form)
    astronomer_btns.grid(row=0, column=2, rowspan=2, padx=6, pady=4, sticky="n")

    astronomer_tree = ttk.Treeview(
        tab_astronomer,
        columns=("id_astronomer", "name", "affiliation"),
        show="headings",
        height=10,
    )
    astronomer_tree.heading("id_astronomer", text="ID")
    astronomer_tree.heading("name", text="Nome")
    astronomer_tree.heading("affiliation", text="Afiliacao")
    astronomer_tree.column("id_astronomer", width=80, anchor="w")
    astronomer_tree.column("name", width=200, anchor="w")
    astronomer_tree.column("affiliation", width=240, anchor="w")
    astronomer_tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    astronomer_scroll = ttk.Scrollbar(tab_astronomer, orient="vertical", command=astronomer_tree.yview)
    astronomer_tree.configure(yscrollcommand=astronomer_scroll.set)
    astronomer_scroll.place(in_=astronomer_tree, relx=1.0, rely=0, relheight=1.0, anchor="ne")

    # ---- Observacoes ----
    var_obs_date = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M"))
    var_obs_duration = tk.StringVar(value="")
    obs_mode_values = ["Optica", "Radar", "Infravermelho", "Fotometria", "Espectroscopia", "Tracking"]
    var_obs_mode = tk.StringVar(value=obs_mode_values[0])
    var_obs_asteroid = tk.StringVar(value="")
    var_obs_astronomer = tk.StringVar(value="")
    var_obs_software = tk.StringVar(value="")
    var_obs_equipment = tk.StringVar(value="")

    observation_form = ttk.LabelFrame(tab_observation, text="Nova Observacao")
    observation_form.pack(fill="x", padx=10, pady=10)
    observation_form.grid_columnconfigure(1, weight=1)

    ttk.Label(observation_form, text="Data (YYYY-MM-DD HH:MM):").grid(row=0, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(observation_form, textvariable=var_obs_date).grid(row=0, column=1, sticky="we", padx=6, pady=4)
    ttk.Label(observation_form, text="Duracao (min):").grid(row=1, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(observation_form, textvariable=var_obs_duration).grid(row=1, column=1, sticky="we", padx=6, pady=4)
    ttk.Label(observation_form, text="Modo:").grid(row=2, column=0, sticky="w", padx=6, pady=4)
    obs_mode_combo = ttk.Combobox(
        observation_form,
        textvariable=var_obs_mode,
        state="readonly",
        values=obs_mode_values,
        width=28,
    )
    obs_mode_combo.grid(row=2, column=1, sticky="w", padx=6, pady=4)
    ttk.Label(observation_form, text="ID Asteroide:").grid(row=3, column=0, sticky="w", padx=6, pady=4)
    ttk.Entry(observation_form, textvariable=var_obs_asteroid).grid(row=3, column=1, sticky="we", padx=6, pady=4)
    ttk.Label(observation_form, text="Astronomo:").grid(row=4, column=0, sticky="w", padx=6, pady=4)
    obs_astronomer_combo = ttk.Combobox(observation_form, textvariable=var_obs_astronomer, state="readonly", width=28)
    obs_astronomer_combo.grid(row=4, column=1, sticky="w", padx=6, pady=4)
    ttk.Label(observation_form, text="Software:").grid(row=5, column=0, sticky="w", padx=6, pady=4)
    obs_software_combo = ttk.Combobox(observation_form, textvariable=var_obs_software, state="readonly", width=28)
    obs_software_combo.grid(row=5, column=1, sticky="w", padx=6, pady=4)
    ttk.Label(observation_form, text="Equipamento:").grid(row=6, column=0, sticky="w", padx=6, pady=4)
    obs_equipment_combo = ttk.Combobox(observation_form, textvariable=var_obs_equipment, state="readonly", width=28)
    obs_equipment_combo.grid(row=6, column=1, sticky="w", padx=6, pady=4)

    observation_btns = ttk.Frame(observation_form)
    observation_btns.grid(row=0, column=2, rowspan=7, padx=6, pady=4, sticky="n")

    observation_tree = ttk.Treeview(
        tab_observation,
        columns=("id_observation", "date", "duration", "mode", "asteroid", "astronomer", "software", "equipment", "center"),
        show="headings",
        height=10,
    )
    observation_tree.heading("id_observation", text="ID")
    observation_tree.heading("date", text="Data")
    observation_tree.heading("duration", text="Duracao")
    observation_tree.heading("mode", text="Modo")
    observation_tree.heading("asteroid", text="Asteroide")
    observation_tree.heading("astronomer", text="Astronomo")
    observation_tree.heading("software", text="Software")
    observation_tree.heading("equipment", text="Equipamento")
    observation_tree.heading("center", text="Centro")
    observation_tree.column("id_observation", width=70, anchor="w")
    observation_tree.column("date", width=150, anchor="w")
    observation_tree.column("duration", width=80, anchor="w")
    observation_tree.column("mode", width=120, anchor="w")
    observation_tree.column("asteroid", width=200, anchor="w")
    observation_tree.column("astronomer", width=160, anchor="w")
    observation_tree.column("software", width=120, anchor="w")
    observation_tree.column("equipment", width=180, anchor="w")
    observation_tree.column("center", width=160, anchor="w")
    observation_tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    observation_scroll = ttk.Scrollbar(tab_observation, orient="vertical", command=observation_tree.yview)
    observation_tree.configure(yscrollcommand=observation_scroll.set)
    observation_scroll.place(in_=observation_tree, relx=1.0, rely=0, relheight=1.0, anchor="ne")
    observation_scroll_x = ttk.Scrollbar(tab_observation, orient="horizontal", command=observation_tree.xview)
    observation_tree.configure(xscrollcommand=observation_scroll_x.set)
    observation_scroll_x.pack(fill="x", padx=10, pady=(0, 10))

    def clear_obs_tree(tree: ttk.Treeview) -> None:
        for item in tree.get_children():
            tree.delete(item)

    def refresh_centers() -> None:
        cfg = cfg_from_fields()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                cur.execute("SELECT id_center, name, location FROM Center_observation ORDER BY id_center;")
                rows = cur.fetchall()
                conn.close()
                q_obs.put(("centers", rows))
            except Exception as ex:
                q_obs.put(("error", f"Centros: {ex}"))

        threading.Thread(target=worker, daemon=True).start()

    def refresh_equipments() -> None:
        cfg = cfg_from_fields()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                cur.execute("""
                    SELECT e.id_equipment, e.tipo, e.modelo, COALESCE(c.name, '')
                    FROM Equipment e
                    LEFT JOIN Center_observation c ON c.id_center = e.id_center
                    ORDER BY e.id_equipment;
                """)
                rows = cur.fetchall()
                conn.close()
                q_obs.put(("equipments", rows))
            except Exception as ex:
                q_obs.put(("error", f"Equipamentos: {ex}"))

        threading.Thread(target=worker, daemon=True).start()

    def refresh_software() -> None:
        cfg = cfg_from_fields()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                cur.execute("SELECT id_software, version FROM Software ORDER BY id_software;")
                rows = cur.fetchall()
                conn.close()
                q_obs.put(("software", rows))
            except Exception as ex:
                q_obs.put(("error", f"Software: {ex}"))

        threading.Thread(target=worker, daemon=True).start()

    def refresh_astronomers() -> None:
        cfg = cfg_from_fields()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                cur.execute("SELECT id_astronomer, name, affiliation FROM Astronomer ORDER BY id_astronomer;")
                rows = cur.fetchall()
                conn.close()
                q_obs.put(("astronomers", rows))
            except Exception as ex:
                q_obs.put(("error", f"Astronomos: {ex}"))

        threading.Thread(target=worker, daemon=True).start()

    def refresh_observations() -> None:
        cfg = cfg_from_fields()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                cur.execute("""
                    SELECT
                        o.id_observation,
                        o.date,
                        o.duration,
                        o.mode,
                        o.id_internal,
                        COALESCE(a.full_name, ''),
                        COALESCE(astr.name, ''),
                        COALESCE(s.version, ''),
                        COALESCE(e.tipo + ' ' + e.modelo, ''),
                        COALESCE(c.name, '')
                    FROM Observation o
                    LEFT JOIN Asteroid a ON a.id_internal = o.id_internal
                    LEFT JOIN Astronomer astr ON astr.id_astronomer = o.id_astronomer
                    LEFT JOIN Software s ON s.id_software = o.id_software
                    LEFT JOIN Equipment e ON e.id_equipment = o.id_equipment
                    LEFT JOIN Center_observation c ON c.id_center = e.id_center
                    ORDER BY o.date DESC;
                """)
                rows = cur.fetchall()
                conn.close()
                q_obs.put(("observations", rows))
            except Exception as ex:
                q_obs.put(("error", f"Observacoes: {ex}"))

        threading.Thread(target=worker, daemon=True).start()

    def refresh_reference_lists() -> None:
        cfg = cfg_from_fields()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                cur.execute("SELECT id_center, name FROM Center_observation ORDER BY id_center;")
                centers = cur.fetchall()
                cur.execute("SELECT id_equipment, tipo, modelo FROM Equipment ORDER BY id_equipment;")
                equipments = cur.fetchall()
                cur.execute("SELECT id_software, version FROM Software ORDER BY id_software;")
                softwares = cur.fetchall()
                cur.execute("SELECT id_astronomer, name FROM Astronomer ORDER BY id_astronomer;")
                astronomers = cur.fetchall()
                conn.close()
                q_obs.put(("refs", (centers, equipments, softwares, astronomers)))
            except Exception as ex:
                q_obs.put(("error", f"Listas: {ex}"))

        threading.Thread(target=worker, daemon=True).start()

    def add_center() -> None:
        name = var_center_name.get().strip()
        location = var_center_location.get().strip()
        if not name or not location:
            messagebox.showwarning("Centro", "Preenche o nome e a localizacao.")
            return
        cfg = cfg_from_fields()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                cur.execute("SELECT ISNULL(MAX(id_center), 0) + 1 FROM Center_observation;")
                row = cur.fetchone()
                new_id = int(row[0] if row else 1)
                cur.execute(
                    "INSERT INTO Center_observation (id_center, name, location) VALUES (?, ?, ?);",
                    new_id,
                    name,
                    location,
                )
                conn.commit()
                conn.close()
                q_obs.put(("log", f"Centro criado (ID {new_id})."))
                q_obs.put(("refresh", "centers"))
                q_obs.put(("refresh_refs", None))
            except Exception as ex:
                q_obs.put(("error", f"Centro: {ex}"))

        threading.Thread(target=worker, daemon=True).start()

    def add_equipment() -> None:
        tipo = var_equipment_tipo.get().strip()
        modelo = var_equipment_modelo.get().strip()
        center_id = parse_combo_id(var_equipment_center.get())
        if not tipo or not modelo or center_id is None:
            messagebox.showwarning("Equipamento", "Preenche tipo, modelo e centro.")
            return
        cfg = cfg_from_fields()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                cur.execute("SELECT ISNULL(MAX(id_equipment), 0) + 1 FROM Equipment;")
                row = cur.fetchone()
                new_id = int(row[0] if row else 1)
                cur.execute(
                    "INSERT INTO Equipment (id_equipment, tipo, modelo, id_center) VALUES (?, ?, ?, ?);",
                    new_id,
                    tipo,
                    modelo,
                    center_id,
                )
                conn.commit()
                conn.close()
                q_obs.put(("log", f"Equipamento criado (ID {new_id})."))
                q_obs.put(("refresh", "equipments"))
                q_obs.put(("refresh_refs", None))
            except Exception as ex:
                q_obs.put(("error", f"Equipamento: {ex}"))

        threading.Thread(target=worker, daemon=True).start()

    def add_software() -> None:
        version = var_software_version.get().strip()
        if not version:
            messagebox.showwarning("Software", "Preenche a versao.")
            return
        cfg = cfg_from_fields()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                cur.execute("SELECT ISNULL(MAX(id_software), 0) + 1 FROM Software;")
                row = cur.fetchone()
                new_id = int(row[0] if row else 1)
                cur.execute(
                    "INSERT INTO Software (id_software, version) VALUES (?, ?);",
                    new_id,
                    version,
                )
                conn.commit()
                conn.close()
                q_obs.put(("log", f"Software criado (ID {new_id})."))
                q_obs.put(("refresh", "software"))
                q_obs.put(("refresh_refs", None))
            except Exception as ex:
                q_obs.put(("error", f"Software: {ex}"))

        threading.Thread(target=worker, daemon=True).start()

    def add_astronomer() -> None:
        name = var_astronomer_name.get().strip()
        affiliation = var_astronomer_aff.get().strip()
        if not name or not affiliation:
            messagebox.showwarning("Astronomo", "Preenche nome e afiliacao.")
            return
        cfg = cfg_from_fields()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                cur.execute("SELECT ISNULL(MAX(id_astronomer), 0) + 1 FROM Astronomer;")
                row = cur.fetchone()
                new_id = int(row[0] if row else 1)
                cur.execute(
                    "INSERT INTO Astronomer (id_astronomer, name, affiliation) VALUES (?, ?, ?);",
                    new_id,
                    name,
                    affiliation,
                )
                conn.commit()
                conn.close()
                q_obs.put(("log", f"Astronomo criado (ID {new_id})."))
                q_obs.put(("refresh", "astronomers"))
                q_obs.put(("refresh_refs", None))
            except Exception as ex:
                q_obs.put(("error", f"Astronomo: {ex}"))

        threading.Thread(target=worker, daemon=True).start()

    def add_observation() -> None:
        date_val = parse_datetime_text(var_obs_date.get())
        duration_val = parse_int(var_obs_duration.get())
        mode_val = var_obs_mode.get().strip()
        asteroid_id = parse_int(var_obs_asteroid.get())
        astronomer_id = parse_combo_id(var_obs_astronomer.get())
        software_id = parse_combo_id(var_obs_software.get())
        equipment_id = parse_combo_id(var_obs_equipment.get())
        if not date_val or duration_val is None or not mode_val or asteroid_id is None:
            messagebox.showwarning("Observacao", "Preenche data, duracao, modo e ID do asteroide.")
            return
        if astronomer_id is None or software_id is None or equipment_id is None:
            messagebox.showwarning("Observacao", "Seleciona astronomo, software e equipamento.")
            return
        cfg = cfg_from_fields()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                cur.execute("SELECT ISNULL(MAX(id_observation), 0) + 1 FROM Observation;")
                row = cur.fetchone()
                new_id = int(row[0] if row else 1)
                cur.execute(
                    """
                    INSERT INTO Observation (
                        id_observation, date, duration, mode, id_internal, id_astronomer, id_software, id_equipment
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    new_id,
                    date_val,
                    duration_val,
                    mode_val,
                    asteroid_id,
                    astronomer_id,
                    software_id,
                    equipment_id,
                )
                conn.commit()
                conn.close()
                q_obs.put(("log", f"Observacao criada (ID {new_id})."))
                q_obs.put(("refresh", "observations"))
            except Exception as ex:
                q_obs.put(("error", f"Observacao: {ex}"))

        threading.Thread(target=worker, daemon=True).start()

    ttk.Button(center_btns, text="Adicionar", command=add_center).grid(row=0, column=0, padx=4, pady=4, sticky="e")
    ttk.Button(center_btns, text="Atualizar Lista", command=refresh_centers).grid(row=1, column=0, padx=4, pady=4, sticky="e")

    ttk.Button(equipment_btns, text="Adicionar", command=add_equipment).grid(row=0, column=0, padx=4, pady=4, sticky="e")
    ttk.Button(equipment_btns, text="Atualizar Lista", command=refresh_equipments).grid(row=1, column=0, padx=4, pady=4, sticky="e")
    ttk.Button(equipment_btns, text="Carregar Centros", command=refresh_reference_lists).grid(row=2, column=0, padx=4, pady=4, sticky="e")

    ttk.Button(software_btns, text="Adicionar", command=add_software).grid(row=0, column=0, padx=4, pady=4, sticky="e")
    ttk.Button(software_btns, text="Atualizar Lista", command=refresh_software).grid(row=1, column=0, padx=4, pady=4, sticky="e")

    ttk.Button(astronomer_btns, text="Adicionar", command=add_astronomer).grid(row=0, column=0, padx=4, pady=4, sticky="e")
    ttk.Button(astronomer_btns, text="Atualizar Lista", command=refresh_astronomers).grid(row=1, column=0, padx=4, pady=4, sticky="e")

    ttk.Button(observation_btns, text="Adicionar", command=add_observation).grid(row=0, column=0, padx=4, pady=4, sticky="e")
    ttk.Button(observation_btns, text="Atualizar Lista", command=refresh_observations).grid(row=1, column=0, padx=4, pady=4, sticky="e")
    ttk.Button(observation_btns, text="Carregar Listas", command=refresh_reference_lists).grid(row=2, column=0, padx=4, pady=4, sticky="e")

    def poll_obs_queue() -> None:
        try:
            while True:
                kind, payload = q_obs.get_nowait()
                if kind == "centers":
                    clear_obs_tree(center_tree)
                    for row in cast(list[tuple[Any, ...]], payload):
                        center_tree.insert("", "end", values=(row[0], row[1], row[2]))
                elif kind == "equipments":
                    clear_obs_tree(equipment_tree)
                    for row in cast(list[tuple[Any, ...]], payload):
                        equipment_tree.insert("", "end", values=(row[0], row[1], row[2], row[3]))
                elif kind == "software":
                    clear_obs_tree(software_tree)
                    for row in cast(list[tuple[Any, ...]], payload):
                        software_tree.insert("", "end", values=(row[0], row[1]))
                elif kind == "astronomers":
                    clear_obs_tree(astronomer_tree)
                    for row in cast(list[tuple[Any, ...]], payload):
                        astronomer_tree.insert("", "end", values=(row[0], row[1], row[2]))
                elif kind == "observations":
                    clear_obs_tree(observation_tree)
                    for row in cast(list[tuple[Any, ...]], payload):
                        date_val = row[1]
                        date_txt = date_val.strftime("%Y-%m-%d %H:%M") if hasattr(date_val, "strftime") else str(date_val)
                        asteroid_txt = row[5] if row[5] else f"ID {row[4]}"
                        equipment_txt = row[8]
                        observation_tree.insert(
                            "",
                            "end",
                            values=(
                                row[0],
                                date_txt,
                                row[2],
                                row[3],
                                asteroid_txt,
                                row[6],
                                row[7],
                                equipment_txt,
                                row[9],
                            ),
                        )
                elif kind == "refs":
                    centers, equipments, softwares, astronomers = cast(
                        tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], list[tuple[Any, ...]], list[tuple[Any, ...]]],
                        payload,
                    )
                    center_values = [f"{row[0]} - {row[1]}" for row in centers]
                    equipment_values = [f"{row[0]} - {row[1]} {row[2]}" for row in equipments]
                    software_values = [f"{row[0]} - {row[1]}" for row in softwares]
                    astronomer_values = [f"{row[0]} - {row[1]}" for row in astronomers]
                    equipment_center_combo.configure(values=center_values)
                    obs_equipment_combo.configure(values=equipment_values)
                    obs_software_combo.configure(values=software_values)
                    obs_astronomer_combo.configure(values=astronomer_values)
                    if var_equipment_center.get() not in center_values:
                        var_equipment_center.set(center_values[0] if center_values else "")
                    if var_obs_equipment.get() not in equipment_values:
                        var_obs_equipment.set(equipment_values[0] if equipment_values else "")
                    if var_obs_software.get() not in software_values:
                        var_obs_software.set(software_values[0] if software_values else "")
                    if var_obs_astronomer.get() not in astronomer_values:
                        var_obs_astronomer.set(astronomer_values[0] if astronomer_values else "")
                elif kind == "refresh":
                    if payload == "centers":
                        refresh_centers()
                    elif payload == "equipments":
                        refresh_equipments()
                    elif payload == "software":
                        refresh_software()
                    elif payload == "astronomers":
                        refresh_astronomers()
                    elif payload == "observations":
                        refresh_observations()
                elif kind == "refresh_refs":
                    refresh_reference_lists()
                elif kind == "log":
                    obs_status_var.set(str(payload))
                elif kind == "error":
                    obs_status_var.set(str(payload))
                    messagebox.showerror("Observacoes", str(payload))
        except queue.Empty:
            pass
        root.after(200, poll_obs_queue)

    # --- Tab Monitorizacao ---
    monitor_canvas = tk.Canvas(tab_monitor, highlightthickness=0)
    monitor_scroll = ttk.Scrollbar(tab_monitor, orient="vertical", command=monitor_canvas.yview)
    monitor_canvas.configure(yscrollcommand=monitor_scroll.set)
    monitor_scroll.pack(side="right", fill="y")
    monitor_canvas.pack(side="left", fill="both", expand=True)

    monitor_body = ttk.Frame(monitor_canvas)
    monitor_window = monitor_canvas.create_window((0, 0), window=monitor_body, anchor="nw")

    def _sync_monitor_scroll(_event: tk.Event) -> None:
        monitor_canvas.configure(scrollregion=monitor_canvas.bbox("all"))

    def _sync_monitor_width(event: tk.Event) -> None:
        monitor_canvas.itemconfigure(monitor_window, width=event.width)

    monitor_body.bind("<Configure>", _sync_monitor_scroll)
    monitor_canvas.bind("<Configure>", _sync_monitor_width)

    mon_top = ttk.Frame(monitor_body)
    mon_top.pack(fill="x", padx=10, pady=8)
    ttk.Label(mon_top, text="Monitorizacao e estatisticas", style="Header.TLabel").pack(side="left")

    stats_frame = ttk.LabelFrame(monitor_body, text="Resumo geral")
    stats_frame.pack(fill="x", padx=10, pady=(0, 8))

    var_ast = tk.StringVar(value="-")
    var_orbit = tk.StringVar(value="-")
    var_alert = tk.StringVar(value="-")
    var_high = tk.StringVar(value="-")
    var_red = tk.StringVar(value="-")
    var_orange = tk.StringVar(value="-")
    var_pha_over = tk.StringVar(value="-")
    var_new_neos = tk.StringVar(value="-")
    var_next_critical = tk.StringVar(value="-")
    monitor_status_var = tk.StringVar(value="")

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
    ttk.Label(stats_frame, text="Estado:").grid(row=4, column=2, sticky="w", padx=6, pady=2)
    ttk.Label(stats_frame, textvariable=monitor_status_var).grid(row=4, column=3, sticky="w", padx=6, pady=2)
    stats_frame.grid_columnconfigure(4, weight=1)

    trend_frame = ttk.LabelFrame(monitor_body, text="Tendencias")
    trend_frame.pack(fill="x", padx=10, pady=(0, 8))
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

    tables_frame = ttk.Frame(monitor_body)
    tables_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
    tables_frame.grid_columnconfigure(0, weight=1, uniform="tbl")
    tables_frame.grid_columnconfigure(1, weight=1, uniform="tbl")

    precision_box = ttk.LabelFrame(tables_frame, text="Detalhe: Precisao orbital (RMS medio por ano)")
    discovery_box = ttk.LabelFrame(tables_frame, text="Detalhe: Novas descobertas (por mes)")
    latest_box = ttk.LabelFrame(tables_frame, text="Ultimas descobertas")
    precision_box.grid(row=0, column=0, sticky="nsew", padx=(0, 6), pady=(0, 8))
    discovery_box.grid(row=0, column=1, sticky="nsew", padx=(6, 0), pady=(0, 8))
    latest_box.grid(row=1, column=0, columnspan=2, sticky="nsew")

    precision_tree = ttk.Treeview(precision_box, columns=("year", "count", "avg_rms"), show="headings", height=8)
    precision_tree.heading("year", text="Ano")
    precision_tree.heading("count", text="Orbits")
    precision_tree.heading("avg_rms", text="RMS Medio")
    precision_tree.column("year", width=80, anchor="w")
    precision_tree.column("count", width=80, anchor="w")
    precision_tree.column("avg_rms", width=120, anchor="w")
    precision_tree.pack(fill="x", padx=6, pady=6)

    discovery_tree = ttk.Treeview(discovery_box, columns=("period", "count"), show="headings", height=8)
    discovery_tree.heading("period", text="Periodo (YYYY-MM)")
    discovery_tree.heading("count", text="Novas Descobertas")
    discovery_tree.column("period", width=120, anchor="w")
    discovery_tree.column("count", width=150, anchor="w")
    discovery_tree.pack(fill="x", padx=6, pady=6)

    latest_tree = ttk.Treeview(latest_box, columns=("id_internal", "full_name", "created_at"), show="headings", height=8)
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

    def refresh_charts() -> None:
        draw_line_chart(precision_canvas, trend_cache["precision"][0], trend_cache["precision"][1])
        draw_line_chart(discovery_canvas, trend_cache["discovery"][0], trend_cache["discovery"][1])

    def refresh_monitor() -> None:
        cfg = cfg_from_fields()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM Asteroid;")
                row = cur.fetchone()
                ast_count = int(row[0] if row else 0)
                cur.execute("SELECT COUNT(*) FROM Orbit;")
                row = cur.fetchone()
                orbit_count = int(row[0] if row else 0)
                cur.execute("SELECT COUNT(*) FROM Alert;")
                row = cur.fetchone()
                alert_count = int(row[0] if row else 0)
                cur.execute("SELECT COUNT(*) FROM Alert WHERE id_priority = 1;")
                row = cur.fetchone()
                high_count = int(row[0] if row else 0)

                cur.execute("SELECT red_alerts, orange_alerts FROM vw_Alert_Stats;")
                row = cur.fetchone()
                red_count = int(row[0] if row and row[0] is not None else 0)
                orange_count = int(row[1] if row and row[1] is not None else 0)

                cur.execute("""
                    SELECT COUNT(*)
                    FROM Asteroid
                    WHERE pha_flag = 'Y' AND diameter IS NOT NULL AND diameter > 0.1;
                """)
                row = cur.fetchone()
                pha_over = int(row[0] if row and row[0] is not None else 0)

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

                q_monitor.put((
                    "stats",
                    (ast_count, orbit_count, alert_count, high_count, red_count, orange_count, pha_over, new_neos, next_critical, precision_rows, discovery_rows, latest_rows),
                ))
            except Exception as ex:
                q_monitor.put(("error", str(ex)))

        threading.Thread(target=worker, daemon=True).start()

    def update_monitor(payload: Tuple[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]) -> None:
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

    ttk.Button(mon_top, text="Atualizar Estatisticas", command=refresh_monitor).pack(side="right")

    def poll_monitor_queue() -> None:
        try:
            while True:
                kind, payload = q_monitor.get_nowait()
                if kind == "stats":
                    update_monitor(cast(Tuple[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any], payload))
                elif kind == "error":
                    monitor_status_var.set(f"Erro: {payload}")
        except queue.Empty:
            pass
        root.after(200, poll_monitor_queue)

    precision_canvas.bind("<Configure>", lambda _e: refresh_charts())
    discovery_canvas.bind("<Configure>", lambda _e: refresh_charts())
    poll_monitor_queue()

    # --- Tab Alertas ---
    filter_frame = ttk.Frame(tab_alert)
    filter_frame.pack(fill="x", padx=10, pady=8)

    ttk.Label(filter_frame, text="Prioridade:").grid(row=0, column=0, sticky="w", padx=4, pady=4)
    ttk.Label(filter_frame, text="Nivel:").grid(row=0, column=2, sticky="w", padx=4, pady=4)

    var_priority = tk.StringVar(value="Todas")
    var_level = tk.StringVar(value="Todos")
    priority_combo = ttk.Combobox(filter_frame, textvariable=var_priority, state="readonly", width=20)
    level_combo = ttk.Combobox(filter_frame, textvariable=var_level, state="readonly", width=20)
    priority_combo.grid(row=0, column=1, padx=4, pady=4, sticky="w")
    level_combo.grid(row=0, column=3, padx=4, pady=4, sticky="w")
    filter_frame.grid_columnconfigure(4, weight=1)

    var_active_only = tk.BooleanVar(value=False)
    active_check = ttk.Checkbutton(
        filter_frame,
        text="Apenas ativos (30 dias)",
        variable=var_active_only,
    )
    active_check.grid(row=1, column=0, sticky="w", padx=4, pady=4)

    notify_check = ttk.Checkbutton(
        filter_frame,
        text="Notificar novos alertas de alta prioridade",
        variable=var_notify_high,
    )
    notify_check.grid(row=1, column=1, columnspan=3, sticky="w", padx=4, pady=4)

    alert_tree = ttk.Treeview(
        tab_alert,
        columns=("id_alert", "data_generation", "priority", "level", "asteroid", "criteria"),
        show="headings",
        height=12,
    )
    for col, title, width in (
        ("id_alert", "ID", 70),
        ("data_generation", "Data", 160),
        ("priority", "Prioridade", 120),
        ("level", "Nivel", 120),
        ("asteroid", "Asteroid", 240),
        ("criteria", "Criterio", 420),
    ):
        alert_tree.heading(col, text=title)
        alert_tree.column(col, width=width, anchor="w")
    alert_tree.pack(fill="both", expand=True, padx=10, pady=(0, 6))

    alert_scroll = ttk.Scrollbar(tab_alert, orient="vertical", command=alert_tree.yview)
    alert_tree.configure(yscrollcommand=alert_scroll.set)
    alert_scroll.place(in_=alert_tree, relx=1.0, rely=0, relheight=1.0, anchor="ne")
    alert_scroll_x = ttk.Scrollbar(tab_alert, orient="horizontal", command=alert_tree.xview)
    alert_tree.configure(xscrollcommand=alert_scroll_x.set)
    alert_scroll_x.pack(fill="x", padx=10, pady=(0, 6))

    alert_log = tk.Text(tab_alert, height=6, wrap="word")
    alert_log.pack(fill="x", padx=10, pady=(0, 10))
    alert_log.configure(state="disabled")

    def log_alert(msg: str) -> None:
        alert_log.configure(state="normal")
        alert_log.insert("end", msg + "\n")
        alert_log.see("end")
        alert_log.configure(state="disabled")

    def parse_choice_id(value: str) -> Optional[int]:
        if not value or value in ("Todas", "Todos"):
            return None
        try:
            return int(value.split(" - ", 1)[0])
        except ValueError:
            return None

    def load_filter_options() -> None:
        cfg = cfg_from_fields()
        try:
            conn = connect(cfg)
            cur = conn.cursor()
            cur.execute("SELECT id_priority, name FROM Priority ORDER BY id_priority;")
            priorities = ["Todas"] + [f"{pid} - {name}" for pid, name in cur.fetchall()]
            cur.execute("SELECT id_level, description FROM Level ORDER BY id_level;")
            levels = ["Todos"] + [f"{lid} - {desc}" for lid, desc in cur.fetchall()]
            conn.close()
            priority_combo.configure(values=priorities)
            level_combo.configure(values=levels)
            if var_priority.get() not in priorities:
                var_priority.set("Todas")
            if var_level.get() not in levels:
                var_level.set("Todos")
        except Exception as ex:
            log_alert(f"[ERRO] Falha ao carregar filtros: {ex}")

    def refresh_alerts() -> None:
        cfg = cfg_from_fields()
        priority_id = parse_choice_id(var_priority.get())
        level_id = parse_choice_id(var_level.get())
        active_only = bool(var_active_only.get())

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                where = []
                params = []
                if priority_id is not None:
                    where.append("a.id_priority = ?")
                    params.append(priority_id)
                if level_id is not None:
                    where.append("a.id_level = ?")
                    params.append(level_id)
                if active_only:
                    where.append("a.data_generation >= DATEADD(DAY, -30, SYSDATETIME())")
                where_sql = " WHERE " + " AND ".join(where) if where else ""
                sql = f"""
                    SELECT
                        a.id_alert,
                        a.data_generation,
                        COALESCE(p.name, CONCAT('ID ', a.id_priority)),
                        COALESCE(l.description, CONCAT('ID ', a.id_level)),
                        COALESCE(ast.full_name, CONCAT('ID ', a.id_internal)),
                        a.criteria_trigger
                    FROM Alert a
                    LEFT JOIN Priority p ON p.id_priority = a.id_priority
                    LEFT JOIN Level l ON l.id_level = a.id_level
                    LEFT JOIN Asteroid ast ON ast.id_internal = a.id_internal
                    {where_sql}
                    ORDER BY a.data_generation DESC;
                """
                cur.execute(sql, params)
                rows = cur.fetchall()
                conn.close()
                q_alerts.put(("rows", rows))
            except Exception as ex:
                q_alerts.put(("error", [str(ex)]))

        threading.Thread(target=worker, daemon=True).start()

    def simulate_alerts() -> None:
        cfg = cfg_from_fields()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()

                cur.execute("""
                    SELECT TOP 1 id_internal, diameter
                    FROM Asteroid
                    WHERE diameter IS NOT NULL AND diameter > 10
                    ORDER BY diameter DESC;
                """)
                row = cur.fetchone()
                if not row:
                    q_alerts.put(("error", ["Nao encontrei asteroide com diametro > 10."]))
                    conn.close()
                    return
                id_internal = int(row[0])

                cur.execute("SELECT ISNULL(MAX(id_ca), 0) + 1 FROM Close_Approach;")
                row = cur.fetchone()
                id_ca = int(row[0] if row else 1)
                cur.execute(
                    "INSERT INTO Close_Approach (id_ca, approach_date, rel_velocity_kms, dist_ld, id_internal) "
                    "VALUES (?, DATEADD(DAY, 3, CAST(GETDATE() AS date)), 12.3, 0.5, ?)",
                    id_ca,
                    id_internal,
                )

                cur.execute("""
                    SELECT TOP 1 id_internal
                    FROM Asteroid
                    WHERE pha_flag = 'Y' AND diameter IS NOT NULL AND diameter > 0.1
                    ORDER BY diameter DESC;
                """)
                pha_row = cur.fetchone()
                if pha_row:
                    pha_id = int(pha_row[0])
                    cur.execute("""
                        UPDATE TOP (1) Orbit
                        SET rms = 0.9, moid_ld = 10
                        WHERE id_internal = ?;
                    """, pha_id)

                cur.execute("""
                    SELECT TOP 1 id_internal
                    FROM Asteroid
                    WHERE diameter IS NOT NULL AND diameter > 500
                      AND created_at >= DATEADD(MONTH, -1, SYSDATETIME())
                    ORDER BY diameter DESC;
                """)
                new_row = cur.fetchone()
                if new_row:
                    new_id = int(new_row[0])
                    cur.execute("""
                        UPDATE TOP (1) Orbit
                        SET moid_ld = 30
                        WHERE id_internal = ?;
                    """, new_id)

                cur.execute("""
                    SELECT TOP 1 id_internal
                    FROM Asteroid
                    WHERE diameter IS NOT NULL AND diameter > 200
                      AND albedo IS NOT NULL AND albedo > 0.3
                    ORDER BY diameter DESC;
                """)
                an_row = cur.fetchone()
                if an_row:
                    an_id = int(an_row[0])
                    cur.execute("""
                        UPDATE TOP (1) Orbit
                        SET e = 0.85, i = 75
                        WHERE id_internal = ?;
                    """, an_id)

                conn.commit()
                conn.close()
                q_alerts.put(("log", ["Simulacao concluida. Atualiza a lista de alertas."]))
            except Exception as ex:
                q_alerts.put(("error", [str(ex)]))

        threading.Thread(target=worker, daemon=True).start()

    def update_alert_tree(rows: list) -> None:
        for item in alert_tree.get_children():
            alert_tree.delete(item)
        for row in rows:
            data_gen = row[1]
            data_txt = data_gen.strftime("%Y-%m-%d %H:%M:%S") if hasattr(data_gen, "strftime") else str(data_gen)
            alert_tree.insert("", "end", values=(row[0], data_txt, row[2], row[3], row[4], row[5]))

    last_high_id: dict = {"value": None}
    notify_running: dict = {"value": False}

    def check_high_alerts() -> None:
        if notify_running["value"]:
            return
        notify_running["value"] = True

        cfg = cfg_from_fields()

        def worker() -> None:
            try:
                conn = connect(cfg)
                cur = conn.cursor()
                cur.execute("SELECT ISNULL(MAX(id_alert), 0) FROM Alert WHERE id_priority = 1;")
                row = cur.fetchone()
                max_id = int(row[0] if row else 0)
                if last_high_id["value"] is None:
                    last_high_id["value"] = max_id
                    conn.close()
                    q_alerts.put(("notify_init", []))
                    return
                cur.execute(
                    "SELECT id_alert, data_generation, criteria_trigger FROM Alert WHERE id_priority = 1 AND id_alert > ? ORDER BY id_alert;",
                    last_high_id["value"],
                )
                new_rows = cur.fetchall()
                if new_rows:
                    last_high_id["value"] = max_id
                conn.close()
                if new_rows:
                    q_alerts.put(("notify", new_rows))
            except Exception as ex:
                q_alerts.put(("error", [f"Notificacoes: {ex}"]))
            finally:
                notify_running["value"] = False

        threading.Thread(target=worker, daemon=True).start()

    def on_toggle_notify() -> None:
        if var_notify_high.get():
            last_high_id["value"] = None
            log_alert("Notificacoes de alta prioridade ativadas.")
            check_high_alerts()
        else:
            log_alert("Notificacoes de alta prioridade desativadas.")
        cfg = cfg_from_fields()
        cfg["notify_high"] = bool(var_notify_high.get())
        save_loader_config(cfg, DEFAULT_LOADER_CONFIG)

    notify_check.configure(command=on_toggle_notify)

    def schedule_notify() -> None:
        if var_notify_high.get():
            check_high_alerts()
        root.after(10000, schedule_notify)

    ttk.Button(filter_frame, text="Atualizar Lista", command=refresh_alerts).grid(row=0, column=4, padx=4, pady=4, sticky="e")
    ttk.Button(filter_frame, text="Carregar Filtros", command=load_filter_options).grid(row=1, column=4, padx=4, pady=4, sticky="e")
    ttk.Button(filter_frame, text="Simular Alertas", command=simulate_alerts).grid(row=0, column=5, rowspan=2, padx=4, pady=4, sticky="e")

    def poll_alert_queue() -> None:
        try:
            while True:
                kind, payload = q_alerts.get_nowait()
                if kind == "rows":
                    update_alert_tree(payload)
                    log_alert(f"Lista atualizada: {len(payload)} alertas.")
                elif kind == "notify":
                    for row in payload:
                        data_txt = row[1].strftime("%Y-%m-%d %H:%M:%S") if hasattr(row[1], "strftime") else str(row[1])
                        msg = f"Novo alerta HIGH #{row[0]} em {data_txt}: {row[2]}"
                        log_alert(msg)
                        messagebox.showwarning("Alerta de Alta Prioridade", msg)
                elif kind == "notify_init":
                    log_alert("Notificacoes iniciadas (baseline atual definido).")
                elif kind == "error":
                    log_alert(f"[ERRO] {payload[0] if payload else ''}")
                elif kind == "log":
                    log_alert(payload[0] if payload else "")
        except queue.Empty:
            pass
        root.after(200, poll_alert_queue)

    def on_close() -> None:
        cfg = cfg_from_fields()
        cfg["notify_high"] = bool(var_notify_high.get())
        save_loader_config(cfg, DEFAULT_LOADER_CONFIG)
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)

    def poll_gen_queue() -> None:
        try:
            while True:
                kind, payload = q_gen.get_nowait()
                if kind == "log":
                    log_gen(payload)
                elif kind == "done":
                    log_gen(f"[OK] SQL gerado: {payload}")
                    gen_button.configure(state="normal")
                elif kind == "error":
                    log_gen(f"[ERRO] {payload}")
                    gen_button.configure(state="normal")
        except queue.Empty:
            pass
        root.after(200, poll_gen_queue)

    poll_queue()
    poll_obs_queue()
    poll_alert_queue()
    schedule_notify()
    poll_gen_queue()
    root.mainloop()


if __name__ == "__main__":
    run_gui()
