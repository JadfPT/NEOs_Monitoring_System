import csv
from datetime import date, timedelta
from typing import Optional, Any, Iterable, List
import pyodbc

# ----------------- datas MPC -----------------
def mpc_packed_to_date(packed: str) -> Optional[date]:
    """
    Epoch MPC packed (ex: K25BL) -> date (YYYY-MM-DD)
    """
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
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"  # A=10 ... V=31 (na prática)
        if ch in alphabet:
            return 10 + alphabet.index(ch)
        return None

    month = decode_md(packed[3])
    day = decode_md(packed[4])
    if not month or not day:
        return None

    try:
        return date(year, month, day)
    except:
        return None

def date_to_mjd(d: date) -> float:
    mjd0 = date(1858, 11, 17)
    return float((d - mjd0).days)

def mjd_to_date(mjd: float) -> date:
    mjd0 = date(1858, 11, 17)
    return mjd0 + timedelta(days=int(mjd))

# ----------------- leitura CSV (single-column safe) -----------------
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

def parse_csv_line(line: str) -> List[str]:
    fields = next(csv.reader([line], delimiter=","))
    if len(fields) == 1 and "," in fields[0]:
        fields = next(csv.reader([fields[0]], delimiter=","))
    return fields

def read_header_line(path: str, encoding: str) -> str:
    with open(path, "r", encoding=encoding, errors="ignore", newline="") as f:
        for line in f:
            if line.strip():
                return line.rstrip("\n\r")
    return ""

def iter_rows_from_single_column_csv(path: str, encoding: str) -> Iterable[dict]:
    header_line = read_header_line(path, encoding)
    if not header_line:
        return
    header_fields = parse_csv_line(header_line)
    if header_fields:
        header_fields[0] = header_fields[0].lstrip("\ufeff")
    header_fields = [h.strip() for h in header_fields if h is not None]
    if not header_fields:
        return

    with open(path, "r", encoding=encoding, errors="ignore", newline="") as f:
        for line in f:
            if line.strip():
                break
        for line in f:
            if not line.strip():
                continue
            values = parse_csv_line(line.rstrip("\n\r"))
            if len(values) < len(header_fields) and len(values) == 1 and "," in values[0]:
                values = parse_csv_line(values[0])
            if len(values) != len(header_fields):
                yield {"__bad_row__": line.rstrip("\n\r")}
                continue
            yield dict(zip(header_fields, values))

# ----------------- parsing -----------------
def parse_float(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    x = x.strip()
    if x == "" or x.upper() == "NULL":
        return None
    try:
        return float(x)
    except:
        return None

def parse_int(x: Optional[str]) -> Optional[int]:
    if x is None:
        return None
    x = x.strip()
    if x == "" or x.upper() == "NULL":
        return None
    try:
        return int(float(x))
    except:
        return None

# ----------------- cfg stor (1 valor por linha) -----------------
def read_stor_cfg_lines(path: str) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = [ln.rstrip("\n\r") for ln in f.readlines()]
    except:
        return None

    while len(lines) < 5:
        lines.append("")

    server = (lines[0] or "").strip()
    port = (lines[1] or "").strip()
    user = (lines[2] or "").strip()
    password = (lines[3] or "").strip()
    database = (lines[4] or "").strip()

    if not server or not user or not database:
        return None

    return {"server": server, "port": port, "user": user, "password": password, "database": database}

def make_conn_str(cfg: dict) -> str:
    server = cfg["server"]
    port = (cfg.get("port") or "").strip()
    if port:
        server = f"{server},{port}"
    return (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};DATABASE={cfg['database']};UID={cfg['user']};PWD={cfg['password']};"
        "TrustServerCertificate=yes;"
    )

# ----------------- DB helpers -----------------
def ensure_class(cur, cls: str, desc: str):
    cls = (cls or "UNK").strip()[:20] or "UNK"
    desc = (desc or cls).strip()[:255]
    cur.execute("SELECT 1 FROM Class_Orbital WHERE class = ?", cls)
    if cur.fetchone() is None:
        cur.execute("INSERT INTO Class_Orbital(class, class_description) VALUES (?, ?)", cls, desc)

def get_id_internal_by_pdes(cur, pdes: str) -> Optional[int]:
    cur.execute("SELECT id_internal FROM Asteroid WHERE pdes = ?", pdes)
    r = cur.fetchone()
    return int(r[0]) if r else None

def orbit_exists(cur, orbit_id: str) -> bool:
    cur.execute("SELECT 1 FROM Orbit WHERE id_orbita = ?", orbit_id)
    return cur.fetchone() is not None

# ----------------- loader principal -----------------
def load_mpcorb(csv_path: str, cfg_path: str = "ultima_configuracao.cfg", commit_every: int = 20000, allow_link: bool = False):
    cfg = read_stor_cfg_lines(cfg_path)
    if not cfg:
        raise RuntimeError(f"Config inválida: {cfg_path}")
    if not allow_link:
        print("[WARN] Carregamento MPC desativado: falta regra valida para associar orbitas a asteroides.")
        return

    conn = pyodbc.connect(make_conn_str(cfg))
    conn.autocommit = False
    cur = conn.cursor()

    processed = 0
    inserted = 0
    skipped_exists = 0
    skipped_no_asteroid = 0
    skipped_bad_row = 0
    errors = 0

    encoding = detect_encoding(csv_path)
    for row in iter_rows_from_single_column_csv(csv_path, encoding):
        if "__bad_row__" in row:
            skipped_bad_row += 1
            continue
        processed += 1
        try:
            designation = (row.get("designation") or "").strip()
            if not designation:
                skipped_bad_row += 1
                continue
            else:

                # 1) gerar um id_orbita que não colida com os do neo.csv (ex: "JPL 36")
                #    vamos prefixar sempre com "MPC:"
                orbit_id = f"MPC:{designation}"

                if orbit_exists(cur, orbit_id):
                    skipped_exists += 1
                    continue

                # 2) ligar ao Asteroid via pdes
                #    - no teu Asteroid.pdes provavelmente está "193", "1", etc (sem zeros)
                #    - no mpcorb vem "00001"
                pdes_try = []
                pdes_try.append(designation)  # "00001"
                try:
                    pdes_try.append(str(int(designation)))  # "1"
                except:
                    pass

                id_internal = None
                for pdes in pdes_try:
                    id_internal = get_id_internal_by_pdes(cur, pdes)
                    if id_internal is not None:
                        break

                if id_internal is None:
                    skipped_no_asteroid += 1
                    continue

                # 3) epoch (packed) -> epoch_cal / mjd / jd
                epoch_packed = row.get("epoch") or ""
                epoch_cal = mpc_packed_to_date(epoch_packed)

                if epoch_cal is None:
                    # epoch/epoch_mjd são NOT NULL -> mete defaults seguros
                    epoch_mjd = 0.0
                    epoch_jd = 2400000.5
                else:
                    epoch_mjd = date_to_mjd(epoch_cal)
                    epoch_jd = epoch_mjd + 2400000.5

                # 4) elementos orbitais
                ma = parse_float(row.get("mean_anomaly")) or 0.0          # deg
                w  = parse_float(row.get("arg_perihelion")) or 0.0        # deg
                om = parse_float(row.get("long_asc_node")) or 0.0         # deg
                inc= parse_float(row.get("inclination")) or 0.0           # deg
                e  = parse_float(row.get("eccentricity")) or 0.0
                n  = parse_float(row.get("mean_motion")) or 0.0           # deg/day
                a  = parse_float(row.get("semi_major_axis")) or 0.0       # AU
                rms= parse_float(row.get("rms_residual")) or 0.0

                q  = a * (1.0 - e) if a else 0.0
                ad = a * (1.0 + e) if a else 0.0

                per = (360.0 / n) if n else 0.0          # dias
                per_y = per / 365.25 if per else 0.0

                # tp (JD) e tp_cal (DATE NOT NULL)
                if n:
                    delta_days = ma / n
                    tp_jd = epoch_jd - delta_days
                    tp_mjd = tp_jd - 2400000.5
                    tp_cal = mjd_to_date(tp_mjd)
                    tp = tp_jd
                else:
                    tp = epoch_jd
                    tp_cal = epoch_cal if epoch_cal else date.today()

                # 5) class
                orbit_type = (row.get("orbit_type") or "UNK").strip()
                cls = orbit_type[:20] if orbit_type else "UNK"
                ensure_class(cur, cls, orbit_type)

                # 6) inserir Orbit (preencher NOT NULL com defaults)
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
                orbit_id,
                float(epoch_jd), float(rms), 0.0, float(epoch_mjd), epoch_cal,
                float(tp), tp_cal, float(per), float(per_y), "J2000",
                parse_int(row.get("uncertainty")), None,
                float(e), float(a), float(q), float(inc), float(om), float(w), float(ma), float(ad), float(n), 0.0,
                None, None, None, None, None, None, None, None, None, None, None,
                id_internal, cls
                )

                inserted += 1
                if inserted % commit_every == 0:
                    conn.commit()

        except Exception as ex:
            errors += 1
            conn.rollback()
            print(f"[ERRO] {ex} | designation={row.get('designation')}")

    conn.commit()
    conn.close()

    print("\n=== RESULTADO (MPCORB) ===")
    print(f"Linhas processadas:        {processed}")
    print(f"Orbits inseridas:          {inserted}")
    print(f"Skipped (orbit existe):    {skipped_exists}")
    print(f"Skipped (sem asteroid):    {skipped_no_asteroid}")
    print(f"Skipped (linha inválida):  {skipped_bad_row}")
    print(f"Erros:                     {errors}")
    print("==========================\n")
