import csv
import os
from datetime import date, datetime, timedelta

TEMPLATE_SQL = os.path.join("Base de Dados", "NEOs_database.sql")
MERGED_CSV = os.path.join("Ficheiros .csv", "neo_mpcorb_merged_correct.csv")
OUTPUT_SQL = os.path.join("Base de Dados", "NEOs_database_correct.sql")


def detect_delimiter(line):
    if line.count(";") >= line.count(","):
        return ";"
    return ","


def normalize_header(fields):
    counts = {}
    out = []
    for c in fields:
        c = (c or "").strip().lower().lstrip("\ufeff")
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


def parse_float(x):
    if x is None:
        return None
    x = str(x).strip()
    if x == "" or x.upper() == "NULL":
        return None
    try:
        return float(x)
    except Exception:
        return None


def parse_int(x):
    if x is None:
        return None
    x = str(x).strip()
    if x == "" or x.upper() == "NULL":
        return None
    try:
        return int(float(x))
    except Exception:
        return None


def parse_date(x):
    if x is None:
        return None
    x = str(x).strip()
    if x == "" or x.upper() == "NULL":
        return None
    if len(x) >= 8 and x[:8].isdigit():
        try:
            return datetime.strptime(x[:8], "%Y%m%d").date()
        except Exception:
            return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(x, fmt).date()
        except Exception:
            pass
    return None


def mpc_packed_to_date(packed):
    packed = (packed or "").strip()
    if len(packed) != 5:
        return None
    century_map = {"I": 1800, "J": 1900, "K": 2000}
    c = packed[0]
    if c not in century_map or not packed[1:3].isdigit():
        return None
    year = century_map[c] + int(packed[1:3])

    def decode_md(ch):
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


def date_to_mjd(d):
    mjd0 = date(1858, 11, 17)
    return float((d - mjd0).days)


def mjd_to_date(mjd):
    mjd0 = date(1858, 11, 17)
    try:
        return mjd0 + timedelta(days=int(mjd))
    except Exception:
        return None


def norm_flag(x):
    v = (x or "").strip().upper()[:1] or "N"
    return v if v in ("Y", "N") else "N"

def norm_text(x):
    if x is None:
        return None
    v = str(x).strip()
    if v == "" or v.upper() == "NULL":
        return None
    return v


def sql_text(value, allow_null=True, empty_as_null=True):
    if value is None:
        return "NULL" if allow_null else "N''"
    v = str(value).strip()
    if v == "" or v.upper() == "NULL":
        if allow_null and empty_as_null:
            return "NULL"
        return "N''"
    v = v.replace("'", "''")
    return f"N'{v}'"


def sql_float(value):
    if value is None:
        return "NULL"
    return repr(float(value))


def sql_int(value):
    if value is None:
        return "NULL"
    return str(int(value))


def sql_date(value):
    if value is None:
        return "NULL"
    return f"CAST(N'{value.isoformat()}' AS Date)"


def merge_field(current, new_value):
    if current is None or current == "" or current == "NULL":
        return new_value
    return current


def merge_numeric(current, new_value):
    if current is None:
        return new_value
    return current


def ensure_prefix(value):
    if value is None or value == "":
        return ""
    return value


def split_designation_full(value):
    v = (value or "").strip()
    if v.startswith("(") and ")" in v:
        close = v.find(")")
        num = v[1:close].strip()
        rest = v[close + 1 :].strip()
        return num, rest
    return "", v


def build_data_from_csv(path):
    with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        first_line = ""
        for line in f:
            if line.strip():
                first_line = line
                break
        if not first_line:
            raise RuntimeError("CSV vazio ou sem header.")
        delim = detect_delimiter(first_line)
        header = normalize_header(first_line.strip().split(delim))
        reader = csv.DictReader(f, delimiter=delim, fieldnames=header)

        class_map = {}
        neo_map = {}
        spk_map = {}
        mpc_map = {}
        next_id = 1
        asteroids = {}
        orbits = {}

        for row in reader:
            if not isinstance(row, dict):
                continue
            neo_id = (row.get("id") or "").strip()
            spkid = parse_int(row.get("spkid") or "")
            mpc_des = (row.get("designation") or "").strip()
            mpc_full = (row.get("designation_full") or "").strip()
            mpc_key = (mpc_full or mpc_des or neo_id).strip().lower()

            if neo_id and spkid is not None:
                neo_key = neo_id.lower()
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
            else:
                if mpc_key in mpc_map:
                    id_internal = mpc_map[mpc_key]
                else:
                    id_internal = next_id
                    next_id += 1
                    mpc_map[mpc_key] = id_internal

            cls = (row.get("class") or "").strip()
            orbit_type = (row.get("orbit_type") or "").strip()
            if not cls and orbit_type:
                cls = orbit_type[:20]
            cls_desc = (row.get("class_description") or orbit_type or cls).strip()
            if cls:
                class_map.setdefault(cls, cls_desc or cls)

            full_name = norm_text(row.get("full_name"))
            if full_name:
                full_name = full_name[:100]
            pdes = norm_text(row.get("pdes"))
            if pdes:
                pdes = pdes[:50]
            if not full_name:
                full_name = (mpc_full or mpc_des or pdes or "UNKNOWN").strip()[:100]
            if not pdes:
                num, rest = split_designation_full(mpc_full)
                pdes = (rest or num or mpc_des or "").strip()[:50]
            name = norm_text(row.get("name"))
            if name:
                name = name[:100]
            prefix = norm_text(row.get("prefix"))
            if prefix:
                prefix = prefix[:10]
            prefix = ensure_prefix(prefix)
            neo_flag = norm_flag(row.get("neo"))
            pha_flag = norm_flag(row.get("pha"))
            h = parse_float(row.get("h") or "")
            if h is None:
                h = parse_float(row.get("abs_mag") or "")
            if h is None:
                h = 0.0
            diameter = parse_float(row.get("diameter") or "")
            albedo = parse_float(row.get("albedo") or "")
            diameter_sigma = parse_float(row.get("diameter_sigma") or "")

            ast = asteroids.get(id_internal)
            if ast is None:
                asteroids[id_internal] = {
                    "id_internal": id_internal,
                    "spkid": spkid,
                    "full_name": full_name,
                    "pdes": pdes,
                    "name": name,
                    "prefix": prefix,
                    "neo_flag": neo_flag,
                    "pha_flag": pha_flag,
                    "diameter": diameter,
                    "absolute_magnitude": h,
                    "albedo": albedo,
                    "diameter_sigma": diameter_sigma,
                    "neo_id": neo_id,
                }
            else:
                ast["spkid"] = merge_numeric(ast["spkid"], spkid)
                ast["full_name"] = merge_field(ast["full_name"], full_name)
                ast["pdes"] = merge_field(ast["pdes"], pdes)
                ast["name"] = merge_field(ast["name"], name)
                ast["prefix"] = merge_field(ast["prefix"], prefix)
                ast["neo_flag"] = merge_field(ast["neo_flag"], neo_flag)
                ast["pha_flag"] = merge_field(ast["pha_flag"], pha_flag)
                ast["diameter"] = merge_numeric(ast["diameter"], diameter)
                ast["absolute_magnitude"] = merge_numeric(ast["absolute_magnitude"], h)
                ast["albedo"] = merge_numeric(ast["albedo"], albedo)
                ast["diameter_sigma"] = merge_numeric(ast["diameter_sigma"], diameter_sigma)
                ast["neo_id"] = merge_field(ast["neo_id"], neo_id)

            orbit_id = (row.get("orbit_id") or "").strip()
            if not orbit_id:
                if mpc_des:
                    orbit_id = f"MPC:{mpc_des}"
                elif mpc_full:
                    orbit_id = f"MPC:{mpc_full}"
            if not orbit_id:
                continue

            epoch = parse_float(row.get("epoch") or "")
            epoch_mjd = parse_float(row.get("epoch_mjd") or "")
            epoch_cal = parse_date(row.get("epoch_cal") or "")
            epoch_mpc = (row.get("epoch_mpc") or "").strip()
            if epoch is None and epoch_mjd is not None:
                epoch = epoch_mjd + 2400000.5
            if epoch is None and epoch_mpc and epoch_cal is None:
                epoch_cal = mpc_packed_to_date(epoch_mpc)
                if epoch_cal is not None:
                    epoch_mjd = date_to_mjd(epoch_cal)
                    epoch = epoch_mjd + 2400000.5

            equinox = (row.get("equinox") or "J2000").strip() or "J2000"
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

            if tp is None and epoch is not None and n and ma is not None:
                tp_jd = epoch - (ma / n)
                tp = tp_jd
                tp_mjd = tp_jd - 2400000.5
                tp_cal = mjd_to_date(tp_mjd)

            if tp_cal is None:
                tp_cal = epoch_cal if epoch_cal is not None else date.today()

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

            orb = orbits.get(orbit_id)
            if orb is None:
                orbits[orbit_id] = {
                    "id_orbita": orbit_id,
                    "epoch": epoch if epoch is not None else (epoch_mjd if epoch_mjd is not None else 0.0),
                    "rms": rms or 0.0,
                    "moid_ld": moid_ld or 0.0,
                    "epoch_mjd": epoch_mjd,
                    "epoch_cal": epoch_cal,
                    "tp": tp or 0.0,
                    "tp_cal": tp_cal,
                    "per": per or 0.0,
                    "per_y": per_y or 0.0,
                    "equinox": equinox,
                    "orbit_uncertainty": orbit_uncertainty,
                    "condition_code": None,
                    "e": e or 0.0,
                    "a": a or 0.0,
                    "q": q or 0.0,
                    "i": inc or 0.0,
                    "om": om or 0.0,
                    "w": w or 0.0,
                    "ma": ma or 0.0,
                    "ad": ad or 0.0,
                    "n": n or 0.0,
                    "moid": moid or 0.0,
                    "sigma_e": sigma_e,
                    "sigma_a": sigma_a,
                    "sigma_q": sigma_q,
                    "sigma_i": sigma_i,
                    "sigma_n": sigma_n,
                    "sigma_ma": sigma_ma,
                    "sigma_om": sigma_om,
                    "sigma_w": sigma_w,
                    "sigma_ad": sigma_ad,
                    "sigma_tp": sigma_tp,
                    "sigma_per": sigma_per,
                    "id_internal": id_internal,
                    "class": cls or "NEA",
                }
            else:
                if int(orb["id_internal"]) != int(id_internal):
                    continue
                orb["epoch"] = merge_numeric(orb["epoch"], epoch)
                orb["rms"] = merge_numeric(orb["rms"], rms)
                orb["moid_ld"] = merge_numeric(orb["moid_ld"], moid_ld)
                orb["epoch_mjd"] = merge_numeric(orb["epoch_mjd"], epoch_mjd)
                orb["epoch_cal"] = merge_field(orb["epoch_cal"], epoch_cal)
                orb["tp"] = merge_numeric(orb["tp"], tp)
                orb["tp_cal"] = merge_field(orb["tp_cal"], tp_cal)
                orb["per"] = merge_numeric(orb["per"], per)
                orb["per_y"] = merge_numeric(orb["per_y"], per_y)
                orb["equinox"] = merge_field(orb["equinox"], equinox)
                orb["orbit_uncertainty"] = merge_numeric(orb["orbit_uncertainty"], orbit_uncertainty)
                orb["e"] = merge_numeric(orb["e"], e)
                orb["a"] = merge_numeric(orb["a"], a)
                orb["q"] = merge_numeric(orb["q"], q)
                orb["i"] = merge_numeric(orb["i"], inc)
                orb["om"] = merge_numeric(orb["om"], om)
                orb["w"] = merge_numeric(orb["w"], w)
                orb["ma"] = merge_numeric(orb["ma"], ma)
                orb["ad"] = merge_numeric(orb["ad"], ad)
                orb["n"] = merge_numeric(orb["n"], n)
                orb["moid"] = merge_numeric(orb["moid"], moid)
                orb["sigma_e"] = merge_numeric(orb["sigma_e"], sigma_e)
                orb["sigma_a"] = merge_numeric(orb["sigma_a"], sigma_a)
                orb["sigma_q"] = merge_numeric(orb["sigma_q"], sigma_q)
                orb["sigma_i"] = merge_numeric(orb["sigma_i"], sigma_i)
                orb["sigma_n"] = merge_numeric(orb["sigma_n"], sigma_n)
                orb["sigma_ma"] = merge_numeric(orb["sigma_ma"], sigma_ma)
                orb["sigma_om"] = merge_numeric(orb["sigma_om"], sigma_om)
                orb["sigma_w"] = merge_numeric(orb["sigma_w"], sigma_w)
                orb["sigma_ad"] = merge_numeric(orb["sigma_ad"], sigma_ad)
                orb["sigma_tp"] = merge_numeric(orb["sigma_tp"], sigma_tp)
                orb["sigma_per"] = merge_numeric(orb["sigma_per"], sigma_per)
                orb["class"] = merge_field(orb["class"], cls)

        return class_map, asteroids, orbits


def build_insert_blocks(class_map, asteroids, orbits):
    class_lines = []
    for cls in sorted(class_map.keys()):
        desc = class_map[cls]
        class_lines.append(
            "INSERT [dbo].[Class_Orbital] ([class_description], [class]) VALUES ("
            f"{sql_text(desc)}, {sql_text(cls, allow_null=False, empty_as_null=False)});"
        )

    asteroid_lines = []
    for id_internal in sorted(asteroids.keys()):
        a = asteroids[id_internal]
        asteroid_lines.append(
            "INSERT [dbo].[Asteroid] "
            "([id_internal], [spkid], [full_name], [pdes], [name], [prefix], [neo_flag], "
            "[pha_flag], [diameter], [absolute_magnitude], [albedo], [diameter_sigma], "
            "[created_at], [neo_id]) VALUES ("
            f"{sql_int(a['id_internal'])}, {sql_int(a['spkid'])}, "
            f"{sql_text(a['full_name'], allow_null=False, empty_as_null=False)}, "
            f"{sql_text(a['pdes'], allow_null=False, empty_as_null=False)}, "
            f"{sql_text(a['name'])}, "
            f"{sql_text(a['prefix'], allow_null=False, empty_as_null=False)}, "
            f"{sql_text(a['neo_flag'], allow_null=False, empty_as_null=False)}, "
            f"{sql_text(a['pha_flag'], allow_null=False, empty_as_null=False)}, "
            f"{sql_float(a['diameter'])}, {sql_float(a['absolute_magnitude'])}, "
            f"{sql_float(a['albedo'])}, {sql_float(a['diameter_sigma'])}, "
            "SYSDATETIME(), "
            f"{sql_text(a['neo_id'])}"
            ");"
        )

    orbit_lines = []
    for orbit_id in sorted(orbits.keys()):
        o = orbits[orbit_id]
        orbit_lines.append(
            "INSERT [dbo].[Orbit] "
            "([id_orbita], [epoch], [rms], [moid_ld], [epoch_mjd], [epoch_cal], [tp], [tp_cal], "
            "[per], [per_y], [equinox], [orbit_uncertainty], [condition_code], [e], [a], [q], "
            "[i], [om], [w], [ma], [ad], [n], [moid], [sigma_e], [sigma_a], [sigma_q], [sigma_i], "
            "[sigma_n], [sigma_ma], [sigma_om], [sigma_w], [sigma_ad], [sigma_tp], [sigma_per], "
            "[id_internal], [class]) VALUES ("
            f"{sql_text(o['id_orbita'], allow_null=False, empty_as_null=False)}, {sql_float(o['epoch'])}, {sql_float(o['rms'])}, "
            f"{sql_float(o['moid_ld'])}, {sql_float(o['epoch_mjd'])}, {sql_date(o['epoch_cal'])}, "
            f"{sql_float(o['tp'])}, {sql_date(o['tp_cal'])}, {sql_float(o['per'])}, "
            f"{sql_float(o['per_y'])}, {sql_text(o['equinox'], allow_null=False, empty_as_null=False)}, {sql_int(o['orbit_uncertainty'])}, "
            f"{sql_int(o['condition_code'])}, {sql_float(o['e'])}, {sql_float(o['a'])}, "
            f"{sql_float(o['q'])}, {sql_float(o['i'])}, {sql_float(o['om'])}, {sql_float(o['w'])}, "
            f"{sql_float(o['ma'])}, {sql_float(o['ad'])}, {sql_float(o['n'])}, {sql_float(o['moid'])}, "
            f"{sql_float(o['sigma_e'])}, {sql_float(o['sigma_a'])}, {sql_float(o['sigma_q'])}, "
            f"{sql_float(o['sigma_i'])}, {sql_float(o['sigma_n'])}, {sql_float(o['sigma_ma'])}, "
            f"{sql_float(o['sigma_om'])}, {sql_float(o['sigma_w'])}, {sql_float(o['sigma_ad'])}, "
            f"{sql_float(o['sigma_tp'])}, {sql_float(o['sigma_per'])}, {sql_int(o['id_internal'])}, "
            f"{sql_text(o['class'], allow_null=False, empty_as_null=False)}"
            ");"
        )

    return class_lines, asteroid_lines, orbit_lines


def read_text_with_bom(path):
    with open(path, "rb") as f:
        data = f.read()
    if data.startswith(b"\xff\xfe"):
        return data.decode("utf-16-le", errors="ignore")
    if data.startswith(b"\xfe\xff"):
        return data.decode("utf-16-be", errors="ignore")
    if data.startswith(b"\xef\xbb\xbf"):
        return data.decode("utf-8-sig", errors="ignore")
    return data.decode("utf-8", errors="ignore")


def write_sql(template_path, output_path, class_lines, asteroid_lines, orbit_lines):
    lines = read_text_with_bom(template_path).splitlines()

    def strip_prefix(line):
        return line.lstrip().lstrip("\ufeff")

    def insert_type(line):
        s = strip_prefix(line).lower()
        if s.startswith("insert [dbo].[asteroid]"):
            return "asteroid"
        if s.startswith("insert [dbo].[orbit]"):
            return "orbit"
        if s.startswith("insert [dbo].[class_orbital]"):
            return "class"
        return None

    found = {"asteroid": False, "orbit": False, "class": False}
    out_lines = []
    in_asteroid = False
    for line in lines:
        if line.strip().startswith("CREATE TABLE [dbo].[Asteroid]"):
            in_asteroid = True
        if in_asteroid:
            stripped = line.strip()
            if stripped.startswith("[spkid]") and "NOT NULL" in stripped:
                line = line.replace("NOT NULL", "NULL")
            if stripped.startswith("[neo_id]") and "NOT NULL" in stripped:
                line = line.replace("NOT NULL", "NULL")
            if stripped == ") ON [PRIMARY]":
                in_asteroid = False

        kind = insert_type(line)
        if kind == "class":
            if not found["class"]:
                out_lines.extend(class_lines)
                found["class"] = True
            continue
        if kind == "asteroid":
            if not found["asteroid"]:
                out_lines.extend(asteroid_lines)
                found["asteroid"] = True
            continue
        if kind == "orbit":
            if not found["orbit"]:
                out_lines.extend(orbit_lines)
                found["orbit"] = True
            continue
        out_lines.append(line)

    if not found["class"]:
        out_lines.extend(class_lines)
        found["class"] = True
    if not found["asteroid"]:
        out_lines.extend(asteroid_lines)
        found["asteroid"] = True
    if not found["orbit"]:
        out_lines.extend(orbit_lines)
        found["orbit"] = True

    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        for ln in out_lines:
            f.write(ln + "\n")


def main():
    print("Loading CSV data...")
    class_map, asteroids, orbits = build_data_from_csv(MERGED_CSV)
    print(f"Classes: {len(class_map)} | Asteroids: {len(asteroids)} | Orbits: {len(orbits)}")
    print("Building INSERT blocks...")
    class_lines, asteroid_lines, orbit_lines = build_insert_blocks(class_map, asteroids, orbits)
    print("Writing SQL...")
    write_sql(TEMPLATE_SQL, OUTPUT_SQL, class_lines, asteroid_lines, orbit_lines)
    print(f"Done: {OUTPUT_SQL}")


if __name__ == "__main__":
    main()
