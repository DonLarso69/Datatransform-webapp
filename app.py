import io
import re
from typing import List, Tuple
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Datentransformation", page_icon="📊", layout="wide")

# ---------- Helpers ----------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).replace("\xa0", " ").strip() for c in df.columns]
    return df

def find_col_positions(df: pd.DataFrame, regex_pattern: str) -> List[int]:
    positions = []
    for i, c in enumerate(df.columns):
        if re.search(regex_pattern, c):
            positions.append(i)
    def suffix_num(cname: str) -> int:
        m = re.search(r"(\d+)$", cname)
        return int(m.group(1)) if m else 0
    positions = sorted(positions, key=lambda i: suffix_num(df.columns[i]))
    return positions

def col_letter_to_index(col_letters: str) -> int:
    col_letters = col_letters.upper()
    total = 0
    for ch in col_letters:
        total = total * 26 + (ord(ch) - ord('A') + 1)
    return total - 1

def replace_text_values(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    repl = {"trifft vollkommen zu": "1", "trifft überhaupt nicht zu": "6"}
    obj_cols = df.select_dtypes(include=["object"]).columns
    df[obj_cols] = df[obj_cols].replace(repl)
    return df

def append_only_new_ids(existing: pd.DataFrame, new_df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    if existing is None or existing.empty:
        if "ID" in new_df.columns:
            new_ids = sorted(set(new_df["ID"].dropna().astype(str)))
        else:
            new_ids = []
        return new_df, new_ids
    if "ID" not in existing.columns or "ID" not in new_df.columns:
        merged = pd.concat([existing, new_df], ignore_index=True)
        return merged, []
    existing_ids = set(existing["ID"].dropna().astype(str))
    new_rows = new_df[new_df["ID"].astype(str).apply(lambda x: x not in existing_ids)]
    new_ids = sorted(set(new_rows["ID"].dropna().astype(str)))
    merged = pd.concat([existing, new_rows], ignore_index=True) if not new_rows.empty else existing
    return merged, new_ids

def to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Ergebnis") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()

def to_csv_bytes(lines: List[str], header: str = "ID") -> bytes:
    out = io.StringIO()
    out.write(header + "\n")
    for x in lines:
        out.write(f"{x}\n")
    return out.getvalue().encode("utf-8")


# ---------- Transformation Ausbildungen ----------
def transform_ausbildungen(df_src: pd.DataFrame, only_first_row_per_id: bool = True) -> pd.DataFrame:
    df = normalize_columns(df_src)

    teacher_pos = [i for i, c in enumerate(df.columns)
                   if (str(c).startswith("Name Lehrkraft") or "Dozent" in str(c)) and "Wie viele Lehrkräfte" not in str(c)]
    teacher_pos = sorted(teacher_pos, key=lambda i: int(re.search(r"(\d+)$", str(df.columns[i])).group(1)) if re.search(r"(\d+)$", str(df.columns[i])) else 0)

    under_pos    = find_col_positions(df, r"^vermittelt Unterrichtsinhalte verständlich\d*$")
    support_pos  = find_col_positions(df, r"^geht im Unterricht auf die Teilnehmer\*innen ein.*\d*$")
    feedback_pos = find_col_positions(df, r"^gibt Rückmeldungen zum jeweiligen Leistungsstand.*\d*$")
    comm_pos     = find_col_positions(df, r"^Die Kommunikation war freundlich und zugewandt\d*$")

    static_names = ["ID", "Startzeit", "Fertigstellungszeit", "E-Mail", "Kurstitel", "Kursnummer", "An welchem Standort findet die Ausbildung statt?"]
    static_pos = [i for i, c in enumerate(df.columns) if c in static_names]

    block_positions = set(teacher_pos + under_pos + support_pos + feedback_pos + comm_pos)
    once_per_id_pos = [i for i in range(len(df.columns)) if i not in block_positions and i not in static_pos]

    if not teacher_pos:
        return pd.DataFrame()

    max_sets = max(len(teacher_pos), len(under_pos), len(support_pos), len(feedback_pos), len(comm_pos))
    records = []
    for _, row in df.iterrows():
        for i in range(max_sets):
            rec = {df.columns[p]: row.iloc[p] for p in static_pos}
            def get(pos_list, idx): return row.iloc[pos_list[idx]] if idx < len(pos_list) else None
            rec["Name Lehrkraft"] = get(teacher_pos, i)
            rec["vermittelt Unterrichtsinhalte verständlich"] = get(under_pos, i)
            rec["geht auf Teilnehmer*innen ein"] = get(support_pos, i)
            rec["gibt Rückmeldung zum Leistungsstand"] = get(feedback_pos, i)
            rec["Kommunikation freundlich & zugewandt"] = get(comm_pos, i)
            for p in once_per_id_pos:
                rec[df.columns[p]] = row.iloc[p]
            if pd.notna(rec["Name Lehrkraft"]) and str(rec["Name Lehrkraft"]).strip() != "":
                records.append(rec)

    long_df = pd.DataFrame(records)

    if "ID" in long_df.columns and only_first_row_per_id and not long_df.empty:
        once_cols = [df.columns[p] for p in once_per_id_pos]
        for col in once_cols:
            if col in long_df.columns:
                long_df[col] = long_df.groupby("ID")[col].transform(lambda s: [s.iloc[0]] + [""] * (len(s) - 1))
    return replace_text_values(long_df)


# ---------- Transformation Fort und Weiterbildungen ----------
def transform_weiterbildungen(df_src: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df_src)

    # Bereiche
    general_pos = list(range(0, 8))            # A bis H
    iwk_pos = list(range(9, 27))               # J bis AA
    ab_idx = col_letter_to_index("AB")
    en_idx = min(col_letter_to_index("EN"), len(df.columns) - 1)

    # Namensanker in AB bis EN
    pos_name = []
    for i in range(ab_idx, en_idx + 1):
        if "Name der*des Lehrkraft/Dozent*in" in str(df.columns[i]):
            pos_name.append(i)
    if not pos_name:
        return pd.DataFrame()

    def base_header(h):
        return re.sub(r"\d+$", "", str(h)).strip()

    records = []
    for _, row in df.iterrows():
        for k, start_idx in enumerate(pos_name):
            end_idx = pos_name[k + 1] if k + 1 < len(pos_name) else (en_idx + 1)
            name_val = row.iloc[start_idx]
            if pd.isna(name_val) or str(name_val).strip() == "" or str(name_val).strip().isdigit():
                continue

            rec = {}
            # A bis H immer mitführen
            for p in general_pos:
                if 0 <= p < len(df.columns):
                    rec[df.columns[p]] = row.iloc[p]
            # IWK J bis AA zunächst kopieren, wird gleich auf erste Zeile pro ID beschränkt
            for p in iwk_pos:
                if 0 <= p < len(df.columns):
                    rec[df.columns[p]] = row.iloc[p]
            # Lehrerblock
            for idx in range(start_idx + 1, end_idx):
                rec[base_header(df.columns[idx])] = row.iloc[idx]
            rec["Name Lehrkraft"] = name_val
            if "ID" in df.columns:
                rec["ID"] = row["ID"]
            records.append(rec)

    long_df = pd.DataFrame(records)

    # IWK nur in der ersten Zeile pro ID
    if not long_df.empty and "ID" in long_df.columns:
        iwk_cols = [df.columns[p] for p in iwk_pos if 0 <= p < len(df.columns)]
        for col in iwk_cols:
            if col in long_df.columns:
                long_df[col] = long_df.groupby("ID")[col].transform(lambda s: [s.iloc[0]] + [""] * (len(s) - 1))

    return replace_text_values(long_df)


# ---------- UI ----------
st.title("Datentransformation")

with st.sidebar:
    st.header("Modus")
    mode = st.radio("Bitte auswählen", ["Ausbildungen", "Fort und Weiterbildungen"], index=0)
    st.markdown("Optional kannst du eine bestehende Ausgabedatei hochladen. Es werden nur neue IDs angehängt.")

uploaded_files = st.file_uploader("Excel Dateien hochladen", type=["xlsx"], accept_multiple_files=True)
existing_output = st.file_uploader("Bestehende Ausgabedatei für Deduplikation hochladen (optional)", type=["xlsx"], accept_multiple_files=False)

if mode == "Ausbildungen":
    only_first = st.checkbox("Allgemeine Fragen nur in erster Zeile je ID füllen", value=True)
else:
    only_first = True  # bei Weiterbildungen fest so gewünscht

btn = st.button("Verarbeiten")

if btn:
    if not uploaded_files:
        st.warning("Bitte mindestens eine Eingabedatei hochladen.")
        st.stop()

    all_results = []
    for uf in uploaded_files:
        try:
            with pd.ExcelFile(uf) as xl:
                df_src = xl.parse(xl.sheet_names[0])
        except Exception as e:
            st.error(f"Fehler beim Lesen von {uf.name}: {e}")
            continue

        if mode == "Ausbildungen":
            out_df = transform_ausbildungen(df_src, only_first_row_per_id=only_first)
        else:
            out_df = transform_weiterbildungen(df_src)

        if out_df is None or out_df.empty:
            st.info(f"{uf.name}: Keine Lehrerblöcke erkannt oder Ergebnis leer.")
        else:
            all_results.append(out_df)

    if not all_results:
        st.warning("Keine verwertbaren Daten erzeugt.")
        st.stop()

    combined = pd.concat(all_results, ignore_index=True)

    # Optional bestehende Ausgabe berücksichtigen
    existing_df = None
    if existing_output is not None:
        try:
            with pd.ExcelFile(existing_output) as xl2:
                existing_df = xl2.parse(xl2.sheet_names[0])
        except Exception as e:
            st.error(f"Bestehende Ausgabedatei konnte nicht gelesen werden: {e}")

    merged, new_ids = append_only_new_ids(existing_df, combined)

    st.subheader("Ergebnis")
    st.write(f"Zeilen neu hinzugefügt: {len(new_ids)}")
    if new_ids:
        short_list = ", ".join(new_ids[:20]) + (f" … (+{len(new_ids) - 20} weitere)" if len(new_ids) > 20 else "")
        st.write(f"Neue IDs: {short_list}")

    # Downloads
    excel_bytes = to_excel_bytes(merged, sheet_name="Ergebnis")
    st.download_button(
        label="Excel Ergebnis herunterladen",
        data=excel_bytes,
        file_name=("Ausbildungen_bearbeitet.xlsx" if mode == "Ausbildungen" else "Weiterbildungen_bearbeitet.xlsx"),
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    csv_bytes = to_csv_bytes(new_ids, header="Neue_IDs")
    st.download_button(
        label="CSV mit neuen IDs herunterladen",
        data=csv_bytes,
        file_name="Neue_IDs.csv",
        mime="text/csv"
    )
