import io
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
from app.common.s3_utils import read_file_from_s3, upload_file_to_s3, copy_file_in_s3, delete_file_from_s3, get_latest_file_from_s3
from app.core.config import settings

class ReconciliationService:
    def __init__(self):
        self.bucket = settings.AWS_BUCKET_NAME
        self.report_prefix = "tls/reports/"
        self.processed_prefix = "tls/reports/processed/"

    def _load_df_from_s3(self, s3_key: str) -> Optional[pd.DataFrame]:
        content = read_file_from_s3(s3_key)
        if not content:
            return None
        # cargar excel desde memoria
        return pd.read_excel(io.BytesIO(content))

    def _get_latest_report(self, provider_name: str) -> Optional[str]:
        # buscar el reporte mas reciente del proveedor
        files = [f for f in list_files_in_s3(self.report_prefix) if provider_name.lower() in f.lower() and f.endswith(".xlsx")]
        files.sort(reverse=True)
        return files[0] if files else None

    async def run_reconciliation(self, date_str: str):
        # obtener lista de archivos en s3
        files_in_s3 = list_files_in_s3(self.report_prefix)
        
        mvt_file = self._get_latest_report_from_list(files_in_s3, "mvt")
        # first puede tener multiples reportes
        first_files = [f for f in files_in_s3 if f.split("/")[-1].lower().startswith("first_") and any(f.endswith(ext) for ext in [".xlsx", ".xls"]) and "processed/" not in f]
        vgr_file = self._get_latest_report_from_list(files_in_s3, "vgr")
        gr_file = self._get_latest_report_from_list(files_in_s3, "gr")
        lot_file = self._get_latest_report_from_list(files_in_s3, "lottingo")

        if not mvt_file:
            print("[ALERTA] no se encontro reporte de mvt")
            return None

        # cargar data de mvt
        df_mvt_all = self._load_df_from_s3(mvt_file)
        if df_mvt_all is None: return None

        # limpiar data de mvt
        df_mvt_all['ID-TX'] = df_mvt_all['ID-TX'].astype(str).str.strip()
        df_mvt_all['Deposito(S/)'] = pd.to_numeric(df_mvt_all['Deposito(S/)'], errors='coerce').fillna(0)
        
        # filtrar proveedores permitidos en mvt
        allowed_providers = ["Virtual Golden Race", "First", "Golden Race", "Lottingo", "MVT Golden Race"]
        df_mvt_raw = df_mvt_all[df_mvt_all['Proveedor'].isin(allowed_providers)].copy()

        # definir tipos de operacion mvt
        bet_types = ["Apuesta Generada"]
        win_types = ["Apuesta Pagada", "Apuesta Cancelada", "Apuesta Generada Rollback", "Apuesta Retornada"]

        summary_dfs = {}

        # funcion para analizar discrepancias por proveedor
        def analyze_provider(prov_name, df_prov, prov_id_col, prov_bet_col, prov_win_col, prov_status_col):
            # mapear nombre de proveedor a filtro mvt
            provider_filter = {
                "First": ["First"],
                "VGR": ["Virtual Golden Race"],
                "GR": ["Golden Race", "MVT Golden Race"],
                "Lottingo": ["Lottingo"]
            }.get(prov_name, [prov_name])

            # subset de mvt para el proveedor actual
            p_mvt = df_mvt_raw[df_mvt_raw['Proveedor'].isin(provider_filter)].copy()
            
            # separar mvt por apuestas y ganancias
            p_mvt_bets = p_mvt[p_mvt['Tipo'].isin(bet_types)]
            p_mvt_wins = p_mvt[p_mvt['Tipo'].isin(win_types)]
            
            cols = [
                "ID-TX", "Proveedor", "Tipo de Caso", 
                "MVT Monto Apuesta", "MVT Monto Ganado", 
                "Prov Monto Apuesta", "Prov Monto Ganado", 
                "Estado Proveedor", "MVT Tipo"
            ]
            
            # agregar columna de origen si es first
            if prov_name == "First":
                cols.append("Archivo Origen")

            findings = []

            for _, row in df_prov.iterrows():
                # normalizar id para comparacion
                raw_id = row[prov_id_col]
                if pd.api.types.is_number(raw_id):
                    p_id = str(int(float(raw_id))).strip()
                else:
                    p_id = str(raw_id).strip()
                
                p_bet = float(row[prov_bet_col]) if pd.notnull(row[prov_bet_col]) else 0
                p_win = float(row[prov_win_col]) if pd.notnull(row[prov_win_col]) and str(row[prov_win_col]).strip() != "" else 0
                p_status = str(row[prov_status_col]) if prov_status_col in row else ""

                # buscar registros en mvt
                mvt_bet_matches = p_mvt_bets[p_mvt_bets['ID-TX'] == p_id]
                mvt_win_matches = p_mvt_wins[p_mvt_wins['ID-TX'] == p_id]
                
                mvt_bet_val = float(mvt_bet_matches['Deposito(S/)'].sum()) if not mvt_bet_matches.empty else 0
                mvt_win_val = float(mvt_win_matches['Deposito(S/)'].sum()) if not mvt_win_matches.empty else 0
                
                issue = None
                found_type = ""
                if not mvt_bet_matches.empty: found_type = str(mvt_bet_matches['Tipo'].iloc[0])
                elif not mvt_win_matches.empty: found_type = str(mvt_win_matches['Tipo'].iloc[0])

                # etapa 1: no presente
                if mvt_bet_matches.empty and mvt_win_matches.empty:
                    issue = "ETAPA 01: No presente en MVT"
                # etapa 2: diferencia apuesta
                elif abs(mvt_bet_val - p_bet) > 0.01:
                    issue = "ETAPA 02: Diferencia en monto Apuesta"
                # etapa 3: diferencia ganancia
                elif p_win > 0 and abs(mvt_win_val - p_win) > 0.01:
                    issue = "ETAPA 03: Diferencia en monto Ganancia"

                if issue:
                    finding = {
                        "ID-TX": p_id,
                        "Proveedor": prov_name,
                        "Tipo de Caso": issue,
                        "MVT Monto Apuesta": mvt_bet_val,
                        "MVT Monto Ganado": mvt_win_val,
                        "Prov Monto Apuesta": p_bet,
                        "Prov Monto Ganado": p_win,
                        "Estado Proveedor": p_status,
                        "MVT Tipo": found_type
                    }
                    if prov_name == "First":
                        finding["Archivo Origen"] = row.get("Archivo Origen", "")
                    findings.append(finding)
            
            return pd.DataFrame(findings, columns=cols)

        # procesar reportes de first
        if first_files:
            first_dataframes = []
            for f in first_files:
                df = self._load_df_from_s3(f)
                if df is not None:
                    # identificar origen del reporte
                    file_name = f.split("/")[-1].lower()
                    if "bethistory" in file_name: source = "first_bethistory"
                    elif "openbets" in file_name: source = "first_openbets"
                    elif "declinedbets" in file_name: source = "first_declinedbets"
                    else: source = file_name
                    
                    df["Archivo Origen"] = source
                    first_dataframes.append(df)
            
            if first_dataframes:
                df_first = pd.concat(first_dataframes, ignore_index=True)
                summary_dfs["First"] = analyze_provider("First", df_first, "Purchase_ID", "Stake", "Return", "Bet_Status")

        # procesar vgr
        if vgr_file:
            df_vgr = self._load_df_from_s3(vgr_file)
            if df_vgr is not None:
                summary_dfs["VGR"] = analyze_provider("VGR", df_vgr, "Ticket ID", "Stake", "Won", "Status")

        # procesar gr
        if gr_file:
            df_gr = self._load_df_from_s3(gr_file)
            if df_gr is not None:
                summary_dfs["GR"] = analyze_provider("GR", df_gr, "Ticket ID", "Stake", "Won", "Status")

        # procesar lottingo
        if lot_file:
            df_lot = self._load_df_from_s3(lot_file)
            if df_lot is not None:
                # filtro room name solicitado
                df_lot_filt = df_lot[df_lot['Room Name'] == "MVT Televentas "].copy()
                summary_dfs["Lottingo"] = analyze_provider("Lottingo", df_lot_filt, "Ticket Id", "Cantidad", "Winning", "Estado")

        # generar reporte final excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            print("\n*** resumen de conciliacion ***")
            for sheet, df_find in summary_dfs.items():
                if not df_find.empty:
                    df_find.to_excel(writer, sheet_name=sheet, index=False)
                else:
                    pd.DataFrame(columns=["ID-TX", "Resultado"]).to_excel(writer, sheet_name=sheet, index=False)
                
                # calcular estadisticas para reporte en consola
                missing = len(df_find[df_find['Tipo de Caso'].str.contains("ETAPA 01")])
                
                total_prov = 0
                if sheet == "First": total_prov = len(df_first) if 'df_first' in locals() else 0
                elif sheet == "VGR": total_prov = len(df_vgr) if 'df_vgr' in locals() else 0
                elif sheet == "GR": total_prov = len(df_gr) if 'df_gr' in locals() else 0
                elif sheet == "Lottingo": total_prov = len(df_lot_filt) if 'df_lot_filt' in locals() else 0
                
                found_count = total_prov - missing
                
                prov_filter = {
                    "First": ["First"],
                    "VGR": ["Virtual Golden Race"],
                    "GR": ["Golden Race", "MVT Golden Race"],
                    "Lottingo": ["Lottingo"]
                }.get(sheet, [])
                mvt_rows = df_mvt_raw[df_mvt_raw['Proveedor'].isin(prov_filter)]
                
                print(f"Total de operaciones en proveedor {sheet}: {total_prov}")
                print(f"Total de operaciones del proveedor {sheet} en MVT: {len(mvt_rows)}")
                print(f"Tx del proveedor {sheet} presentes en MVT: {found_count}")
                print(f"Tx del proveedor {sheet} no presentes en MVT: {missing}")
                print("-" * 30)

        # subir reporte a s3
        suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        final_filename = f"conciliacion_completa_{suffix}.xlsx"
        upload_file_to_s3(output.getvalue(), f"tls/reports/{final_filename}")

        # mover archivos procesados
        sources = [mvt_file, vgr_file, gr_file, lot_file] + first_files
        unique_sources = list(set([f for f in sources if f]))
        
        for f in unique_sources:
            target = f.replace(self.report_prefix, self.processed_prefix)
            try:
                copy_file_in_s3(f, target)
                delete_file_from_s3(f)
            except Exception as e:
                print(f"[ALERTA] no se pudo mover {f}: {e}")

        return final_filename

    def _get_latest_report_from_list(self, files: List[str], provider_name: str) -> Optional[str]:
        # extensiones aceptadas
        exts = [".xlsx", ".xls"]
        # evitar conflictos de nombre gr y vgr
        file_pat = f"{provider_name.lower()}_reporte"
        
        matches = [
            f for f in files 
            if f.split("/")[-1].lower().startswith(file_pat)
            and any(f.endswith(e) for e in exts)
            and "processed/" not in f
        ]
        matches.sort(reverse=True)
        return matches[0] if matches else None

def list_files_in_s3(prefix: str):
    # listar archivos en s3 usando utilitario
    from app.common.s3_utils import list_files_in_s3 as ls
    return ls(prefix)
