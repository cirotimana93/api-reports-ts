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
        # cargar excel (los IDs ya vienen como string desde los conversores)
        return pd.read_excel(io.BytesIO(content))

    def _get_latest_report(self, provider_name: str) -> Optional[str]:
        # buscar el reporte mas reciente del proveedor
        files = [f for f in list_files_in_s3(self.report_prefix) if provider_name.lower() in f.lower() and f.endswith(".xlsx")]
        files.sort(reverse=True)
        return files[0] if files else None
    async def run_reconciliation(self, date_str: str):
        # captura de tiempo de inicio
        start_time_dt = datetime.now()
        start_time_str = start_time_dt.strftime('%H:%M:%S %d/%m/%Y')
        print(f"Fecha de inicio de proceso: {start_time_str}")

        # obtener lista de archivos en s3
        files_in_s3 = list_files_in_s3(self.report_prefix)
        
        mvt_file = self._get_latest_report_from_list(files_in_s3, "mvt")
        # first independiente
        first_files = [
            f for f in files_in_s3 
            if f.split("/")[-1].lower().startswith("first_") 
            and any(f.endswith(ext) for ext in [".xlsx", ".xls"]) 
            and "processed/" not in f
        ]
        vgr_file = self._get_latest_report_from_list(files_in_s3, "vgr")
        gr_file = self._get_latest_report_from_list(files_in_s3, "gr")
        lot_file = self._get_latest_report_from_list(files_in_s3, "lottingo")

        if not mvt_file:
            print("[ALERTA] no se encontro reporte de mvt")
            return None

        # cargar data de mvt
        df_mvt_all = self._load_df_from_s3(mvt_file)
        if df_mvt_all is None: return None

        # eliminar duplicados por columna "ID" antes de procesar
        if "ID" in df_mvt_all.columns:
            initial_count = len(df_mvt_all)
            df_mvt_all = df_mvt_all.drop_duplicates(subset=["ID"], keep="first")
            print(f"[MVT] Duplicados eliminados: {initial_count - len(df_mvt_all)}")

        # limpiar data de mvt (forzar string y quitar .0 si pandas lo trato como float)
        df_mvt_all['ID-TX'] = df_mvt_all['ID-TX'].astype(str).str.strip()
        df_mvt_all['ID-TX'] = df_mvt_all['ID-TX'].apply(lambda x: x[:-2] if x.endswith('.0') else x)
        
        df_mvt_all['Deposito(S/)'] = pd.to_numeric(df_mvt_all['Deposito(S/)'], errors='coerce').fillna(0)
        
        # filtrar proveedores permitidos en mvt
        allowed_providers = ["Virtual Golden Race", "First", "Golden Race", "Lottingo", "MVT Golden Race"]
        df_mvt_raw = df_mvt_all[df_mvt_all['Proveedor'].isin(allowed_providers)].copy()

        # definir tipos de operacion mvt
        bet_types = ["Apuesta Generada"]
        win_types = ["Apuesta Pagada", "Apuesta Cancelada", "Apuesta Generada Rollback", "Apuesta Retornada"]

        summary_dfs = {}
        # coleccion de ids procesados para el reporte mvt no encontrados
        all_seen_ids = set()

        # optimizacion: pre-agrupar mvt por proveedor y tipo para busqueda instantanea
        mvt_lookup = {}
        for prov in allowed_providers:
            prov_df = df_mvt_raw[df_mvt_raw['Proveedor'] == prov].copy()
            
            # indexar apuestas
            bets = prov_df[prov_df['Tipo'].isin(bet_types)]
            bets_indexed = bets.groupby('ID-TX').agg({
                'Deposito(S/)': 'sum',
                'Tipo': 'first',
                'Fecha Registro': 'first',
                'Numero Documento': 'first',
                'Cliente': 'first'
            }).to_dict('index')
            
            # indexar ganancias
            wins = prov_df[prov_df['Tipo'].isin(win_types)]
            wins_indexed = wins.groupby('ID-TX').agg({
                'Deposito(S/)': 'sum',
                'Tipo': 'first',
                'Fecha Registro': 'first',
                'Numero Documento': 'first',
                'Cliente': 'first'
            }).to_dict('index')
            
            mvt_lookup[prov] = {'bets': bets_indexed, 'wins': wins_indexed}

        # funcion para analizar discrepancias por proveedor
        def analyze_provider(prov_name, df_prov, prov_id_col, prov_bet_col, prov_win_col, prov_status_col, prov_date_col, extra_cols=None):
            # mapear nombre de proveedor a filtros de mvt_lookup
            lookups_to_check = {
                "First": ["First"],
                "VGR": ["Virtual Golden Race"],
                "GR": ["Golden Race", "MVT Golden Race"],
                "Lottingo": ["Lottingo"]
            }.get(prov_name, [prov_name])

            cols = [
                "ID-TX", "Proveedor", "Tipo de Caso", 
                "MVT Monto Apuesta", "MVT Monto Ganado", 
                "Prov Monto Apuesta", "Prov Monto Ganado", 
                "Estado Proveedor", "MVT Tipo", "Fecha Registro",
                "Fecha Proveedor", "DNI", "Cliente"
            ]
            
            if extra_cols:
                cols.extend(extra_cols.keys())

            if prov_name == "First":
                cols.append("Archivo Origen")

            findings = []

            for _, row in df_prov.iterrows():
                # normalizacion robusta de id para evitar redondeos si viniera como float o str
                raw_id = row[prov_id_col]
                if pd.isna(raw_id):
                    p_id = ""
                else:
                    p_id = str(raw_id).strip()
                    if p_id.endswith('.0'):
                        p_id = p_id[:-2]
                
                # registrar id visto para el reporte de mvt no encontrados
                all_seen_ids.add(p_id)

                p_bet = float(row[prov_bet_col]) if pd.notnull(row[prov_bet_col]) else 0
                p_win = float(row[prov_win_col]) if pd.notnull(row[prov_win_col]) and str(row[prov_win_col]).strip() != "" else 0
                p_status = str(row[prov_status_col]) if prov_status_col in row else ""
                p_date = str(row[prov_date_col]) if prov_date_col in row else ""

                mvt_bet_val = 0
                mvt_win_val = 0
                found_info = None

                # buscar en los indices optimizados
                for prov_key in lookups_to_check:
                    lookup = mvt_lookup.get(prov_key)
                    if not lookup: continue
                    
                    bet_data = lookup['bets'].get(p_id)
                    if bet_data:
                        mvt_bet_val += bet_data['Deposito(S/)']
                        if not found_info: found_info = bet_data
                    
                    win_data = lookup['wins'].get(p_id)
                    if win_data:
                        mvt_win_val += win_data['Deposito(S/)']
                        if not found_info: found_info = win_data

                issue = None
                found_type = found_info['Tipo'] if found_info else ""
                fecha_reg = found_info['Fecha Registro'] if found_info else ""
                dni = found_info['Numero Documento'] if found_info else ""
                cliente = found_info['Cliente'] if found_info else ""

                if not found_info:
                    issue = "ETAPA 01: No presente en MVT"
                elif abs(mvt_bet_val - p_bet) > 0.01:
                    issue = "ETAPA 02: Diferencia en monto Apuesta"
                elif mvt_win_val > 0 and abs(mvt_win_val - p_win) > 0.01:
                    issue = "ETAPA 03: Diferencia en monto Ganancia"

                if issue:
                    entry = {
                        "ID-TX": p_id, "Proveedor": prov_name, "Tipo de Caso": issue,
                        "MVT Monto Apuesta": mvt_bet_val, "MVT Monto Ganado": mvt_win_val,
                        "Prov Monto Apuesta": p_bet, "Prov Monto Ganado": p_win,
                        "Estado Proveedor": p_status, "MVT Tipo": found_type,
                        "Fecha Registro": fecha_reg, "Fecha Proveedor": p_date,
                        "DNI": dni, "Cliente": cliente
                    }
                    
                    if extra_cols:
                        for label, src_col in extra_cols.items():
                            entry[label] = row.get(src_col, "")
                            
                    if prov_name == "First":
                        entry["Archivo Origen"] = row.get("Archivo Origen", "")
                        
                    findings.append(entry)
            
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
                    else: source = file_name
                    
                    df["Archivo Origen"] = source
                    first_dataframes.append(df)
            
            if first_dataframes:
                df_first = pd.concat(first_dataframes, ignore_index=True)
                summary_dfs["First"] = analyze_provider("First", df_first, "Purchase_ID", "Stake", "Return", "Bet_Status", "Bet_Date_And_Time", extra_cols={"Customer ID": "Customer_ID"})

        # procesar vgr (omitir CANCELLED y REJECTED)
        if vgr_file:
            df_vgr_raw = self._load_df_from_s3(vgr_file)
            if df_vgr_raw is not None:
                df_vgr = df_vgr_raw[~df_vgr_raw['Status'].isin(["CANCELLED", "REJECTED"])].copy()
                summary_dfs["VGR"] = analyze_provider("VGR", df_vgr, "Ticket ID", "Stake", "Won", "Status", "Date,Time", extra_cols={"Issued from": "Issued from"})

        # procesar gr (omitir CANCELLED y REJECTED)
        if gr_file:
            df_gr_raw = self._load_df_from_s3(gr_file)
            if df_gr_raw is not None:
                df_gr = df_gr_raw[~df_gr_raw['Status'].isin(["CANCELLED", "REJECTED"])].copy()
                summary_dfs["GR"] = analyze_provider("GR", df_gr, "Ticket ID", "Stake", "Won", "Status", "Date,Time", extra_cols={"Issued from": "Issued from"})

        # procesar lottingo
        if lot_file:
            df_lot = self._load_df_from_s3(lot_file)
            if df_lot is not None:
                # filtro room name solicitado
                df_lot_filt = df_lot[df_lot['Room Name'] == "MVT Televentas "].copy()
                summary_dfs["Lottingo"] = analyze_provider("Lottingo", df_lot_filt, "Ticket Id", "Cantidad", "Winning", "Estado", "Creado en")

        # reporte de mvt no encontrados en ningun proveedor (solo Apuesta Generada)
        mvt_not_found = df_mvt_raw[
            (~df_mvt_raw['ID-TX'].isin(all_seen_ids)) & 
            (df_mvt_raw['Tipo'] == "Apuesta Generada")
        ].copy()
        if not mvt_not_found.empty:
            summary_dfs["MVT no encontrados"] = mvt_not_found[[
                "ID-TX", "Proveedor", "Deposito(S/)", "Tipo", "Fecha Registro", "Numero Documento", "Cliente"
            ]].rename(columns={
                "Deposito(S/)": "Monto MVT", 
                "Tipo": "MVT Tipo", 
                "Numero Documento": "DNI"
            })

        # generar reporte final excel
        output = io.BytesIO()
        console_summary = ""
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            header_str = "\n*** resumen de conciliacion ***\n"
            print(header_str)
            console_summary += header_str
            
            for sheet, df_find in summary_dfs.items():
                if not df_find.empty:
                    df_find.to_excel(writer, sheet_name=sheet, index=False)
                else:
                    pd.DataFrame(columns=["ID-TX", "Resultado"]).to_excel(writer, sheet_name=sheet, index=False)
                
                # omitir estadisticas para la pestana especial de mvt no encontrados
                if sheet == "MVT no encontrados": continue

                # calcular estadisticas para reporte en consola
                missing = len(df_find[df_find['Tipo de Caso'].str.contains("ETAPA 01")]) if 'Tipo de Caso' in df_find.columns else 0
                
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
                
                sum_lines = [
                    f"Total de operaciones en proveedor {sheet}: {total_prov}",
                    f"Total de operaciones del proveedor {sheet} en MVT: {len(mvt_rows)}",
                    f"Tx del proveedor {sheet} presentes en MVT: {found_count}",
                    f"Tx del proveedor {sheet} no presentes en MVT: {missing}",
                    "-" * 30 + "\n"
                ]
                
                for line in sum_lines:
                    print(line)
                    console_summary += line + "\n"

        # captura de tiempo de fin
        end_time_dt = datetime.now()
        end_time_str = end_time_dt.strftime('%H:%M:%S %d/%m/%Y')
        print(f"Fecha de fin de proceso: {end_time_str}")

        # subir reporte a s3
        suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        final_filename = f"conciliacion_completa_{suffix}.xlsx"
        report_bytes = output.getvalue()
        upload_file_to_s3(report_bytes, f"tls/reports/processed_report/{final_filename}")

        # enviar correo con el reporte y el resumen
        try:
            from app.common.mail import sendMailOffice365
            email_to = settings.GRAPH_EMAIL_TO.split(",")
            subject = f"Reporte de conciliacion - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
            
            # construir cuerpo del correo con el resumen y tiempos
            email_content = f"<p>Fecha de inicio de proceso: {start_time_str}</p>"
            email_content += f"<p>Fecha de fin de proceso: {end_time_str}</p>"
            email_content += "<h3>Resumen de Reportes</h3>"
            email_content += "<pre>" + console_summary + "</pre>"
            email_content += "<p>Se adjunta el reporte detallado en Excel.</p>"
            
            await sendMailOffice365(
                subject=subject,
                content=email_content,
                to_recipients=email_to,
                attachment_content=report_bytes,
                attachment_name=final_filename
            )
        except Exception as e:
            print(f"[ALERTA] no se pudo enviar el correo: {e}")

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
            and "processed_report/" not in f
        ]
        matches.sort(reverse=True)
        return matches[0] if matches else None

def list_files_in_s3(prefix: str):
    # listar archivos en s3 usando utilitario
    from app.common.s3_utils import list_files_in_s3 as ls
    return ls(prefix)
