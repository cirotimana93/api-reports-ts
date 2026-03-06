import io
from datetime import datetime
from typing import Dict, List

try:
    from openpyxl import Workbook
except ImportError:
    Workbook = None


# ──────────────────────────────────────────────────────────────────────────────
# headers exactos del xlsx de referencia (reporte_teleservicios_*.xlsx)
# ──────────────────────────────────────────────────────────────────────────────

MVT_HEADERS = [
    "ID",
    "Origen de Cliente",
    "Local",
    "Fecha Registro",
    "Tipo",
    "Saldo",
    "Proveedor",
    "IP-TX",
    "ID-TX",
    "WEB-ID",
    "OPERATION-ID",
    "PLAYER-ID",
    "Tipo Documento",
    "Numero Documento",
    "Cliente",
    "Telefono",
    "Cajero",
    "Deposito(S/)",
    "Cuenta",
]


def _map_mvt_row(item: Dict) -> Dict:
    return {
        "ID":                str(item.get("id", "")),
        "Origen de Cliente": item.get("origen", ""),
        "Local":             item.get("tienda", ""),
        "Fecha Registro":    item.get("fecha_hora_registro", ""),
        "Tipo":              item.get("tipo_transaccion", ""),
        "Saldo":             item.get("tipo_saldo", ""),
        "Proveedor":         item.get("proveedor_nombre", ""),
        "IP-TX":             item.get("direccion_ip", ""),
        "ID-TX":             str(item.get("txn_id", "")),
        "WEB-ID":            str(item.get("web_id", "")),
        "OPERATION-ID":      str(item.get("operation_id", "")),
        "PLAYER-ID":         str(item.get("player_id", "")),
        "Tipo Documento":    item.get("tipo_doc", ""),
        "Numero Documento":  item.get("num_doc", ""),
        "Cliente":           item.get("cliente", ""),
        "Telefono":          item.get("telefono", ""),
        "Cajero":            item.get("cajero", ""),
        "Deposito(S/)":      item.get("monto", ""),
        "Cuenta":            item.get("cuenta", ""),
    }


def json_to_excel_mvt(data: List[Dict]) -> bytes:
    """convierte lista de registros de mvt a bytes xlsx"""
    wb = Workbook()
    ws = wb.active
    ws.append(MVT_HEADERS)
    for item in data:
        row = _map_mvt_row(item)
        ws.append([row.get(h, "") for h in MVT_HEADERS])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
