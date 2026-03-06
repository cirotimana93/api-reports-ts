import io
from datetime import datetime, timezone, timedelta
from typing import Dict, List

try:
    from openpyxl import Workbook
except ImportError:
    Workbook = None


# ──────────────────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────────────────

def _fmt_datetime(iso_str: str, tz_offset: float = -5.0) -> str:
    """
    convierte iso utc a formato del excel de referencia:
    '5 Mar 2026, 15:01:34 (-05:00)'
    """
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        delta = timedelta(hours=tz_offset)
        dt_local = dt + delta
        sign = "+" if tz_offset >= 0 else "-"
        hh = int(abs(tz_offset))
        mm = int((abs(tz_offset) - hh) * 60)
        tz_label = f"({sign}{hh:02d}:{mm:02d})"
        return dt_local.strftime(f"%-d %b %Y, %H:%M:%S") + f" {tz_label}"
    except Exception:
        try:
            # fallback sin %-d (windows no soporta %-d)
            dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
            delta = timedelta(hours=tz_offset)
            dt_local = dt + delta
            sign = "+" if tz_offset >= 0 else "-"
            hh = int(abs(tz_offset))
            mm = int((abs(tz_offset) - hh) * 60)
            tz_label = f"({sign}{hh:02d}:{mm:02d})"
            day = str(dt_local.day)
            return dt_local.strftime(f"{day} %b %Y, %H:%M:%S") + f" {tz_label}"
        except Exception:
            return iso_str


def _bool_str(val) -> str:
    if val is None:
        return "false"
    return str(val).lower()


def _unit_label(obj: Dict) -> str:
    """formatea el campo de unidad: 'id name'"""
    if not obj:
        return ""
    uid = obj.get("id", "")
    name = obj.get("name", "")
    return f"{uid} {name}".strip()


def _won(ticket: Dict) -> str:
    won_data = ticket.get("wonData")
    if not won_data:
        return ""
    won_amount = won_data.get("wonAmount")
    return str(won_amount) if won_amount is not None else ""


def _bonus(ticket: Dict) -> str:
    won_data = ticket.get("wonData")
    if not won_data:
        return ""
    won_bonus = won_data.get("wonBonus")
    return str(won_bonus) if won_bonus is not None else ""


def _jackpot_won(ticket: Dict) -> str:
    jackpot = ticket.get("jackpotData")
    if not jackpot:
        return ""
    return str(jackpot.get("wonAmount", ""))


def _target_balance(ticket: Dict) -> str:
    wd = ticket.get("winningData")
    if not wd:
        return ""
    tb = wd.get("targetBalance")
    return str(tb) if tb is not None else ""


def _target_rtp(ticket: Dict) -> str:
    wd = ticket.get("winningData")
    if not wd:
        return ""
    rtp = wd.get("targetRTP")
    if rtp is None:
        return ""
    # convertir a porcentaje como en el excel: 0.972972 -> '97.3%'
    return f"{rtp * 100:.1f}%"


# ──────────────────────────────────────────────────────────────────────────────
# headers y mapeo
# ──────────────────────────────────────────────────────────────────────────────

VGR_HEADERS = [
    "Date,Time",
    "Ticket ID",
    "Parent ID",
    "Is test",
    "Printed",
    "Issued from",
    "Sold by",
    "Games",
    "Stake",
    "Status",
    "Won",
    "Bonus",
    "Jackpot Won",
    "Target balance",
    "Target RTP",
]


def _map_vgr_row(ticket: Dict) -> Dict:
    tz = ticket.get("advancedInfo", {}).get("userTz", -5.0) if ticket.get("advancedInfo") else -5.0
    unit = ticket.get("unit") or {}
    sell_staff = ticket.get("sellStaff") or {}
    advanced = ticket.get("advancedInfo") or {}

    return {
        "Date,Time":      _fmt_datetime(ticket.get("timeRegister", ""), tz),
        "Ticket ID":      ticket.get("ticketId", ""),
        "Parent ID":      ticket.get("parentTicketId") or "",
        "Is test":        _bool_str(advanced.get("testMode")),
        "Printed":        str(ticket.get("timePrint") is not None).upper(),
        "Issued from":    _unit_label(unit),
        "Sold by":        _unit_label(sell_staff),
        "Games":          ticket.get("numBets", ""),
        "Stake":          ticket.get("stake", ""),
        "Status":         ticket.get("status", ""),
        "Won":            _won(ticket),
        "Bonus":          _bonus(ticket),
        "Jackpot Won":    _jackpot_won(ticket),
        "Target balance": _target_balance(ticket),
        "Target RTP":     _target_rtp(ticket),
    }


def json_to_excel_vgr(data: List[Dict]) -> bytes:
    """convierte lista de tickets de vgr a bytes xlsx"""
    wb = Workbook()
    ws = wb.active
    ws.title = "Tickets"
    ws.append(VGR_HEADERS)
    for ticket in data:
        row = _map_vgr_row(ticket)
        ws.append([row.get(h, "") for h in VGR_HEADERS])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
