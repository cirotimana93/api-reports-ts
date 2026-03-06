import io
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

try:
    import openpyxl
    from openpyxl import Workbook
except ImportError:
    openpyxl = None
    Workbook = None


# ──────────────────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────────────────

def _utc_to_local(iso_str: str) -> str:
    """convierte iso utc a hora local (utc-5) en formato 'YYYY-MM-DD HH:MM:SS UTC'"""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        dt_local = dt.astimezone(timezone(timedelta(hours=-5)))
        return dt_local.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return iso_str


def _join(values: list, sep: str = ";") -> str:
    """une lista de valores con separador"""
    return sep.join(str(v) for v in values if v not in (None, ""))


def _live_prematch(is_live: bool) -> int:
    """0=prematch, 1=live segun lo que usa el csv"""
    return 1 if is_live else 0


def _bet_type_name(bet_type_id: int) -> str:
    mapping = {1: "Single", 2: "Combo bets", 3: "System"}
    return mapping.get(int(bet_type_id), str(bet_type_id))


def _sports_level(selections: list) -> int:
    """cantidad de deportes distintos en el ticket"""
    sports = {s.get("branchID") for s in selections}
    return len(sports)


def _write_xlsx(headers: list, rows: list) -> bytes:
    """genera bytes de un xlsx con los headers y filas dados"""
    wb = Workbook()
    ws = wb.active
    ws.append(headers)
    for row in rows:
        ws.append([row.get(h, "") for h in headers])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ──────────────────────────────────────────────────────────────────────────────
# bethistory
# ──────────────────────────────────────────────────────────────────────────────

BETHISTORY_HEADERS = [
    "Purchase_ID", "Bets_BetID", "Bet_Type", "Bet_Date_And_Time",
    "Customer_ID", "Username", "Merchant_Customer_Code",
    "Sports_Level", "Live_Prematch",
    "Sports", "Leagues", "Events", "EventDate", "Market", "Selection",
    "Operator", "Brand",
    "Stake", "Stake_ARS", "Stake_BRL", "Stake_CNY", "Stake_COP",
    "Stake_EUR", "Stake_GBP", "Stake_KRW", "Stake_MYR", "Stake_THB", "Stake_USD",
    "Return", "Return_ARS", "Return_BRL", "Return_CNY", "Return_COP",
    "Return_EUR", "Return_GBP", "Return_KRW", "Return_MYR", "Return_THB", "Return_USD",
    "Win_Loss", "Win_Loss_ARS", "Win_Loss_BRL", "Win_Loss_CNY", "Win_Loss_COP",
    "Win_Loss_EUR", "Win_Loss_GBP", "Win_Loss_KRW", "Win_Loss_MYR", "Win_Loss_THB", "Win_Loss_USD",
    "Odds_Style", "Client_Odds", "Decimal_Odds",
    "Bet_Status", "Resettled",
    "Settlement_Date",
    "File_Generated_Time",
]

_STATUS_MAP = {0: "Opened", 1: "Lost", 2: "Won", 3: "Void", 4: "Cashout", 5: "Partial Win", 6: "Partial Loss"}
_ODDS_MAP = {0: "American", 1: "Decimal", 2: "Fractional"}


def _map_bethistory_row(purchase: Dict) -> Dict:
    bets = purchase.get("bets", [])
    bet = bets[0] if bets else {}
    selections = bet.get("selections", [])

    customer = purchase.get("customer", {})
    stake_d = bet.get("stakeDecimal", {})
    ret_d = bet.get("return", {})
    pl_d = bet.get("pl", {})
    settlement = purchase.get("settlementHistory", {})

    row = {
        "Purchase_ID":            str(purchase.get("purchaseID", "")),
        "Bets_BetID":             str(bet.get("betID", "")),
        "Bet_Type":               _bet_type_name(bet.get("betTypeID", 1)),
        "Bet_Date_And_Time":      _utc_to_local(purchase.get("creationDate", "")),
        "Customer_ID":            customer.get("customerID", ""),
        "Username":               customer.get("loginName", ""),
        "Merchant_Customer_Code": customer.get("merchantCustomerCode", ""),
        "Sports_Level":           _sports_level(selections),
        "Live_Prematch":          _live_prematch(bet.get("isLive", False)),
        "Sports":                 _join([s.get("branchName", "") for s in selections]),
        "Leagues":                _join([s.get("leagueName", "") for s in selections]),
        "Events":                 _join([s.get("eventName", "") for s in selections]),
        "EventDate":              _join([_utc_to_local(s.get("eventDate", "")) for s in selections]),
        "Market":                 _join([s.get("eventTypeName", "") for s in selections]),
        "Selection":              _join([s.get("yourBet", "") for s in selections]),
        "Operator":               customer.get("agentID", ""),
        "Brand":                  customer.get("agentName", ""),
        # stake
        "Stake":                  stake_d.get("stakeDecimal", ""),
        "Stake_ARS":              stake_d.get("stakeDecimalARS", ""),
        "Stake_BRL":              stake_d.get("stakeDecimalBRL", ""),
        "Stake_CNY":              stake_d.get("stakeDecimalCNY", ""),
        "Stake_COP":              stake_d.get("stakeDecimalCOP", ""),
        "Stake_EUR":              stake_d.get("stakeDecimalEUR", ""),
        "Stake_GBP":              stake_d.get("stakeDecimalGBP", ""),
        "Stake_KRW":              stake_d.get("stakeDecimalKRW", ""),
        "Stake_MYR":              stake_d.get("stakeDecimalMYR", ""),
        "Stake_THB":              stake_d.get("stakeDecimalTHB", ""),
        "Stake_USD":              stake_d.get("stakeDecimalUSD", ""),
        # return
        "Return":                 ret_d.get("return", ""),
        "Return_ARS":             ret_d.get("returnARS", ""),
        "Return_BRL":             ret_d.get("returnBRL", ""),
        "Return_CNY":             ret_d.get("returnCNY", ""),
        "Return_COP":             ret_d.get("returnCOP", ""),
        "Return_EUR":             ret_d.get("returnEUR", ""),
        "Return_GBP":             ret_d.get("returnGBP", ""),
        "Return_KRW":             ret_d.get("returnKRW", ""),
        "Return_MYR":             ret_d.get("returnMYR", ""),
        "Return_THB":             ret_d.get("returnTHB", ""),
        "Return_USD":             ret_d.get("returnUSD", ""),
        # win/loss (pl)
        "Win_Loss":               pl_d.get("pl", ""),
        "Win_Loss_ARS":           pl_d.get("plARS", ""),
        "Win_Loss_BRL":           pl_d.get("plBRL", ""),
        "Win_Loss_CNY":           pl_d.get("plCNY", ""),
        "Win_Loss_COP":           pl_d.get("plCOP", ""),
        "Win_Loss_EUR":           pl_d.get("plEUR", ""),
        "Win_Loss_GBP":           pl_d.get("plGBP", ""),
        "Win_Loss_KRW":           pl_d.get("plKRW", ""),
        "Win_Loss_MYR":           pl_d.get("plMYR", ""),
        "Win_Loss_THB":           pl_d.get("plTHB", ""),
        "Win_Loss_USD":           pl_d.get("plUSD", ""),
        # odds
        "Odds_Style":             _ODDS_MAP.get(purchase.get("oddStyleID", 1), "Decimal"),
        "Client_Odds":            bet.get("clientOdds", ""),
        "Decimal_Odds":           bet.get("enhancedOdds", ""),
        "Bet_Status":             _STATUS_MAP.get(bet.get("betStatusID", 0), ""),
        "Resettled":              "Yes" if bet.get("isResettled") else "No",
        "Settlement_Date":        _utc_to_local(settlement.get("dateSettled", "")),
        "File_Generated_Time":    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
    }
    return row


def json_to_excel_bethistory(data: List[Dict]) -> bytes:
    """convierte lista de purchases de bethistory a bytes xlsx"""
    rows = [_map_bethistory_row(p) for p in data]
    return _write_xlsx(BETHISTORY_HEADERS, rows)


# ──────────────────────────────────────────────────────────────────────────────
# openbets
# ──────────────────────────────────────────────────────────────────────────────

OPENBETS_HEADERS = [
    "Purchase_ID", "Bets_BetID", "Rate", "Currency", "Odds_Style",
    "Site_ID", "Client_Type", "BettingView", "Platform",
    "Bet_Date_And_Time", "Client_Odds", "Bet_Type",
    "Resettled", "Bet_Status",
    "Total_Win_Loss", "Brand_Name", "Live_Prematch",
    "Number_Of_Lines", "Number_Of_Bets", "Combo_Size",
    "To_Return", "Stake",
    "Stake_ARS", "Stake_BRL", "Stake_CNY", "Stake_COP",
    "Stake_EUR", "Stake_GBP", "Stake_KRW", "Stake_MYR", "Stake_THB", "Stake_USD",
    "Possible_Winning",
    "Possible_Winning_ARS", "Possible_Winning_BRL", "Possible_Winning_CNY",
    "Possible_Winning_COP", "Possible_Winning_EUR", "Possible_Winning_GBP",
    "Possible_Winning_KRW", "Possible_Winning_MYR", "Possible_Winning_THB", "Possible_Winning_USD",
    "Decimal_Odds",
    "Free_Bet_ID", "Free_Bet_Amount",
    "Free_Bet_Amount_ARS", "Free_Bet_Amount_BRL", "Free_Bet_Amount_CNY",
    "Free_Bet_Amount_COP", "Free_Bet_Amount_EUR", "Free_Bet_Amount_GBP",
    "Free_Bet_Amount_KRW", "Free_Bet_Amount_MYR", "Free_Bet_Amount_THB", "Free_Bet_Amount_USD",
    "Is_Risk_Free_Bet",
    "Customer_ID", "User_Name", "Merchant_Customer_Code",
    "Enhanced_Odds", "ComboBonusID",
    "Sports", "League", "Event", "Event_Date", "Market", "Selection",
    "BookingCode",
    "File_Generated_Time",
]

_PLATFORM_MAP = {1: "web", 2: "mobile", 8: "app", 9: "web"}
_BETTING_VIEW_MAP = {8: "South American View", 9: "South American View"}


def _map_openbets_row(purchase: Dict) -> Dict:
    bets = purchase.get("bets", [])
    bet = bets[0] if bets else {}
    selections = bet.get("selections", [])

    customer = purchase.get("customer", {})
    stake_d = bet.get("stakeDecimal", {})
    fba = bet.get("freeBetAmount", {})

    # enhanced winning part para possible winnings del openbets viene de possibleWinnings a nivel purchase
    row = {
        "Purchase_ID":                str(purchase.get("purchaseID", "")),
        "Bets_BetID":                 str(bet.get("betID", "")),
        "Rate":                       purchase.get("rateUSD", ""),
        "Currency":                   purchase.get("currencyCode", ""),
        "Odds_Style":                 _ODDS_MAP.get(purchase.get("oddStyleID", 1), "Decimal"),
        "Site_ID":                    purchase.get("siteID", ""),
        "Client_Type":                "",  # no disponible en json
        "BettingView":                _BETTING_VIEW_MAP.get(bet.get("bettingView"), ""),
        "Platform":                   _PLATFORM_MAP.get(bet.get("platform"), ""),
        "Bet_Date_And_Time":          _utc_to_local(purchase.get("creationDate", "")),
        "Client_Odds":                bet.get("clientOdds", ""),
        "Bet_Type":                   _bet_type_name(bet.get("betTypeID", 1)),
        "Resettled":                  "Yes" if bet.get("isResettled") else "No",
        "Bet_Status":                 _STATUS_MAP.get(bet.get("betStatusID", 0), "Opened"),
        "Total_Win_Loss":             purchase.get("totalWinLoss", ""),
        "Brand_Name":                 customer.get("agentName", ""),
        "Live_Prematch":              _live_prematch(bet.get("isLive", False)),
        "Number_Of_Lines":            bet.get("betsNumberOfLines", ""),
        "Number_Of_Bets":             bet.get("numberOfBets", ""),
        "Combo_Size":                 bet.get("betsComboSize", ""),
        "To_Return":                  purchase.get("totalReturn", ""),
        "Stake":                      stake_d.get("stakeDecimal", ""),
        "Stake_ARS":                  stake_d.get("stakeDecimalARS", ""),
        "Stake_BRL":                  stake_d.get("stakeDecimalBRL", ""),
        "Stake_CNY":                  stake_d.get("stakeDecimalCNY", ""),
        "Stake_COP":                  stake_d.get("stakeDecimalCOP", ""),
        "Stake_EUR":                  stake_d.get("stakeDecimalEUR", ""),
        "Stake_GBP":                  stake_d.get("stakeDecimalGBP", ""),
        "Stake_KRW":                  stake_d.get("stakeDecimalKRW", ""),
        "Stake_MYR":                  stake_d.get("stakeDecimalMYR", ""),
        "Stake_THB":                  stake_d.get("stakeDecimalTHB", ""),
        "Stake_USD":                  stake_d.get("stakeDecimalUSD", ""),
        "Possible_Winning":           purchase.get("possibleWinnings", ""),
        "Possible_Winning_ARS":       "",
        "Possible_Winning_BRL":       "",
        "Possible_Winning_CNY":       "",
        "Possible_Winning_COP":       "",
        "Possible_Winning_EUR":       "",
        "Possible_Winning_GBP":       "",
        "Possible_Winning_KRW":       "",
        "Possible_Winning_MYR":       "",
        "Possible_Winning_THB":       "",
        "Possible_Winning_USD":       "",
        "Decimal_Odds":               bet.get("enhancedOdds", ""),
        "Free_Bet_ID":                bet.get("freeBetID", ""),
        "Free_Bet_Amount":            fba.get("freeBetAmount", ""),
        "Free_Bet_Amount_ARS":        fba.get("freeBetAmountARS", ""),
        "Free_Bet_Amount_BRL":        fba.get("freeBetAmountBRL", ""),
        "Free_Bet_Amount_CNY":        fba.get("freeBetAmountCNY", ""),
        "Free_Bet_Amount_COP":        fba.get("freeBetAmountCOP", ""),
        "Free_Bet_Amount_EUR":        fba.get("freeBetAmountEUR", ""),
        "Free_Bet_Amount_GBP":        fba.get("freeBetAmountGBP", ""),
        "Free_Bet_Amount_KRW":        fba.get("freeBetAmountKRW", ""),
        "Free_Bet_Amount_MYR":        fba.get("freeBetAmountMYR", ""),
        "Free_Bet_Amount_THB":        fba.get("freeBetAmountTHB", ""),
        "Free_Bet_Amount_USD":        fba.get("freeBetAmountUSD", ""),
        "Is_Risk_Free_Bet":           "Yes" if bet.get("isRiskFreeBet") else "No",
        "Customer_ID":                customer.get("customerID", ""),
        "User_Name":                  customer.get("loginName", ""),
        "Merchant_Customer_Code":     customer.get("merchantCustomerCode", ""),
        "Enhanced_Odds":              bet.get("enhancedOdds", ""),
        "ComboBonusID":               bet.get("comboBonusID", ""),
        "Sports":                     _join([s.get("branchName", "") for s in selections]),
        "League":                     _join([s.get("leagueName", "") for s in selections]),
        "Event":                      _join([s.get("eventName", "") for s in selections]),
        "Event_Date":                 _join([_utc_to_local(s.get("eventDate", "")) for s in selections]),
        "Market":                     _join([s.get("eventTypeName", "") for s in selections]),
        "Selection":                  _join([s.get("yourBet", "") for s in selections]),
        "BookingCode":                bet.get("betSlipCode", ""),
        "File_Generated_Time":        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
    }
    return row


def json_to_excel_openbets(data: List[Dict]) -> bytes:
    """convierte lista de purchases de openbets a bytes xlsx"""
    rows = [_map_openbets_row(p) for p in data]
    return _write_xlsx(OPENBETS_HEADERS, rows)


# ──────────────────────────────────────────────────────────────────────────────
# declinedbets
# ──────────────────────────────────────────────────────────────────────────────

DECLINEDBETS_HEADERS = [
    "Purchase_ID", "Declined_Bets_ID", "Bet_Type", "Bet_Date_And_Time",
    "Customer_ID", "Username", "Merchant_Customer_Code",
    "Sports_Level", "Live_Prematch",
    "Sports", "League", "Event", "EventDate", "Market", "Selection",
    "Operator", "Brand",
    "Stake", "Odds_Style", "Client_Odds",
    "Declined_Type", "Declined_Details",
    "File_Generated_Time",
]

_DECLINE_TYPE_MAP = {
    "StakeLimitReached":          "Reached stake limit",
    "OddsChanged":                "Odds have changed",
    "PointsChanged":              "Points have changed",
    "EventSuspended":             "Event is suspended",
    "MarketSuspended":            "Market is suspended",
    "RejectAutoNewOffer":         "Customer declined the auto new offer",
    "IgnoredAutoNewOffer":        "Customer ignored the auto new offer",
}


def _map_declinedbets_row(purchase: Dict) -> Dict:
    bets = purchase.get("bets", [])
    bet = bets[0] if bets else {}
    selections = bet.get("selections", [])

    customer = purchase.get("customer", {})
    stake_d = bet.get("stakeDecimal", {})

    row = {
        "Purchase_ID":            str(purchase.get("purchaseID", "")),
        "Declined_Bets_ID":       str(bet.get("betID", "")),
        "Bet_Type":               bet.get("betTypeID", 1),
        "Bet_Date_And_Time":      _utc_to_local(purchase.get("creationDate", "")),
        "Customer_ID":            customer.get("customerID", ""),
        "Username":               customer.get("loginName", ""),
        "Merchant_Customer_Code": customer.get("merchantCustomerCode", ""),
        "Sports_Level":           _sports_level(selections),
        "Live_Prematch":          _live_prematch(bet.get("isLive", False)),
        "Sports":                 _join([s.get("branchName", "") for s in selections]),
        "League":                 _join([s.get("leagueName", "") for s in selections]),
        "Event":                  _join([s.get("eventName", "") for s in selections]),
        "EventDate":              _join([_utc_to_local(s.get("eventDate", "")) for s in selections]),
        "Market":                 _join([s.get("eventTypeName", "") for s in selections]),
        "Selection":              _join([s.get("yourBet", "") for s in selections]),
        "Operator":               customer.get("agentID", ""),
        "Brand":                  customer.get("agentName", ""),
        "Stake":                  stake_d.get("stakeDecimal", ""),
        "Odds_Style":             _ODDS_MAP.get(purchase.get("oddStyleID", 1), "Decimal"),
        "Client_Odds":            bet.get("clientOdds", ""),
        "Declined_Type":          purchase.get("declineDetails", ""),
        "Declined_Details":       _DECLINE_TYPE_MAP.get(
                                      purchase.get("declineTypeName", ""),
                                      purchase.get("declineTypeName", "")
                                  ),
        "File_Generated_Time":    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
    }
    return row


def json_to_excel_declinedbets(data: List[Dict]) -> bytes:
    """convierte lista de purchases de declinedbets a bytes xlsx"""
    rows = [_map_declinedbets_row(p) for p in data]
    return _write_xlsx(DECLINEDBETS_HEADERS, rows)


# ──────────────────────────────────────────────────────────────────────────────
# dispatch
# ──────────────────────────────────────────────────────────────────────────────

_CONVERTERS = {
    "bethistory":   json_to_excel_bethistory,
    "openbets":     json_to_excel_openbets,
    "declinedbets": json_to_excel_declinedbets,
}


def convert_first_report(report_name: str, data: List[Dict]) -> bytes:
    """
    convierte datos json de first a bytes xlsx segun el tipo de reporte.
    report_name: 'bethistory' | 'openbets' | 'declinedbets'
    """
    fn = _CONVERTERS.get(report_name)
    if not fn:
        raise ValueError(f"reporte desconocido: {report_name}")
    return fn(data)
