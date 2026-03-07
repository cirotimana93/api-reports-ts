from datetime import datetime, timedelta, timezone
from fastapi_utilities import repeat_at
from app.logic.orchestrator import execute_full_reconciliation
import asyncio

# definir zona horaria de lima (utc-5)
LIMA_TZ = timezone(timedelta(hours=-5))

@repeat_at(cron="0 */3 * * *")
async def scheduled_reconciliation():
    # usamos la fecha de hoy en lima para el proceso automatico
    today = datetime.now(LIMA_TZ).strftime("%Y-%m-%d")
    
    print(f"\n[CRON] [{datetime.now(LIMA_TZ).strftime('%Y-%m-%d %H:%M:%S')}] Iniciando proceso completo...")
    
    try:
        # ejecutamos el flujo completo (step 1 y step 2)
        await execute_full_reconciliation(start_date=today)
        print(f"[CRON] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Proceso programado finalizado con exito.")
    except Exception as e:
        print(f"[CRON] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR en proceso programado: {e}")

@repeat_at(cron="15 3 * * *")
async def daily_full_month_reconciliation():
    # usamos la hora de lima
    now = datetime.now(LIMA_TZ)
    
    if now.day == 1:
        # es el primer dia del mes, ejecutar todo el mes anterior
        last_month = now.replace(day=1) - timedelta(days=1)
        start_date = last_month.replace(day=1).strftime("%Y-%m-%d")
        end_date = last_month.strftime("%Y-%m-%d")
    else:
        # procesar desde el dia 1 del mes actual hasta ayer
        yesterday = now - timedelta(days=1)
        start_date = now.replace(day=1).strftime("%Y-%m-%d")
        end_date = yesterday.strftime("%Y-%m-%d")

    print(f"\n[CRON DIARIO] [{now.strftime('%Y-%m-%d %H:%M:%S')}] Iniciando conciliacion acumulada: {start_date} al {end_date}")
    
    try:
        await execute_full_reconciliation(start_date=start_date, end_date=end_date)
        print(f"[CRON DIARIO] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Finalizado con exito.")
    except Exception as e:
        print(f"[CRON DIARIO] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: {e}")

async def run_events():
    # al llamar a la funcion decorada con repeat_at, se activa el bucle de programacion
    asyncio.create_task(scheduled_reconciliation())
    asyncio.create_task(daily_full_month_reconciliation())
    print("[EVENTS] Control de eventos programados activado (Cada 3h y Diarios a las 03:15).")
