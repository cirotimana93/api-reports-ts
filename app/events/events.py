from datetime import datetime, timedelta, timezone
from fastapi_utilities import repeat_at
from app.logic.orchestrator import execute_full_reconciliation
import asyncio

# definir zona horaria de lima (utc-5)
LIMA_TZ = timezone(timedelta(hours=-5))

async def scheduled_reconciliation():
    # usamos la fecha de hoy en lima para el proceso automatico
    today = datetime.now(LIMA_TZ).strftime("%Y-%m-%d")
    
    print(f"\n[CRON PROGRAMADO] [{datetime.now(LIMA_TZ).strftime('%Y-%m-%d %H:%M:%S')}] Iniciando proceso completo...")
    
    try:
        # ejecutamos el flujo completo (step 1 y step 2)
        await execute_full_reconciliation(start_date=today)
        print(f"[CRON PROGRAMADO] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Proceso programado finalizado con exito.")
    except Exception as e:
        print(f"[CRON PROGRAMADO] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR en proceso programado: {e}")

# se dividen las ejecuciones en dos programaciones para cubrir 09:00, 15:00, 18:00 y 23:50 sin minutos adicionales
@repeat_at(cron="0 9,15,18 * * *")
async def scheduled_reconciliation_day():
    await scheduled_reconciliation()

@repeat_at(cron="50 23 * * *")
async def scheduled_reconciliation_night():
    await scheduled_reconciliation()

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

    print(f"\n[CRON POR MES] [{now.strftime('%Y-%m-%d %H:%M:%S')}] Iniciando conciliacion acumulada: {start_date} al {end_date}")
    
    try:
        await execute_full_reconciliation(start_date=start_date, end_date=end_date)
        print(f"[CRON POR MES] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Finalizado con exito.")
    except Exception as e:
        print(f"[CRON POR MES] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: {e}")

@repeat_at(cron="0 4 * * *")
async def previous_day_reconciliation():
    # ejecutamos del dia anterior
    now = datetime.now(LIMA_TZ)
    start_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\n[CRON DIA ANTERIOR] [{now.strftime('%Y-%m-%d %H:%M:%S')}] Iniciando conciliacion acumulada: {start_date} al {end_date}")
    
    try:
        await execute_full_reconciliation(start_date=start_date, end_date=end_date)
        print(f"[CRON DIA ANTERIOR] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Finalizado con exito.")
    except Exception as e:
        print(f"[CRON DIA ANTERIOR] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: {e}")

async def run_events():
    # al llamar a la funcion decorada con repeat_at, se activa el bucle de programacion
    asyncio.create_task(scheduled_reconciliation_day())
    asyncio.create_task(scheduled_reconciliation_night())
    ##asyncio.create_task(daily_full_month_reconciliation())
    # asyncio.create_task(previous_day_reconciliation())
    print("[EVENTS] Control de eventos programados activado (Diarios a las 09:00, 15:00, 18:00 y 23:50).")

