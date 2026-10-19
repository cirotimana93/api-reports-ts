from typing import Dict, List

def format_reconciliation_email(
    start_time: str, 
    end_time: str, 
    summary_data: List[Dict[str, any]]
) -> str:
       
    # estilos css para la tabla
    styles = """
    <style>
        .conciliacion-table {
            border-collapse: collapse;
            width: 100%;
            font-family: Arial, sans-serif;
            margin-top: 20px;
        }
        .conciliacion-table th {
            background-color: #004a99;
            color: white;
            text-align: left;
            padding: 12px;
            border: 1px solid #ddd;
        }
        .conciliacion-table td {
            padding: 10px;
            border: 1px solid #ddd;
        }
        .conciliacion-table tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        .time-info {
            font-weight: bold;
            color: #333;
            margin-bottom: 5px;
        }
    </style>
    """
    
    html = f"""
    <html>
    <head>{styles}</head>
    <body>
        <div class="time-info">Fecha de inicio de proceso: {start_time}</div>
        <div class="time-info">Fecha de fin de proceso: {end_time}</div>
        
        <table class="conciliacion-table">
            <thead>
                <tr>
                    <th>Proveedor</th>
                    <th>Total operaciones en proveedor</th>
                    <th>Total operaciones del proveedor en MVT</th>
                    <th>Tx del proveedor presentes en MVT</th>
                    <th>Tx del proveedor no presentes en MVT</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for row in summary_data:
        html += f"""
                <tr>
                    <td>{row['proveedor']}</td>
                    <td>{row['total_prov']}</td>
                    <td>{row['total_mvt']}</td>
                    <td>{row['presentes']}</td>
                    <td>{row['no_presentes']}</td>
                </tr>
        """
        
    html += """
            </tbody>
        </table>
        <p style="margin-top: 20px;">Se adjunta el reporte detallado en Excel.</p>
    </body>
    </html>
    """
    
    return html


def format_error_email(failed_reports: Dict[str, str]) -> str:
    # devCt: plantilla html directa para notificacion de fallos sin librerias externas
    styles = """
    <style>
        body { font-family: Arial, sans-serif; color: #333; line-height: 1.5; }
        .alert-box { background-color: #f8d7da; border: 1px solid #f5c6cb; color: #721c24; padding: 15px; border-radius: 4px; margin-bottom: 20px; }
        .error-table { border-collapse: collapse; width: 100%; margin-top: 15px; font-family: Arial, sans-serif; }
        .error-table th { background-color: #c82333; color: white; text-align: left; padding: 10px; border: 1px solid #ddd; }
        .error-table td { padding: 10px; border: 1px solid #ddd; }
        .error-table tr:nth-child(even) { background-color: #fdf7f7; }
        .contact-box { margin-top: 20px; padding: 15px; background-color: #e2e3e5; border-left: 5px solid #6c757d; font-weight: bold; color: #383d41; }
    </style>
    """
    
    html = f"""
    <html>
    <head>{styles}</head>
    <body>
        <div class="alert-box">
            <h3 style="margin: 0 0 10px 0;">Alerta: Error en proceso de conciliacion</h3>
            <p style="margin: 0;">Se presentaron errores en la descarga y/o procesamiento de reportes tras 3 intentos de ejecucion. La conciliacion no pudo completarse.</p>
        </div>
        
        <table class="error-table">
            <thead>
                <tr>
                    <th style="width: 30%;">Reporte / Proveedor</th>
                    <th>Detalle del error</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for reportName, errorDetail in failed_reports.items():
        html += f"""
                <tr>
                    <td><strong>{reportName}</strong></td>
                    <td>{errorDetail}</td>
                </tr>
        """
        
    html += """
            </tbody>
        </table>
        
        <div class="contact-box">
            Por favor, comunicarse con el area de Optimizacion Operativa para la revision correspondiente.
        </div>
    </body>
    </html>
    """
    return html

# devCt: alias camelcase segun reglas de desarrollo
formatErrorEmail = format_error_email

