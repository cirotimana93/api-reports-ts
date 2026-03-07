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
