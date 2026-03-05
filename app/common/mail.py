import base64
import requests
from app.core.config import settings
from msal import ConfidentialClientApplication



def sendMailOffice365(remitente, asunto, mensajeBody, destinatarios, archivos_adjuntos=None):
    try:
        # configuracion
        client_id = settings.GRAPH_CLIENT_ID
        client_secret = settings.GRAPH_CLIENT_SECRET
        tenant_id = settings.GRAPH_TENANT_ID
        sender_email = remitente
        
        if not all([client_id, client_secret, tenant_id, sender_email]):
            raise Exception("Configuración de Graph API incompleta para envío de correos")
        
        # autenticacion
        app = ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=f"https://login.microsoftonline.com/{tenant_id}"
        )
        
        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        
        if "access_token" not in result:
            raise Exception(f"Error obteniendo token: {result.get('error_description')}")
        
        token = result["access_token"]
        
        # preparar destinatarios
        to_recipients = [{"emailAddress": {"address": email.strip()}} for email in destinatarios]
        
        # preparar adjuntos
        attachments = []
        if archivos_adjuntos:
            for archivo in archivos_adjuntos:
                if isinstance(archivo, tuple):
                    # (filename, content)
                    filename, content = archivo
                    if isinstance(content, str):
                        content = content.encode('utf-8')
                    attachment_data = {
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": filename,
                        "contentBytes": base64.b64encode(content).decode('utf-8')
                    }
                    attachments.append(attachment_data)
                else:
                    # Ruta de archivo
                    with open(archivo, 'rb') as f:
                        content = f.read()
                    attachment_data = {
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": os.path.basename(archivo),
                        "contentBytes": base64.b64encode(content).decode('utf-8')
                    }
                    attachments.append(attachment_data)
        
        # preparar mensaje
        message = {
            "subject": asunto,
            "body": {
                "contentType": "HTML",
                "content": mensajeBody
            },
            "toRecipients": to_recipients
        }
        
        if attachments:
            message["attachments"] = attachments
        
        # enviar correo
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
        payload = {"message": message}
        
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code == 202:
            print(f"Correo enviado exitosamente a: {', '.join(destinatarios)}")
            return True
        else:
            raise Exception(f"Error enviando correo: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"Error en sendMailOffice365: {str(e)}")
        return False
