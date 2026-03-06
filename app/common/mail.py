import base64
import httpx
from typing import List, Optional
from app.core.config import settings

async def sendMailOffice365(subject: str, content: str, to_recipients: List[str], attachment_content: Optional[bytes] = None, attachment_name: Optional[str] = None):
    # obtener token de acceso de ms graph
    token_url = f"https://login.microsoftonline.com/{settings.GRAPH_TENANT_ID}/oauth2/v2.0/token"
    token_data = {
        "client_id": settings.GRAPH_CLIENT_ID,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": settings.GRAPH_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    
    async with httpx.AsyncClient() as client:
        # solicitar token
        token_res = await client.post(token_url, data=token_data)
        if token_res.status_code != 200:
            print(f"[mail] error obteniendo token: {token_res.text}")
            return False
        
        access_token = token_res.json().get("access_token")
        
        # construir mensaje
        recipients = [{"emailAddress": {"address": email.strip()}} for email in to_recipients]
        
        email_body = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": content
                },
                "toRecipients": recipients,
            }
        }
        
        # agregar adjunto si existe
        if attachment_content and attachment_name:
            encoded_content = base64.b64encode(attachment_content).decode("utf-8")
            email_body["message"]["attachments"] = [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": attachment_name,
                    "contentBytes": encoded_content
                }
            ]
            
        send_url = f"https://graph.microsoft.com/v1.0/users/{settings.GRAPH_EMAIL_FROM}/sendMail"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # enviar correo
        send_res = await client.post(send_url, json=email_body, headers=headers)
        if send_res.status_code not in [200, 202]:
            print(f"[mail] error enviando correo: {send_res.text}")
            return False
            
        print(f"[mail] correo enviado exitosamente a {to_recipients}")
        return True
