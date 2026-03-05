import boto3
from botocore.exceptions import ClientError
import os
from typing import List
from app.core.config import settings

def get_s3_client_with_role():
    try:
        sts = boto3.client(
            "sts",
            aws_access_key_id=settings.AWS_ACCESS_KEY,
            aws_secret_access_key=settings.AWS_SECRET_KEY
        )
        assumed = sts.assume_role(
            RoleArn=settings.AWS_ROLE_ARN,
            RoleSessionName="user-session",
            DurationSeconds=43200
        )
        creds = assumed['Credentials']
        return boto3.client(
            "s3",
            region_name=settings.AWS_REGION,
            aws_access_key_id=creds['AccessKeyId'],
            aws_secret_access_key=creds['SecretAccessKey'],
            aws_session_token=creds['SessionToken']
        )
    except ClientError as e:
        print("[ALERTA] error al asumir el rol:", e)
        return None
    

def upload_file_to_s3(content: bytes, s3_key: str):
    try:
        s3_client = get_s3_client_with_role()
        s3_client.put_object(Body=content, Bucket=settings.AWS_BUCKET_NAME, Key=s3_key)
        print(f"[INFO] Subido a S3: s3://{settings.AWS_BUCKET_NAME}/{s3_key}")
    except ClientError as e:
        print(f"[ALERTA] error subiendo {s3_key} a S3: {e}")


def read_file_from_s3(s3_key: str) -> bytes:
    try:
        s3_client = get_s3_client_with_role()
        s3 = s3_client
        response = s3.get_object(Bucket=settings.AWS_BUCKET_NAME, Key=str(s3_key))
        return response['Body'].read()
    except ClientError as e:
        print(f"[ALERTA] error al leer archivo S3: {e}")
        return b""


def delete_file_from_s3(s3_key: str):
    try:
        s3_client = get_s3_client_with_role()
        s3 = s3_client
        s3.delete_object(Bucket=settings.AWS_BUCKET_NAME, Key=str(s3_key))
        print(f"[INFO] eliminado de S3: {s3_key}")
    except ClientError as e:
        print(f"[ALERTA] error al eliminar archivo de S3: {e}")


def copy_file_in_s3(source_key: str, dest_key: str):
    """copia un objeto dentro del mismo bucket (usado para mover a processed/)"""
    try:
        s3_client = get_s3_client_with_role()
        s3_client.copy_object(
            Bucket=settings.AWS_BUCKET_NAME,
            CopySource={"Bucket": settings.AWS_BUCKET_NAME, "Key": source_key},
            Key=dest_key
        )
        print(f"[INFO] copiado en S3: {source_key} -> {dest_key}")
    except ClientError as e:
        print(f"[ALERTA] error al copiar en S3: {e}")


def list_files_in_s3(prefix: str) -> List[str]:
    try:
        s3_client = get_s3_client_with_role()
        s3 = s3_client
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=settings.AWS_BUCKET_NAME, Prefix=prefix)
        files = []
        for page in pages:
            for obj in page.get('Contents', []):
                files.append(obj['Key'])
        return files
    except ClientError as e:
        print(f"[ALERTA] error al listar archivos: {e}")
        return []


def get_latest_file_from_s3(prefix: str) -> str:
    try:
        files = list_files_in_s3(prefix)
        if not files:
            return None
        # Ordena por fecha (asumiendo que los nombres contienen fechas)
        files.sort(reverse=True)
        return files[0]
    except Exception as e:
        print(f"[ALERTA] error al obtener el archivo mas reciente de S3: {e}")
        return None
    
    
def get_attachment_from_s3(s3_key):
    # obtiene el contenido binario y el nombre del archivo
    content = read_file_from_s3(s3_key)
    filename = os.path.basename(s3_key)
    return (filename, content)


def download_file_from_s3_to_local(s3_key: str, local_dir: str = "debug_output") -> str:
    try:
        # Crear carpeta local si no existe
        os.makedirs(local_dir, exist_ok=True)

        # Obtener contenido
        content = read_file_from_s3(s3_key)
        if not content:
            print(f"[ALERTA] No se pudo descargar {s3_key} desde S3.")
            return None

        # Nombre local
        local_path = os.path.join(local_dir, os.path.basename(s3_key))

        # Guardar localmente
        with open(local_path, "wb") as f:
            f.write(content)

        print(f"[INFO] Archivo guardado en local: {os.path.abspath(local_path)}")
        return local_path

    except Exception as e:
        print(f"[ALERTA] Error al descargar y guardar archivo de S3: {e}")
        return None
    
def get_s3_file_size(s3_key : str):
    s3_client = get_s3_client_with_role()
    s3 = s3_client
    response = s3.head_object(Bucket=settings.AWS_BUCKET_NAME, Key=str(s3_key))
    size_bytes = response['ContentLength']
    size_mb = size_bytes / (1024 * 1024)
    print(f"[INFO] Tamaño de {s3_key}: {size_bytes} bytes ({size_mb:.2f} MB)")
    return size_mb


def generate_s3_download_link(s3_key: str, expiration_hours: int = 12) -> str:
    try:
        s3_client = get_s3_client_with_role()
        if not s3_client:
            print(f"[ALERTA] No se pudo obtener cliente S3 para generar enlace de {s3_key}")
            return None
            
        expiration_seconds = min(expiration_hours * 3600, 43200)
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': settings.AWS_BUCKET_NAME, 'Key': s3_key},
            ExpiresIn=expiration_seconds
        )
        
        print(f"[INFO] Enlace de descarga generado para {s3_key} (valido por {expiration_hours} horas)")
        return presigned_url
        
    except ClientError as e:
        print(f"[ALERTA] Error al generar enlace de descarga para {s3_key}: {e}")
        return None
    except Exception as e:
        print(f"[ALERTA] Error inesperado al generar enlace: {e}")
        return None

