import boto3
from botocore.exceptions import ClientError
from botocore.config import Config as BotoConfig
import os
import time as _time
from datetime import datetime
from typing import List, Optional
from app.core.config import settings

# cache de cliente s3
_S3_CLIENT_CACHE = None
_S3_CREDS_EXPIRATION = 0

def get_s3_client_with_role():
    global _S3_CLIENT_CACHE, _S3_CREDS_EXPIRATION
    
    current_time = _time.time()
    
    # reutilizar cliente si faltan mas de 5 minutos para que expire
    if _S3_CLIENT_CACHE and current_time < (_S3_CREDS_EXPIRATION - 300):
        return _S3_CLIENT_CACHE

    try:
        print("[info] asumiendo rol de aws para s3...")
        sts = boto3.client(
            "sts",
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY,
            aws_secret_access_key=settings.AWS_SECRET_KEY
        )
        
        # solicitar duracion (por defecto usamos 43200 o el maximo permitido)
        assumed = sts.assume_role(
            RoleArn=settings.AWS_ROLE_ARN,
            RoleSessionName="reconciliation-session",
            DurationSeconds=43200
        )
        
        creds = assumed['Credentials']
        _S3_CREDS_EXPIRATION = creds['Expiration'].timestamp()
        
        # configurar timeouts y reintentos (mode: standard corrige el read timeout de conexiones caidas)
        s3_config = BotoConfig(
            region_name=settings.AWS_REGION,
            connect_timeout=60,
            read_timeout=120,
            retries={'max_attempts': 5, 'mode': 'standard'}
        )
        
        _S3_CLIENT_CACHE = boto3.client(
            "s3",
            config=s3_config,
            aws_access_key_id=creds['AccessKeyId'],
            aws_secret_access_key=creds['SecretAccessKey'],
            aws_session_token=creds['SessionToken']
        )
        
        print(f"[ok] cliente s3 cacheado hasta {creds['Expiration']}")
        return _S3_CLIENT_CACHE
        
    except ClientError as e:
        print(f"[error] error al asumir el rol de s3: {e}")
        return None
    except Exception as e:
        print(f"[error] error al crear el cliente s3: {e}")
        return None


def upload_file_to_s3(content: bytes, s3_key: str):
    try:
        s3_client = get_s3_client_with_role()
        s3_client.put_object(Body=content, Bucket=settings.AWS_BUCKET_NAME, Key=s3_key)
        print(f"[ok] subido a s3: s3://{settings.AWS_BUCKET_NAME}/{s3_key}")
    except ClientError as e:
        print(f"[warn] error subiendo {s3_key} a s3: {e}")


def copy_file_in_s3(src_key: str, dest_key: str):
    try:
        s3_client = get_s3_client_with_role()
        s3_client.copy_object(
            Bucket=settings.AWS_BUCKET_NAME,
            CopySource={'Bucket': settings.AWS_BUCKET_NAME, 'Key': src_key},
            Key=dest_key
        )
        print(f"[ok] copiado en s3: {src_key} -> {dest_key}")
        return True
    except ClientError as e:
        print(f"[warn] error al copiar archivo en s3: {e}")
        return False


def read_file_from_s3(s3_key: str) -> bytes:
    try:
        s3_client = get_s3_client_with_role()
        s3 = s3_client
        response = s3.get_object(Bucket=settings.AWS_BUCKET_NAME, Key=str(s3_key))
        return response['Body'].read()
    except ClientError as e:
        print(f"[warn] error al leer archivo s3: {e}")
        return b""


def delete_file_from_s3(s3_key: str):
    try:
        s3_client = get_s3_client_with_role()
        s3 = s3_client
        s3.delete_object(Bucket=settings.AWS_BUCKET_NAME, Key=str(s3_key))
        print(f"[ok] eliminado de s3: {s3_key}")
    except ClientError as e:
        print(f"[warn] error al eliminar archivo de s3: {e}")


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
        print(f"[warn] error al listar archivos: {e}")
        return []


def get_latest_file_from_s3(prefix: str) -> str:
    try:
        files = list_files_in_s3(prefix)
        if not files:
            return None
        # ordena por fecha (asumiendo que los nombres contienen fechas)
        files.sort(reverse=True)
        return files[0]
    except Exception as e:
        print(f"[warn] error al obtener el archivo mas reciente de s3: {e}")
        return None
    
    
def get_attachment_from_s3(s3_key):
    # obtiene el contenido binario y el nombre del archivo
    content = read_file_from_s3(s3_key)
    filename = os.path.basename(s3_key)
    return (filename, content)


def download_file_from_s3_to_local(s3_key: str, local_dir: str = "debug_output") -> str:
    try:
        # crear carpeta local si no existe
        os.makedirs(local_dir, exist_ok=True)

        # obtener contenido
        content = read_file_from_s3(s3_key)
        if not content:
            print(f"[warn] no se pudo descargar {s3_key} desde s3.")
            return None

        # nombre local
        local_path = os.path.join(local_dir, os.path.basename(s3_key))

        # guardar localmente
        with open(local_path, "wb") as f:
            f.write(content)

        print(f"[ok] archivo guardado en local: {os.path.abspath(local_path)}")
        return local_path

    except Exception as e:
        print(f"[warn] error al descargar y guardar archivo de s3: {e}")
        return None
    
def get_s3_file_size(s3_key : str):
    s3_client = get_s3_client_with_role()
    s3 = s3_client
    response = s3.head_object(Bucket=settings.AWS_BUCKET_NAME, Key=str(s3_key))
    size_bytes = response['ContentLength']
    size_mb = size_bytes / (1024 * 1024)
    print(f"[info] tamaño de {s3_key}: {size_bytes} bytes ({size_mb:.2f} mb)")
    return size_mb


def generate_s3_download_link(s3_key: str, expiration_hours: int = 12) -> str:
    try:
        s3_client = get_s3_client_with_role()
        if not s3_client:
            print(f"[warn] no se pudo obtener cliente s3 para generar enlace de {s3_key}")
            return None
            
        expiration_seconds = min(expiration_hours * 3600, 43200)
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': settings.AWS_BUCKET_NAME, 'Key': s3_key},
            ExpiresIn=expiration_seconds
        )
        
        print(f"[ok] enlace de descarga generado para {s3_key} (valido por {expiration_hours} horas)")
        return presigned_url
        
    except ClientError as e:
        print(f"[warn] error al generar enlace de descarga para {s3_key}: {e}")
        return None
    except Exception as e:
        print(f"[warn] error inesperado al generar enlace: {e}")
        return None

