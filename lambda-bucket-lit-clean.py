import boto3
import os
from datetime import datetime, timedelta

s3 = boto3.client('s3')

BUCKET_NAME = 'bucket-receive-ajr'
PREFIX = 'inbound/'

def lambda_handler(event, context):
    continuation_token = None
    
    # Data atual
    now = datetime.now()
    
    # Data do primeiro dia do mês atual
    first_day_of_current_month = datetime(now.year, now.month, 1)
    
    # Data do primeiro dia do mês anterior
    first_day_of_last_month = (first_day_of_current_month - timedelta(days=1)).replace(day=1)

    # Data do primeiro dia do mês seguinte
    first_day_of_next_month = (first_day_of_current_month + timedelta(days=31)).replace(day=1)
    
    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
        
        if 'Contents' not in response:
            print('No files found.')
            return
        
        for obj in response['Contents']:
            key = obj['Key']
            
            # Extrai a data do nome do arquivo
            try:
                # Supondo que o formato seja 'LIT_HHMMSS_DDMMYYYY.TXT'
                time_date_str = key.split('_')[1]  # '093501'
                date_str = key.split('_')[2].split('.')[0]  # '21082024'
                
                # Concatena as partes para criar uma string completa
                datetime_str = f'{time_date_str}_{date_str}'
                
                # Converte a string em um objeto datetime
                file_date = datetime.strptime(datetime_str, '%H%M%S_%d%m%Y')
                
                # Verifica se o arquivo pertence ao mês atual ou ao mês anterior
                if first_day_of_last_month <= file_date < first_day_of_current_month or file_date >= first_day_of_current_month:
                    print(f'Ignoring file {key} from last or current month.')
                    continue
                
                # Prefixo de destino para o histórico
                dest_prefix = f'historico/lit/{file_date.year}/{file_date.month:02}/'
                
                # Novo caminho do arquivo
                new_key = dest_prefix + os.path.basename(key)
                
                # Copia o arquivo para o novo destino
                copy_source = {'Bucket': BUCKET_NAME, 'Key': key}
                s3.copy_object(CopySource=copy_source, Bucket=BUCKET_NAME, Key=new_key)
                
                # Remove o arquivo original
                s3.delete_object(Bucket=BUCKET_NAME, Key=key)
                
                print(f'Moved: {key} to {new_key}')
            
            except Exception as e:
                print(f'Error processing file {key}: {e}')
        
        # Verifica se há mais páginas de resultados
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break