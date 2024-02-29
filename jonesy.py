import os
import tempfile

import boto3
from botocore.exceptions import ClientError as BotoClientError, ConnectionError as BotoConnectionError
from dotenv import dotenv_values
import oracledb


config = {
    **dotenv_values(".env.shared"),
    **dotenv_values(".env.secret"),
    **os.environ, 
}


def encoded_tsv_row(elements):
    def _to_tsv_string(e):
        if e is None:
            return ''
        else:
            return str(e)
    return '\t'.join([_to_tsv_string(e) for e in elements]).encode()


def get_sts_credentials():
    sts_client = boto3.client('sts')
    assumed_role_object = sts_client.assume_role(
        RoleArn=config['ROLE_ARN'],
        RoleSessionName='AssumeAppRoleSession',
        DurationSeconds=3600,
    )
    return assumed_role_object['Credentials']


def get_session():
    credentials = get_sts_credentials()
    return boto3.Session(
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
    )


def get_client():
    session = get_session()
    return session.client('s3', region_name=config['AWS_REGION'])


def upload_data(data, s3_key, bucket):
    try:
        client = get_client()
        client.put_object(Bucket=bucket, Key=s3_key, Body=data, ServerSideEncryption='AES256')
    except (BotoClientError, BotoConnectionError, ValueError) as e:
        print(f'Error on S3 upload: bucket={bucket}, key={s3_key}, error={e}')
        return False
    print(f'S3 upload complete: bucket={bucket}, key={s3_key}')
    return True


with tempfile.TemporaryFile() as advisor_notes_access_file:
    with oracledb.connect(user=config['UN'], password=config['PW'], host=config['HOST'], port=config['PORT'], sid=config['SID']) as connection:
        with connection.cursor() as cursor:
            sql = """SELECT
                A.USER_ID,
                A.CS_ID,
                A.PERMISSION_LIST
            FROM SYSADM.BOA_ADV_NOTES_ACCESS_VW A"""
            for r in cursor.execute(sql):
                advisor_notes_access_file.write(encoded_tsv_row(r) + b'\n')
    
    tsv_filename = f'advisor_notes_access.tsv'
    s3_key = f'sis-data/jonesy-temp/{tsv_filename}'
    advisor_notes_access_file.seek(0)
    upload_data(advisor_notes_access_file, s3_key, config['BUCKET'])
