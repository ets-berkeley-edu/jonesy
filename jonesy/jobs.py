from contextlib import contextmanager
from datetime import datetime
import gzip
import hashlib
import os
import tempfile
import time

import boto3
from botocore.exceptions import ClientError as BotoClientError, ConnectionError as BotoConnectionError
from jonesy import queries
import oracledb


BATCH_SIZE = 120000
RECENT_REFRESH_CUTOFF_DAYS = 1


class Job:

    def __init__(self, name, config):
        self.name = name
        self.config = config

    def run(self):
        daily_path = get_daily_path()
        if self.name == 'upload_advisors':
            self.upload_query_results(
                queries.get_advisor_notes_access(),
                f'sis-data/sis-sysadm/{daily_path}/advisors/advisor-note-permissions.gz',
            )
            self.upload_query_results(
                queries.get_instructor_advisor_relationships(),
                f'sis-data/sis-sysadm/{daily_path}/advisors/instructor-advisor-map.gz',
            )
        elif self.name == 'upload_recent_refresh':
            recency_cutoff = datetime.fromtimestamp(time.time() - (RECENT_REFRESH_CUTOFF_DAYS * 86400))
            for term_id in self.get_current_term_ids():
                self.upload_query_results(
                    queries.get_recent_instructor_updates(term_id, recency_cutoff),
                    f'sis-data/{daily_path}/instructor-updates-{term_id}.gz',
                )
                self.upload_query_results(
                    queries.get_recent_enrollment_updates(term_id, recency_cutoff),
                    f'sis-data/{daily_path}/enrollment-updates-{term_id}.gz',
                )
        elif self.name == 'upload_snapshot':
            self.upload_batched_query_results(
                queries.get_basic_attributes(),
                f'sis-data/{daily_path}/basic-attributes.gz',
            )
            for term_id in self.get_current_term_ids():
                self.upload_query_results(
                    queries.get_term_courses(term_id),
                    f'sis-data/{daily_path}/courses-{term_id}.gz',
                )
                self.upload_batched_query_results(
                    queries.get_term_enrollments(term_id),
                    f'sis-data/{daily_path}/enrollments-{term_id}.gz',
                )
        else:
            print(f"Job {self.name} not found, aborting")

    def get_client(self):
        session = self.get_session()
        return session.client('s3', region_name=self.config['AWS_REGION'])

    def get_current_term_ids(self):
        with sisedo_connection(self.config) as sisedo:
            term_ids = [r[0] for r in sisedo.execute(queries.get_current_terms())]
        return term_ids

    def get_session(self):
        if self.config['AWS_ROLE_ARN']:
            credentials = self.get_sts_credentials()
            return boto3.Session(
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken'],
            )
        else:
            return boto3.Session(
                aws_access_key_id=self.config['AWS_ACCESS_KEY_ID'],
                aws_secret_access_key=self.config['AWS_SECRET_ACCESS_KEY'],
            )

    def get_sts_credentials(self):
        sts_client = boto3.client('sts')
        assumed_role_object = sts_client.assume_role(
            RoleArn=self.config['AWS_ROLE_ARN'],
            RoleSessionName='AssumeAppRoleSession',
            DurationSeconds=3600,
        )
        return assumed_role_object['Credentials']

    def upload_batched_query_results(self, batch_query, s3_key):
        with tempfile.TemporaryFile() as results_tempfile:
            results_gzipfile = gzip.GzipFile(mode='wb', fileobj=results_tempfile)
            with sisedo_connection(self.config) as sisedo:
                batch = 0
                while True:
                    sql = batch_query(batch, BATCH_SIZE)
                    row_count = 0
                    for r in sisedo.execute(sql):
                        row_count += 1
                        results_gzipfile.write(encoded_tsv_row(r) + b'\n')
                    # If we receive fewer rows than the batch size, we've read all available rows and are done.
                    if row_count < BATCH_SIZE:
                        break
                    batch += 1
            results_gzipfile.close()

            self.upload_data(results_tempfile, s3_key)

    def upload_data(self, data, s3_key):
        if 'TARGETS' not in self.config:
            print('No S3 targets specified, aborting')
            exit()
        client = self.get_client()
        for bucket in self.config['TARGETS'].split(','):
            try:
                data.seek(0)
                client.put_object(Bucket=bucket, Key=s3_key, Body=data, ServerSideEncryption='AES256')
            except (BotoClientError, BotoConnectionError, ValueError) as e:
                print(f'Error on S3 upload: bucket={bucket}, key={s3_key}, error={e}')
                return False
            print(f'S3 upload complete: bucket={bucket}, key={s3_key}')
        return True

    def upload_query_results(self, sql, s3_key):
        with tempfile.TemporaryFile() as results_tempfile:
            results_gzipfile = gzip.GzipFile(mode='wb', fileobj=results_tempfile)
            with sisedo_connection(self.config) as sisedo:
                for r in sisedo.execute(sql):
                    results_gzipfile.write(encoded_tsv_row(r) + b'\n')
            results_gzipfile.close()

            self.upload_data(results_tempfile, s3_key)


def get_daily_path():
    today = datetime.now().strftime('%Y-%m-%d')
    digest = hashlib.md5(today.encode()).hexdigest()
    return f"daily/{digest}-{today}"


def encoded_tsv_row(elements):
    def _to_tsv_string(e):
        if e is None:
            return ''
        else:
            return str(e)
    return '\t'.join([_to_tsv_string(e) for e in elements]).encode()


@contextmanager
def sisedo_connection(config):
    with oracledb.connect(
        user=config['SISEDO_UN'],
        password=config['SISEDO_PW'],
        host=config['SISEDO_HOST'],
        port=config['SISEDO_PORT'],
        sid=config['SISEDO_SID'],
    ) as connection:
        with connection.cursor() as cursor:
            yield cursor
