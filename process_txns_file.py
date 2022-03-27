import csv
import os
from datetime import datetime
from decimal import Decimal

from aws_lambda_powertools import Logger
from aws_lambda_powertools import Tracer
import boto3
import tempfile

aws_region = os.environ['AWS_REGION']

log_level = os.getenv('LOG_LEVEL')

s3_resource = boto3.resource('s3', region_name=aws_region)

logger = Logger()
tracer = Tracer()

credit_type = 'CREDIT'
debit_type = 'DEBIT'


class Transaction:
    def __init__(self, txn_id, txn_date, txn_amount):
        self.txn_id = txn_id
        self.txn_amount = txn_amount
        self.txn_date = txn_date
        self.txn_type = credit_type if txn_amount >= 0 else debit_type


class ResumeTypeTransaction:
    qty = 0
    total_amount = Decimal(0)

    def add_txn_amount(self, amount):
        self.qty += 1
        self.total_amount += amount

    @property
    def average(self):
        return self.total_amount / self.qty


@tracer.capture_method
def get_s3_object(bucket, key_name, local_file):
    s3_resource.Bucket(bucket).download_file(key_name, local_file)


@tracer.capture_method
def read_txns_file(local_file):
    with open(local_file, 'r') as txns_file:
        txns = list(csv.DictReader(txns_file))
        result = [
            Transaction(
                txn['Id'],
                datetime.strptime(txn['Date'], '%m/%d'),
                Decimal(txn['Transaction'])
            )
            for txn in txns
        ]
    return result


@tracer.capture_method
def make_resume_txns(txns):
    months_txn = dict()
    credit_resume = ResumeTypeTransaction()
    debit_resume = ResumeTypeTransaction()
    balance = 0
    for txn in txns:
        balance += txn.txn_amount
        if txn.txn_type == credit_type:
            credit_resume.add_txn_amount(txn.txn_amount)
        else:
            debit_resume.add_txn_amount(txn.txn_amount)
        months_txn[txn.txn_date.month] = months_txn.get(txn.txn_date.month, 0) + 1
    return {
        'balance': balance,
        'average_debit': debit_resume.average,
        'average_credit': credit_resume.average,
        'transactions_by_month': months_txn
    }


@tracer.capture_method
@logger.inject_lambda_context
def handler(event, context):
    for record in event['Records']:
        tmpdir = tempfile.mkdtemp()
        bucket_name = record['s3']['bucket']['name']
        key_name = record['s3']['object']['key']
        logger.info(f'File to process: {key_name}')
        local_file = os.path.join(tmpdir, key_name)
        get_s3_object(bucket_name, key_name, local_file)
        logger.info(f'File downloaded from S3: {bucket_name} - {key_name}')
        account_txns = read_txns_file(local_file)
        logger.info(f'Total transactions: {len(account_txns)}')
        resume_txns = make_resume_txns(account_txns)
        # TODO
        # Save in DB and send email
        logger.info(resume_txns)
        logger.info(f'Success: {bucket_name} - {key_name}')
