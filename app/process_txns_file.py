import calendar
import csv
import os
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

from aws_lambda_powertools import Logger
from aws_lambda_powertools import Tracer
import boto3
import tempfile

aws_region = os.environ['AWS_REGION']

log_level = os.getenv('LOG_LEVEL')

to_email = os.getenv('TO_EMAIL', 'storimiguelzetina@gmail.com')
from_email = os.getenv('FROM_EMAIL',
                       'Stori Take Home <storimiguelzetina@gmail.com>')
table_txn = os.getenv('TABLE_TXN', 'stori_txn')

s3_resource = boto3.resource('s3', region_name=aws_region)
dynamo_client = boto3.client('dynamodb', region_name=aws_region)
ses_client = boto3.client('ses', region_name=aws_region)

logger = Logger()
tracer = Tracer()

credit_type = 'CREDIT'
debit_type = 'DEBIT'


class Transaction:
    def __init__(self, txn_id: str, txn_date: datetime, txn_amount: Decimal):
        self.txn_id = txn_id
        self.txn_amount = txn_amount.quantize(Decimal('.01'), rounding=ROUND_DOWN)
        self.txn_date = txn_date
        self.txn_type = credit_type if txn_amount >= 0 else debit_type


class SummaryTypeTransaction:
    qty = 0
    total_amount = Decimal(0).quantize(Decimal('.01'), rounding=ROUND_DOWN)

    def add_txn_amount(self, amount: Decimal):
        self.qty += 1
        self.total_amount += amount.quantize(Decimal('.01'), rounding=ROUND_DOWN)

    @property
    def average(self):
        return (self.total_amount / self.qty).quantize(Decimal('.01'), rounding=ROUND_DOWN)


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
def make_summary_txns(txns: list):
    months_txn = dict()
    credit_summary = SummaryTypeTransaction()
    debit_summary = SummaryTypeTransaction()
    balance = 0
    for txn in txns:
        balance += txn.txn_amount
        if txn.txn_type == credit_type:
            credit_summary.add_txn_amount(txn.txn_amount)
        else:
            debit_summary.add_txn_amount(txn.txn_amount)
        months_txn[txn.txn_date.month] = (
            months_txn.get(txn.txn_date.month, 0) + 1
        )
    return {
        'balance': balance,
        'average_debit': debit_summary.average,
        'average_credit': credit_summary.average,
        'transactions_by_month': months_txn
    }


@tracer.capture_method
def insert_txn_db(txn_id: str,
                  txn_amount: Decimal,
                  txn_date: datetime,
                  txn_type: str):
    dynamo_client.put_item(
        TableName=table_txn,
        Item={
            'txn_id': {
                'S': txn_id
            },
            'txn_amount': {
                'N': str(txn_amount)
            },
            'txn_date': {
                'S': txn_date.isoformat()
            },
            'txn_type': {
                'S': txn_type
            }
        }
    )


@tracer.capture_method
def save_txns(list_txns: list):
    for txn in list_txns:
        insert_txn_db(txn.txn_id, txn.txn_amount, txn.txn_date, txn.txn_type)


@tracer.capture_method
def send_email(destination_email: str,
               subject: str,
               body_html: str = '',
               body_text: str = ''):
    destination = {
        'ToAddresses': [
            destination_email,
        ]
    }

    charset = 'UTF-8'

    response = ses_client.send_email(
        Destination=destination,
        Message={
            'Body': {
                'Html': {
                    'Charset': charset,
                    'Data': body_html or '',
                },
                'Text': {
                    'Charset': charset,
                    'Data': body_text,
                },
            },
            'Subject': {
                'Charset': charset,
                'Data': subject,
            },
        },
        Source=from_email,
    )
    logger.info(response)


def convert_summary_to_html(summary_data):
    months_summary = ''
    months_data = summary_data['transactions_by_month']
    for month in months_data:
        months_summary += f'<p>Number of transactions in '
        months_summary += f'{calendar.month_name[month]}: '
        months_summary += f'{months_data[month]}</p>'
    html_text = f"""<!DOCTYPE html>
<html>
<body>

<img src="https://blog.storicard.com/wp-content/uploads/2019/07/Stori-horizontal-10.jpg" alt="Stori Logo Horizontal" width="250">

<h1>Transaction summary</h1>

<p>Total balance is {summary_data['balance']} </p>
{months_summary}
<p>Average debit amount: {summary_data['average_debit']}</p>
<p>Average credit amount: {summary_data['average_credit']}</p>

</body>
</html>"""
    return html_text


@tracer.capture_method
def download_file_s3(bucket_name, file_name):
    tmpdir = tempfile.mkdtemp()
    local_file = os.path.join(tmpdir, file_name)
    s3_resource.Bucket(bucket_name).download_file(file_name, local_file)
    return local_file


@tracer.capture_method
def handler(event, context):
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        key_name = record['s3']['object']['key']
        logger.info(f'File to process: {key_name}')
        local_file = download_file_s3(bucket_name, key_name)
        logger.info(f'File downloaded from S3: {bucket_name} - {key_name}')
        account_txns = read_txns_file(local_file)
        save_txns(account_txns)
        logger.info(f'Total transactions: {len(account_txns)}')
        summary_txns = make_summary_txns(account_txns)
        send_email(to_email,
                   'Transaction Summary from Stori',
                   convert_summary_to_html(summary_txns),
                   'Transaction Summary from Stori')
        logger.info(summary_txns)
        logger.info(f'Success: {bucket_name} - {key_name}')
