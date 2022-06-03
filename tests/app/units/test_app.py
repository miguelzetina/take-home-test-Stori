import os

from datetime import datetime
from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from aws_lambda_powertools.logging.lambda_context import LambdaContextModel

from app.process_txns_file import SummaryTypeTransaction, Transaction, credit_type, debit_type
from app.process_txns_file import convert_summary_to_html, download_file_s3, handler, make_summary_txns, read_txns_file, save_txns, send_email


class SummaryTypeTransactionUnitTests(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.debit_amounts = [
            Decimal(1.25),
            Decimal(2.25),
            Decimal(3.33),
            Decimal(4.33),
            Decimal(5.33)
        ]
        cls.credit_amounts = [
            Decimal(-1.25),
            Decimal(-2.25),
            Decimal(-3.33),
            Decimal(-4.33),
            Decimal(-5.33)
        ]

    def test_summary_type_debit_transactions(self):
        summary = SummaryTypeTransaction()
        for amount in self.debit_amounts:
            summary.add_txn_amount(amount)
        self.assertEqual(summary.qty, len(self.debit_amounts))
        self.assertEqual(summary.total_amount, Decimal("16.49"))
        self.assertEqual(summary.average, Decimal("3.29"))

    def test_summary_type_credit_transactions(self):
        summary = SummaryTypeTransaction()
        for amount in self.credit_amounts:
            summary.add_txn_amount(amount)
        self.assertEqual(summary.qty, len(self.credit_amounts))
        self.assertEqual(summary.total_amount, Decimal("-16.49"))
        self.assertEqual(summary.average, Decimal("-3.29"))


class TransactionUnitTests(TestCase):

    def test_create_transaction_as_credit(self):
        txn = Transaction("1", datetime.now(), Decimal(1))
        assert txn.txn_type == credit_type

    def test_create_transaction_as_debit(self):
        txn = Transaction("1", datetime.now(), Decimal(-1))
        assert txn.txn_type == debit_type


class MakeSummaryTransactionsTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.txns = [
            Transaction("0", datetime(2022, 7, 15), Decimal(60.5)),
            Transaction("1", datetime(2022, 7, 28), Decimal(-10.3)),
            Transaction("2", datetime(2022, 8, 2), Decimal(-20.46)),
            Transaction("3", datetime(2022, 8, 13), Decimal(10)),
        ]

    def test_create_correct_summary(self):
        result = make_summary_txns(self.txns)
        expected = {
            "balance" : Decimal("39.74"),
            "average_debit": Decimal("-15.38"),
            "average_credit": Decimal("35.25"),
            "transactions_by_month": {7: 2, 8: 2}
        }
        self.assertEqual(result, expected)


class ConvertSummaryToHTMLTests(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.summary = {
            "balance" : Decimal("39.74"),
            "average_debit": Decimal("-15.38"),
            "average_credit": Decimal("35.25"),
            "transactions_by_month": {7: 2, 8: 2}
        }

    def test_convert_summary_to_html(self):
        result = convert_summary_to_html(self.summary)
        self.assertIn("July: 2", result)
        self.assertIn("August: 2", result)


class ReadTransactionsFileTests(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        current_dir = os.path.dirname(__file__)
        cls.file_name = "stori_account_1234567890.csv"
        cls.file_path = os.path.join(current_dir, "examples/{0}".format(cls.file_name))

    def test_convert_file_to_transactions(self):
        result = read_txns_file(self.file_path)
        self.assertEqual(len(result), 4)
    
    @patch("app.process_txns_file.s3_resource")
    def test_download_transaction_file(self, s3_resource):
        s3_resource.Bucket.download_file.return_value = True
        result = download_file_s3("bucket_name", self.file_name)
        self.assertIn(self.file_name, result)


class SaveTransactionsDynamoTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.txns = [
            Transaction("0", datetime(2022, 7, 15), Decimal(60.5)),
            Transaction("1", datetime(2022, 7, 28), Decimal(-10.3)),
            Transaction("2", datetime(2022, 8, 2), Decimal(-20.46)),
            Transaction("3", datetime(2022, 8, 13), Decimal(10)),
        ]
        return super().setUpClass()

    @patch("app.process_txns_file.dynamo_client")
    def test_save_each_transaction_in_dynamo(self, dynamo_client):
        # TODO validate response types from Dynamo Client
        dynamo_client.put_item.return_value = True
        save_txns(self.txns)

class SendEmailTest(TestCase):

    @patch("app.process_txns_file.ses_client")
    def test_success_send_email(self, ses_client):
        # TODO validate response types from SES Client
        ses_client.send_email.return_value = True
        send_email('email@email.com', 'subject')


class HandlerTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.file_name = "stori_account_1234567890.csv"
        cls.valid_event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {
                            "name": "bucket_name"
                        },
                        "object": {
                            "key": cls.file_name
                        }
                    }
                }
            ]
        }
        current_dir = os.path.dirname(__file__)
        cls.file_path = os.path.join(current_dir, "examples/{}".format(cls.file_name))
    
    @patch("app.process_txns_file.dynamo_client")
    @patch("app.process_txns_file.ses_client")
    @patch("app.process_txns_file.download_file_s3")
    def test_handler_successfully(self, download_file_s3, ses_client, dynamo_client):
        dynamo_client.put_item.return_value = True
        ses_client.send_email.return_value = True
        download_file_s3.return_value = self.file_path
        handler(self.valid_event, LambdaContextModel())
