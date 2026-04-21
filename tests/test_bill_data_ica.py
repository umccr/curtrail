import polars as pl
from curtrail.bill.bill_data_ica import BillDataIca


def test_complete_bill_row_count(bill_data_ica: BillDataIca):
    df = bill_data_ica.as_frame()

    assert df.height == 536


def test_compute_bill_row_count(bill_data_ica: BillDataIca):
    df = bill_data_ica.compute_bill().as_frame()

    assert df.height == 388


def test_storage_bill_row_count(bill_data_ica: BillDataIca):
    df = bill_data_ica.storage_bill().as_frame()

    assert df.height == 146
