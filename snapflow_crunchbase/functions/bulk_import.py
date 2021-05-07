from __future__ import annotations

import csv
import io
import json
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import datetime
from http import HTTPStatus
from pprint import pprint
from typing import Iterator, Dict, Any

from dcp.data_format import Records
from dcp.utils.common import utcnow

import snapflow_crunchbase as crunchbase
from dcp.data_format import CsvFileFormat
from snapflow import Function, Context, DataBlock, DataFunctionContext, datafunction
from snapflow.helpers.connectors.connection import HttpApiConnection

BIGCOMMERCE_API_BASE_URL = "https://api.crunchbase.com/bulk/v4/bulk_export.tar.gz"
CRUNCHBASE_BULK_CSV_URL = "http://static.crunchbase.com/data_crunchbase/bulk_export_sample.tar.gz"
CRUNCHBASE_CSV_TO_SCHEMA_MAP = {
    ""
}


@dataclass
class ImportCrunchbaseCSVState:
    latest_imported_at: datetime



@datafunction(
    "bulk_import",
    namespace="crunchbase",
    # state_class=ImportCrunchbaseCSVState,
    display_name="Import Crunchbase data",
    required_storage_classes=["file"],
)
def bulk_import(
        ctx: DataFunctionContext,
        user_key: str
):
    params = {
        "user_key": user_key,
    }

    # while ctx.should_continue():
    # ctx.emit_state_value("latest_imported_at", utcnow())

    resp = HttpApiConnection().get(
        url=CRUNCHBASE_BULK_CSV_URL,
        params=params,
    )

    print("------")
    print(resp)

    # tf = tempfile.TemporaryFile()
    # tf = open("/Users/rootx/Projects/SnapData/test.tar.gz", "wb")
    # tf.write(resp.content)
    # tf.close()

    ib = io.BytesIO(resp.content)

    # tar = tarfile.open("/Users/rootx/Projects/SnapData/test.tar.gz", "r:gz")
    with tarfile.open(fileobj=ib) as csv_files:
        raw = csv_files.extractfile("funding_rounds.csv".format(data_source))
        print("----------")
        with io.TextIOWrapper(raw) as raw_str:
            print(list(csv.DictReader(raw_str)))
        print("----------")


# tar.extractall("/Users/rootx/Projects/SnapData/test/")
    # tar.close()
    #
    # raw = open("/Users/rootx/Projects/SnapData/test/organizations.csv", "r")
    #
    # dr = csv.DictReader(open("/Users/rootx/Projects/SnapData/test/organizations.csv", "r"))
    # # print(list(dr))
    # print("------")
    # ctx.emit_state_value("imported", True)
    # ctx.emit(raw, data_format=CsvFileFormat, schema="crunchbase.CrunchbasePerson")

    # ctx.emit_state_value("imported", True)
    # ctx.emit(raw, storage=ctx.execution_context.target_storage, data_format=CsvFileFormat)
    # # check if there is anything left to process
    # if resp.status_code == HTTPStatus.NO_CONTENT:
    #     break
    #
    # json_resp = resp.json()
    #
    # assert isinstance(json_resp, list)
    #
    # yield resp.json()
