from __future__ import annotations

import io
import os
import tarfile

from dcp.data_format import CsvFileFormat
from dcp.utils.common import utcnow

from snapflow import DataFunctionContext
from snapflow.helpers.connectors.connection import HttpApiConnection

CRUNCHBASE_BULK_CSV_URL = os.getenv("CRUNCHBASE_BULK_CSV_URL",
                                    default="https://static.crunchbase.com/data_crunchbase/bulk_export_sample.tar.gz")
CRUNCHBASE_CSV_TO_SCHEMA_MAP = {
    "organizations": "crunchbase.CrunchbaseOrganization",
    "people": "crunchbase.CrunchbasePerson",
    "funding_rounds": "crunchbase.CrunchbaseFundingRound",
}


def base_import(
        data_source: str,
        ctx: DataFunctionContext,
        user_key: str
):
    params = {
        "user_key": user_key,
    }

    while ctx.should_continue():
        resp = HttpApiConnection().get(
            url=CRUNCHBASE_BULK_CSV_URL,
            params=params,
        )

        ib = io.BytesIO(resp.content)

        with tarfile.open(fileobj=ib) as csv_files:
            raw = csv_files.extractfile("{}.csv".format(data_source))
            ctx.emit_state_value("imported_{}".format(data_source), True)
            ctx.emit(raw, data_format=CsvFileFormat, schema=CRUNCHBASE_CSV_TO_SCHEMA_MAP[data_source])
            ctx.emit_state_value("latest_imported_at".format(data_source), utcnow())

        return
