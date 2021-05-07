from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from snapflow import DataFunctionContext, datafunction
from . import base_import


@dataclass
class ImportCrunchbasePeopleCSVState:
    latest_imported_at: datetime


@datafunction(
    "import_people",
    namespace="crunchbase",
    state_class=ImportCrunchbasePeopleCSVState,
    display_name="Import Crunchbase People",
)
def import_people(
        ctx: DataFunctionContext,
        user_key: str
):
    base_import(
        data_source="people",
        user_key=user_key,
        ctx=ctx
    )
