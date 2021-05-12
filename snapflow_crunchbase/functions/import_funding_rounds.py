from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from snapflow import DataFunctionContext, datafunction
from . import base_import


@dataclass
class ImportCrunchbaseFundingRoundsCSVState:
    latest_imported_at: datetime


@datafunction(
    "import_funding_rounds",
    namespace="crunchbase",
    state_class=ImportCrunchbaseFundingRoundsCSVState,
    display_name="Import Crunchbase Funding Rounds",
)
def import_funding_rounds(
    ctx: DataFunctionContext, user_key: str, use_sample: bool = False,
):
    base_import(
        data_source="funding_rounds", user_key=user_key, ctx=ctx, use_sample=use_sample,
    )
