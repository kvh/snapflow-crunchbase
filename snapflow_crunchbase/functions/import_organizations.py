from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from snapflow import DataFunctionContext, datafunction
from . import base_import


@dataclass
class ImportCrunchbaseOrganizationsCSVState:
    latest_imported_at: datetime


@datafunction(
    "import_organizations",
    namespace="crunchbase",
    state_class=ImportCrunchbaseOrganizationsCSVState,
    display_name="Import Crunchbase Organizations",
)
def import_organizations(
        ctx: DataFunctionContext,
        user_key: str
):
    return base_import(
        data_source="organizations",
        user_key=user_key,
        ctx=ctx
    )
