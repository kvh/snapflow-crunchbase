from typing import TypeVar

from snapflow import SnapflowModule
from .functions.import_funding_rounds import import_funding_rounds
from .functions.import_organizations import import_organizations
from .functions.import_people import import_people

CrunchbaseFundingRound = TypeVar("CrunchbaseFundingRound")
CrunchbaseOrganization = TypeVar("CrunchbaseOrganization")
CrunchbasePerson = TypeVar("CrunchbasePerson")

module = SnapflowModule(
    "crunchbase",
    py_module_path=__file__,
    py_module_name=__name__
)
module.add_function(import_people)
module.add_function(import_organizations)
module.add_function(import_funding_rounds)
