import os
import pytest

from snapflow import Environment
from snapflow.core.graph import graph


def ensure_user_key() -> str:
    api_key = os.environ.get("CRUNCHBASE_USER_KEY")
    if api_key is not None:
        return api_key
    api_key = input("Enter Crunchbase User Key: ")
    return api_key


def test_crunchbase():
    user_key = ensure_user_key()
    run_test_crunchbase_import_people(user_key)
    run_test_crunchbase_import_organizations(user_key)
    run_test_crunchbase_import_funding_rounds(user_key)


def run_test_crunchbase_import_people(user_key):
    from snapflow_crunchbase import module as snapflow_crunchbase

    env = Environment()
    env.add_module(snapflow_crunchbase)
    env.add_storage("file://.")
    g = graph()

    # test people importer
    import_people = g.create_node(
        snapflow_crunchbase.functions.import_people,
        params={"user_key": user_key, "use_sample": True},
    )
    output = env.produce(node_like=import_people, graph=g)
    assert len(output[0].as_dataframe()) == 50


def run_test_crunchbase_import_organizations(user_key):
    from snapflow_crunchbase import module as snapflow_crunchbase

    env = Environment()
    env.add_module(snapflow_crunchbase)
    env.add_storage("file://.")
    g = graph()

    # test organizations importer
    import_organizations = g.create_node(
        snapflow_crunchbase.functions.import_organizations,
        params={"user_key": user_key, "use_sample": True},
    )
    output = env.produce(node_like=import_organizations, graph=g)
    assert len(output[0].as_dataframe()) == 50


def run_test_crunchbase_import_funding_rounds(user_key):
    from snapflow_crunchbase import module as snapflow_crunchbase

    env = Environment()
    env.add_module(snapflow_crunchbase)
    env.add_storage("file://.")
    g = graph()

    # test funding rounds importer
    import_funding_rounds = g.create_node(
        snapflow_crunchbase.functions.import_funding_rounds,
        params={"user_key": user_key, "use_sample": True},
    )
    output = env.produce(node_like=import_funding_rounds, graph=g)
    assert len(output[0].as_dataframe()) == 50


if __name__ == "__main__":
    test_crunchbase()
