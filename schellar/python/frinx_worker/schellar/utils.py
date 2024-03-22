from dataclasses import dataclass
from typing import Any

import requests
from frinx.common.frinx_rest import SCHELLAR_HEADERS
from frinx.common.frinx_rest import SCHELLAR_URL_BASE
from frinx.common.type_aliases import DictAny


@dataclass
class SchellarOutput:
    status: str
    code: int
    data: Any


def execute_schellar_query(
    query: str, variables: DictAny | None = None, schellar_url_base: str = SCHELLAR_URL_BASE
) -> SchellarOutput:
    """
    Execute GraphQL query to fetch data from schellar
    Args:
        query: GraphQL query.
        variables: GraphQL variables.
        schellar_url_base: Override default Inventory url.

    Returns:
        Http response.
    """ ""
    response = requests.post(schellar_url_base, json={"query": query, "variables": variables}, headers=SCHELLAR_HEADERS)

    data = response.json()

    if data.get("errors") is not None:
        return SchellarOutput(data=data["errors"], status="errors", code=404)

    if data.get("data") is not None:
        return SchellarOutput(data=data["data"], status="data", code=200)

    return SchellarOutput(data="Request failed", status="failed", code=500)
