from dataclasses import dataclass
from typing import Any
from typing import Optional
from typing import TypeAlias

import requests
from frinx.common.frinx_rest import INVENTORY_HEADERS
from frinx.common.frinx_rest import INVENTORY_URL_BASE
from frinx.common.type_aliases import DictAny

CursorGroup: TypeAlias = dict[str, list[dict[str, str]]]
CursorGroups: TypeAlias = dict[str, dict[str, list[dict[str, str]]]]


@dataclass
class InventoryOutput:
    status: str
    code: int
    data: Any


def execute_inventory_query(
    query: str,
    variables: Optional[DictAny] = None,
    inventory_url_base: str = INVENTORY_URL_BASE
) -> InventoryOutput:
    """
    Execute GraphQL query to fetch data from inventory
    Args:
        query: GraphQL query.
        variables: GraphQL variables.
        inventory_url_base: Override default Inventory url.

    Returns:
        Http response.
    """''
    response = requests.post(
        inventory_url_base,
        json={'query': query, 'variables': variables},
        headers=INVENTORY_HEADERS
    )

    data = response.json()

    if data.get('errors') is not None:
        return InventoryOutput(data=data['errors'], status='errors', code=404)

    if data.get('data') is not None:
        return InventoryOutput(data=data['data'], status='data', code=200)

    return InventoryOutput(data='Request failed', status='failed', code=500)
