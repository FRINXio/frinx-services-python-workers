import requests

from frinx.common.frinx_rest import INVENTORY_HEADERS
from frinx.common.frinx_rest import INVENTORY_URL_BASE
from frinx.common.type_aliases import DictAny


def execute_inventory_query(
    query: str,
    variables: DictAny | None = None,
    inventory_url_base: str = INVENTORY_URL_BASE
) -> requests.Response:
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

    response.raise_for_status()
    return response
