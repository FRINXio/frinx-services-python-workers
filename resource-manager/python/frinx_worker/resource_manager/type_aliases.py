from ipaddress import IPv4Address
from ipaddress import IPv6Address
from typing import Literal
from typing import Optional
from typing import TypeAlias
from typing import Union

from graphql_pydantic_converter.graphql_types import Mutation
from graphql_pydantic_converter.graphql_types import Query
from pydantic.v1.errors import IPv4AddressError
from pydantic.v1.errors import IPv6AddressError

GraphQueryRenderer: TypeAlias = Union[Query, Mutation]
IPvAddress: TypeAlias = Union[IPv4Address, IPv6Address]
IPAddressError: TypeAlias = Union[IPv4AddressError, IPv6AddressError]
IPAddressDict: TypeAlias = dict[str, IPvAddress]
OptionalIPAddressDict: TypeAlias = dict[str, Optional[IPvAddress]]
ISODateTimeString: TypeAlias = str  # not validated datetime (string) format YYYY-MM-DD-hh
CursorID: TypeAlias = str  # not validated cursor ID, probably (16 alphabet string)
# TODO: define prefixes the "smarter way"
IPv4PrefixTypeAlias: TypeAlias = Literal[
    '/0', '/1', '/2', '/3', '/4', '/5', '/6', '/7', '/8', '/9', '/10', '/11', '/12', '/13', '/14', '/15', '/16',
    '/17','/18', '/19', '/20', '/21', '/22', '/23', '/24', '/25', '/26', '/27', '/28', '/29', '/30', '/31', '/32'
]
IPv6PrefixTypeAlias: TypeAlias = Literal[
    '/0', '/1', '/2', '/3', '/4', '/5', '/6', '/7', '/8', '/9', '/10', '/11', '/12', '/13', '/14', '/15', '/16',
    '/17', '/18', '/19', '/20', '/21', '/22', '/23', '/24', '/25', '/26', '/27', '/28', '/29', '/30', '/31',
    '/32', '/33', '/34', '/35', '/36', '/37', '/38', '/39', '/40', '/41', '/42', '/43', '/44', '/45', '/46',
    '/47', '/48', '/49', '/50', '/51', '/52', '/53', '/54', '/55', '/56', '/57', '/58', '/59', '/60', '/61',
    '/62', '/63', '/64', '/65', '/66', '/67', '/68', '/69', '/70', '/71', '/72', '/73', '/74', '/75', '/76',
    '/77', '/78', '/79', '/80', '/81', '/82', '/83', '/84', '/85', '/86', '/87', '/88', '/89', '/90', '/91',
    '/92', '/93', '/94', '/95', '/96', '/97', '/98', '/99', '/100', '/101', '/102', '/103', '/104', '/105',
    '/106', '/107', '/108', '/109', '/110', '/111', '/112', '/113', '/114', '/115', '/116', '/117', '/118',
    '/119', '/120', '/121', '/122', '/123', '/124', '/125', '/126', '/127', '/128'
]
