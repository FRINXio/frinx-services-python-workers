from frinx.common.frinx_rest import INFLUXDB_URL_BASE
from influxdb_client import InfluxDBClient


class InfluxDbWrapper:
    def __init__(self, token: str, org: str) -> None:
        self.url = INFLUXDB_URL_BASE
        self.token = token
        self.org = org

    def client(self) -> InfluxDBClient:
        return InfluxDBClient(url=self.url, token=self.token, org=self.org)
