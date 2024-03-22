import unittest

from frinx_api.uniconfig.device.discovery.discover import Address
from frinx_api.uniconfig.device.discovery.discover import TcpPortItem
from frinx_api.uniconfig.device.discovery.discover import UdpPortItem

from frinx_worker.uniconfig.device_discovery import DeviceDiscoveryWorkers

# from uniconfig.python.frinx_worker.uniconfig.device_discovery import DeviceDiscoveryWorkers  # type: ignore


class TestDeviceDiscovery(unittest.TestCase):
    def test_tcp_validation_list(self) -> None:
        tcp_port = "21,22,23"
        expected = [TcpPortItem(port=21), TcpPortItem(port=22), TcpPortItem(port=23)]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_tcp(tcp_port=tcp_port)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_tcp_validation_range(self) -> None:
        tcp_port = "21-23"
        expected = [TcpPortItem(port=21), TcpPortItem(port=22), TcpPortItem(port=23)]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_tcp(tcp_port=tcp_port)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_tcp_validation_list_range(self) -> None:
        tcp_port = "21-23,25"
        expected = [TcpPortItem(port=21), TcpPortItem(port=22), TcpPortItem(port=23), TcpPortItem(port=25)]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_tcp(tcp_port=tcp_port)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_udp_validation_list(self) -> None:
        udp_port = "21,22,23"
        expected = [UdpPortItem(port=21), UdpPortItem(port=22), UdpPortItem(port=23)]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_udp(udp_port=udp_port)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_udp_validation_range(self) -> None:
        udp_port = "21-23"
        expected = [UdpPortItem(port=21), UdpPortItem(port=22), UdpPortItem(port=23)]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_udp(udp_port=udp_port)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_udp_validation_list_range(self) -> None:
        udp_port = "21-23,25"
        expected = [UdpPortItem(port=21), UdpPortItem(port=22), UdpPortItem(port=23), UdpPortItem(port=25)]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_udp(udp_port=udp_port)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_validate_ip_single_ip_v4(self) -> None:
        ip = "192.168.0.59"
        expected = [
            Address(
                end_ipv6_address=None,
                ip_address="192.168.0.59",
                hostname=None,
                end_ipv4_address=None,
                start_ipv4_address=None,
                start_ipv6_address=None,
                network=None,
            )
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip=ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_validate_ip_range_ip_v4(self) -> None:
        ip = "192.168.0.59-192.168.0.90"
        expected = [
            Address(
                end_ipv6_address=None,
                ip_address=None,
                hostname=None,
                end_ipv4_address="192.168.0.90",
                start_ipv4_address="192.168.0.59",
                start_ipv6_address=None,
                network=None,
            )
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip=ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_validate_ip_network_v4(self) -> None:
        ip = "192.168.0.0/24"
        expected = [
            Address(
                end_ipv6_address=None,
                ip_address=None,
                hostname=None,
                end_ipv4_address=None,
                start_ipv4_address=None,
                start_ipv6_address=None,
                network="192.168.0.0/24",
            )
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip=ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_validate_ip_single_ip_v6(self) -> None:
        ip = "0000:0000:0000:0000:0000:ffff:c0a8:003b"
        expected = [
            Address(
                end_ipv6_address=None,
                ip_address="::ffff:c0a8:3b",
                hostname=None,
                end_ipv4_address=None,
                start_ipv4_address=None,
                start_ipv6_address=None,
                network=None,
            )
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip=ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

        ip = "::ffff:c0a8:3b"
        expected = [
            Address(
                end_ipv6_address=None,
                ip_address="::ffff:c0a8:3b",
                hostname=None,
                end_ipv4_address=None,
                start_ipv4_address=None,
                start_ipv6_address=None,
                network=None,
            )
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip=ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_validate_ip_range_ip_v6(self) -> None:
        ip = "0000:0000:0000:0000:0000:ffff:c0a8:003b-0000:0000:0000:0000:0000:ffff:c0a8:005a"
        expected = [
            Address(
                end_ipv6_address="::ffff:c0a8:5a",
                ip_address=None,
                hostname=None,
                end_ipv4_address=None,
                start_ipv4_address=None,
                start_ipv6_address="::ffff:c0a8:3b",
                network=None,
            )
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip=ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

        ip = "::ffff:c0a8:3b-::ffff:c0a8:5a"
        expected = [
            Address(
                end_ipv6_address="::ffff:c0a8:5a",
                ip_address=None,
                hostname=None,
                end_ipv4_address=None,
                start_ipv4_address=None,
                start_ipv6_address="::ffff:c0a8:3b",
                network=None,
            )
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip=ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_validate_ip_network_v6(self) -> None:
        ip = "::ffff:c0a8:0/128"
        expected = [
            Address(
                end_ipv6_address=None,
                ip_address=None,
                hostname=None,
                end_ipv4_address=None,
                start_ipv4_address=None,
                start_ipv6_address=None,
                network="::ffff:c0a8:0/128",
            )
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)


if __name__ == "__main__":
    unittest.main()
