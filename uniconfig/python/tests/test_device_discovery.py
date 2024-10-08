import unittest

from frinx_api.uniconfig.device.discovery.discover import Address
from frinx_api.uniconfig.device.discovery.discover import DeviceDiscoveryTypeOfAddressModel
from frinx_api.uniconfig.device.discovery.discover import DeviceDiscoveryTypeOfAddressModel1
from frinx_api.uniconfig.device.discovery.discover import DeviceDiscoveryTypeOfAddressModel2
from frinx_api.uniconfig.device.discovery.discover import DeviceDiscoveryTypeOfAddressModel3
from frinx_api.uniconfig.device.discovery.discover import DeviceDiscoveryTypeOfPortModel
from frinx_api.uniconfig.device.discovery.discover import DeviceDiscoveryTypeOfPortModel2
from frinx_api.uniconfig.device.discovery.discover import TcpPortItem
from frinx_api.uniconfig.device.discovery.discover import UdpPortItem

from frinx_worker.uniconfig.device_discovery import DeviceDiscoveryWorkers

# from uniconfig.python.frinx_worker.uniconfig.device_discovery import DeviceDiscoveryWorkers  # type: ignore


class TestDeviceDiscovery(unittest.TestCase):
    def test_tcp_validation_list(self) -> None:
        tcp_port = "21,22,23"
        expected = [TcpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel(port=21)),
                    TcpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel(port=22)),
                    TcpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel(port=23))
                    ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_tcp(tcp_port=tcp_port)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_tcp_validation_range(self) -> None:
        tcp_port = "21-23"
        expected = [TcpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel(port=21)),
                    TcpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel(port=22)),
                    TcpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel(port=23))
                    ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_tcp(tcp_port=tcp_port)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_tcp_validation_list_range(self) -> None:
        tcp_port = "21-23,25"
        expected = [TcpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel(port=21)),
                    TcpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel(port=22)),
                    TcpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel(port=23)),
                    TcpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel(port=25))
                    ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_tcp(tcp_port=tcp_port)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_udp_validation_list(self) -> None:
        udp_port = "21,22,23"
        expected = [UdpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel2(port=21)),
                    UdpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel2(port=22)),
                    UdpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel2(port=23))
                    ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_udp(udp_port=udp_port)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_udp_validation_range(self) -> None:
        udp_port = "21-23"
        expected = [UdpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel2(port=21)),
                    UdpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel2(port=22)),
                    UdpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel2(port=23))
                    ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_udp(udp_port=udp_port)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_udp_validation_list_range(self) -> None:
        udp_port = "21-23,25"
        expected = [UdpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel2(port=21)),
                    UdpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel2(port=22)),
                    UdpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel2(port=23)),
                    UdpPortItem(device_discovery_type_of_port=DeviceDiscoveryTypeOfPortModel2(port=25))
                    ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_udp(udp_port=udp_port)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_validate_ip_single_ip_v4(self) -> None:
        ip = "192.168.0.59"
        expected = [
            Address(device_discovery_type_of_address=DeviceDiscoveryTypeOfAddressModel(ip_address="192.168.0.59"))
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip=ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_validate_ip_range_ip_v4(self) -> None:
        ip = "192.168.0.59-192.168.0.90"
        expected = [
            Address(device_discovery_type_of_address=DeviceDiscoveryTypeOfAddressModel1(
                                start_ipv4_address="192.168.0.59",
                                end_ipv4_address="192.168.0.90",
                            )
            )
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip=ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_validate_ip_network_v4(self) -> None:
        ip = "192.168.0.0/24"
        expected = [
            Address(device_discovery_type_of_address=DeviceDiscoveryTypeOfAddressModel3(network="192.168.0.0/24"))
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip=ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_validate_ip_single_ip_v6(self) -> None:
        ip = "0000:0000:0000:0000:0000:ffff:c0a8:003b"
        expected = [
            Address(device_discovery_type_of_address=DeviceDiscoveryTypeOfAddressModel(ip_address="::ffff:c0a8:3b"))
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip=ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

        ip = "::ffff:c0a8:3b"
        expected = [
            Address(device_discovery_type_of_address=DeviceDiscoveryTypeOfAddressModel(ip_address="::ffff:c0a8:3b"))
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip=ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_validate_ip_range_ip_v6(self) -> None:
        ip = "0000:0000:0000:0000:0000:ffff:c0a8:003b-0000:0000:0000:0000:0000:ffff:c0a8:005a"
        expected = [
            Address(
                device_discovery_type_of_address=DeviceDiscoveryTypeOfAddressModel2(
                    start_ipv6_address="::ffff:c0a8:3b",
                    end_ipv6_address="::ffff:c0a8:5a"
                )
            )
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip=ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

        ip = "::ffff:c0a8:3b-::ffff:c0a8:5a"
        expected = [
            Address(
                device_discovery_type_of_address=DeviceDiscoveryTypeOfAddressModel2(
                    start_ipv6_address="::ffff:c0a8:3b",
                    end_ipv6_address="::ffff:c0a8:5a"
                )
            )
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip=ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)

    def test_validate_ip_network_v6(self) -> None:
        ip = "::ffff:c0a8:0/128"
        expected = [
            Address(device_discovery_type_of_address=DeviceDiscoveryTypeOfAddressModel3(network="::ffff:c0a8:0/128"))
        ]
        result = DeviceDiscoveryWorkers.DeviceDiscoveryWorker.WorkerInput.validate_ip(ip)  # type: ignore
        assert expected == result
        assert isinstance(result, list)


if __name__ == "__main__":
    unittest.main()
