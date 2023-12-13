

def parse_ranges(list_of_strings: list[str]) -> list[int]:
    """Takes list containing integers (str) and ranges (str)
    and returns corresponding list of integers.

    Args:
        list_of_strings: List of strings representing integers or range of integers.
            Example: ["3", "5-6" , "8", "10 - 12"]
    Returns:
        output: List of integers.
            Example: [3, 5, 6, 8, 10, 11, 12]

    """
    output = []
    for x in list_of_strings:
        try:
            number = int(x)
            output.append(number)
        except ValueError:
            try:
                range_start, range_stop = map(int, x.split('-'))
            except Exception:
                # continue
                raise ValueError(f"Value '{x}' does not represent an integer.")

            output += list(range(range_start, range_stop + 1))
    return output


def get_list_of_ip_addresses(ip_addresses: str) -> list[str]:
    """
    Creates list of IP addresses.
    Args:
        ip_addresses:
            String containing valid IP subnet e.g.: 172.16.1.0/24
            or IP address range e.g.: 172.16.1.10-172.16.1.20
    Returns:
        Union[list, str]:
            List containing first and last IP address of given range
            e.g.: ['172.16.1.10', '172.16.1.20']
            or '172.16.1.100/32'
    """
    ip_addresses = ip_addresses.replace(' ', '')

    if '/' in ip_addresses:
        return [ip_addresses]

    if '-' in ip_addresses:
        ip_addresses_list = ip_addresses.split(sep='-', maxsplit=1)
        start_ip = ip_addresses_list[0]
        end_ip = ip_addresses_list[1]

        return [start_ip, end_ip]

    else:
        return [ip_addresses]
