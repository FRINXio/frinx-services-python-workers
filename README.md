# frinx-services-python-workers

## Overview
**frinx-services-python-workers** is a collection of Python workers designed to streamline various 
network automation and management tasks. These workers are part of the FRINX.io ecosystem and can be 
used to automate network device provisioning, data collection, and more.

## Installation
To install all workers, use the following command with the defined branch name. 
Please note that you should not use tags as identifiers when installing the package.


```bash
poetry add "https://github.com/FRINXio/frinx-services-python-workers.git@main"

#with ssh
poetry add "git+ssh://git@github.com/FRINXio/frinx-services-python-workers.git@main"

```

If you only need specific subpackages, you can select them using the following command:

```bash
poetry add git+ssh://git@github.com/FRINXio/frinx-services-python-workers.git@main#subdirectory=inventory/python
```

## Usage
Once you've installed the workers, you can use them in your Python projects. 
Import the required modules and use the provided functionality to automate network tasks and management.

```python

def register_tasks(conductor_client):

    from frinx_worker.uniconfig.transactions import Transactions
    from frinx_worker.uniconfig.uniconfig_manager import UniconfigManager
    from frinx_worker.uniconfig.snapshot_manager import SnapshotManager
    from frinx_worker.uniconfig.connection_manager import ConnectionManager
    from frinx_worker.uniconfig.structured_data import StructuredData
    from frinx_worker.uniconfig.cli_network_topology import CliNetworkTopology

    Transactions().register(conductor_client=conductor_client)
    UniconfigManager().register(conductor_client=conductor_client)
    SnapshotManager().register(conductor_client=conductor_client)
    ConnectionManager().register(conductor_client=conductor_client)
    StructuredData().register(conductor_client=conductor_client)
    CliNetworkTopology().register(conductor_client=conductor_client)

def main():

    from frinx.client.frinx_conductor_wrapper import FrinxConductorWrapper
    from frinx.common.frinx_rest import CONDUCTOR_HEADERS
    from frinx.common.frinx_rest import CONDUCTOR_URL_BASE

    conductor_client = FrinxConductorWrapper(
        server_url=CONDUCTOR_URL_BASE,
        polling_interval=0.1,
        max_thread_count=50,
        headers=CONDUCTOR_HEADERS,
    )

    register_tasks(conductor_client)
    conductor_client.start_workers()


if __name__ == '__main__':
    main()

```
