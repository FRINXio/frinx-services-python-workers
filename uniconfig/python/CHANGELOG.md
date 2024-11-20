# 1.0.0
- Upgrade pydantic version to v2

# 1.0.1
- Add missing default value for optional inputs

# 1.0.2
- Update API dependency [frinx-uniconfig-api v0.1.1](https://github.com/FRINXio/frinx-services-python-api/blob/main/uniconfig/python/CHANGELOG.md)

# 1.0.3
- Updated frinx-python-sdk to version ^1.1

# 1.0.4
- Enhanced 'handle_response' function for better error insights

# 2.0.0
- Updated API dependency frinx-uniconfig-api to rev "[frinx-uniconfig-api_v1.0.0](https://github.com/FRINXio/frinx-services-python-api/blob/main/uniconfig/python/CHANGELOG.md)"
- Simplified response handling from UniConfig and refactored DeviceDiscovery

# 2.0.1
- Update dev deps
- Remove obsolete parameters from request APIs

# 2.1.0
- Implemented worker: connection-manager get-installed-nodes RPC
- Implemented worker: connection-manager check-installed-nodes RPC

# 2.2.0
- Support gNMI installation parameters
- Implemented worker: update-install-parameters

# 2.2.1
- Used fixed version of frinx-services-python-api v1.1.1 - fixed discovery RPC model.

# 2.2.2
- Fixed CreateTransaction worker - 'uniconfig_server_id' is optional cookie that
  is appended by load-balancer (LB). In the deployment without LB, it is not set.

# 2.3.0
- Implemented workflow and workers for closing of all open transactions
  in the specified workflow (close_transactions).

# 2.3.1
- Fixed closed_transactions workflow output type (it must be string).

# 2.3.2
- Update Inventory properties and bump Uniconfig API.

# 2.3.3
- Fix Device Discovery RPC based on new OpenAPI model.

# 2.3.4
- Device Discovery RPC workaround caused by choice-nodes in UniConfig Yang models.

# 2.3.5
- Fix handling of null or empty input in the 'close_transactions' worker.

# 3.0.0
- Upgrade UniConfig API to v2.0.0 (server version 7.0.0).
- Removed temporary workaround related to unwrapping of choice nodes in the Device Discovery and Install Node RPC.
- Integrated new key-value escaping mechanism into workers.

# 3.0.1
- Fix connection-manager and discovery workers.
- Connection-manager - Pydantic cannot correctly handle combination of aliases and unions.
- Discovery - Fixed renamed elements.

# 3.0.2
- Fix missing SNMP support in connection-manager UniConfig workers.

# 3.1.0
- Add find-schema-resources RPC
