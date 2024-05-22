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
  in the specified workflow (rollback_transactions).
