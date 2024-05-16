# 1.0.0
- Upgrade pydantic version to v2

# 1.0.2
- Translate zone_name from the frontend to zone_id by using the _get_zone_id method when initializing the input
- Correction of mount_parameters retyping

# 1.0.3
- Updated frinx-python-sdk to version ^1.1
- Utilize json.dumps for mount parameter serialization for InventoryAddDevice
- Update dependencies

# 1.1.0
- Added inventory update-device mutation
- Added inventory delete-device mutation

# 1.2.0
- Added 'fail_on_missing_label' boolean parameter to the INVENTORY_get_labels_id worker input.
- Controls the behavior of the worker when a label is not found.
