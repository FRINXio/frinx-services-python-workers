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

# 1.2.1
- Updated inventory AddDevice and UpdateDevice settings, so they can also contain empty values.
  Useful when adding devices that have failed during the workflow.

# 1.3.0
- Bumped version of frinx-inventory-api to 2.1.0.
- Implemented new inventory workers for following mutations and query:
  AddStreamMutation, UpdateStreamMutation, DeleteStreamMutation, StreamsQuery.

# 1.4.0
- Bumped version of frinx-inventory-api to 2.2.0.
- Implemented worker for updating discovered at timestamp of selected device.

# 1.5.0
- Bumped version of frinx-inventory-api to 2.3.0.
- Implemented workers for reading, adding, updating and removing inventory locations.

# 2.0.0
- Bumped version of frinx-inventory-api to 3.0.0.
- Bumped version of frinx-python-sdk to 2.2.1.
