class Schema:
    def __init__(self, data: dict):
        self.data = data
    
    def keys_validator(self) -> dict:
        """
        Validates the keys we expect to have exist
        """
        missing_keys = list()
        total_keys = 0
        valid_keys = ["customerId", "name", "address", "age", "car"]
        for key in self.data.keys():
            if key in valid_keys:
                total_keys += 1
                pass
            elif not key in valid_keys:
                missing_keys




def item_validator(context: dict):
    """
    Validates the put item based on the schema we want to ensure we have a standard when deploying devices.
    """

