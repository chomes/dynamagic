class Schema:
    def __init__(self, data: dict):
        self.data = data
    
    def keys_validator(self) -> dict:
        """
        Validates the keys we expect to have in a form to ensure someone doesn't inject wrong data into the table.
        """
        missing_keys = list()
        valid_keys = {"customerId", "name", "address", "age", "car"}
        for valid_key in valid_keys:
            if valid_key in self.data.keys():
                pass
            else:
                missing_keys.append(valid_key)
        if len(missing_keys) > 0:
            return {"status_code": 400, 
            "message": f"You are missing keys to validate this data, "
            "please submit a new form and try again, missing keys: {missing_keys}"}

        
        invalid_keys = list()
        for key in self.data.keys():
            if key in valid_keys:
                pass
            elif key not in valid_keys:
                invalid_keys.append(key)
        
        if len(invalid_keys) > 0:
            return {"status_code": 400,
            "message": f"You have submitted keys that doesn't fit our form try again.  Wrong keys: {invalid_keys}"}
        else:
            return {"status_code": 200, "message": "Keys Validated"}