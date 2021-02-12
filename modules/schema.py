class Schema:
    def __init__(self, data: dict):
        self.data = data
        self.valid_schema: dict = {"customerId": str, "name": str, "address": str, "age": str, "car": str}
    
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

    def data_entegrity(self) -> dict:
        """
        Validates that the user passes is correct to the standard we require in the schema
        """
        try:
            if len(self.data["customerId"]) > 10:
                return {"status_code": 400, "message": "The customer ID is longer then 10 digits long, please try again"}
            elif len(self.data["customerId"]) < 10:
                return {"status_code": 400, "message": "The customer ID is shorter then 10 digits long, please try again"}
        except TypeError:
            return {"status_code": 400, "message": "The customer ID cannot be an int, please make this a string"}
        
        invalid_types = list()
        for key in self.data.keys():
            if isinstance(self.data[key], self.valid_schema[key]):
                pass
            else:
                invalid_types.append(key)
        
        if len(invalid_types) > 0:
            return {"status_code": 400, 
            "message": f"You have made the following data incorrect data types: {invalid_types} please change them "
            f"and try again.  Here is the correct data type standard for the schema {self.valid_schema}"}
        else:
            return {"status_code": 200, "message": "Data has been validated"}
