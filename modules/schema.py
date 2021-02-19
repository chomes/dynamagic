class Schema:
    def __init__(self, data: dict):
        self.data = data
        self.valid_schema: dict = {"CustomerId": str, "name": str, "address": str, "age": str, "car": str}
        self.valid_keys: set = {"CustomerId", "name", "address", "age", "car"}
    
    def keys_validator(self, full_validate: bool = True) -> dict:
        """The method allows you to validate the keys that are parsed in to confirm that they are valid set by the schema.

        Args:
            full_validate (bool, optional): If set to false it will skip checking if all keys in the schema exist. Defaults to True.

        Returns:
            dict: status_code and message telling you if you succeeded or failed validating the data
        """
        if full_validate:
            missing_keys = list()
            for valid_key in self.valid_keys:
                if valid_key in self.data.keys():
                    pass
                else:
                    missing_keys.append(valid_key)
            if len(missing_keys) > 0:
                return {"status_code": 400, 
                "message": f"You are missing keys to validate this data, "
                "please submit a new form and try again, missing keys: {missing_keys}"}
        else:
            pass

        
        invalid_keys = list()
        for key in self.data.keys():
            if key in self.valid_keys:
                pass
            elif key not in self.valid_keys:
                invalid_keys.append(key)
        
        if len(invalid_keys) > 0:
            return {"status_code": 400,
            "message": f"You have submitted keys that doesn't fit our form try again.  Wrong keys: {invalid_keys}"}
        else:
            return {"status_code": 200, "message": "Keys Validated"}
         
    
    def data_entegrity(self) -> dict:
        """Ensures that the data is what data type it's specified to be and that certain information is correct

        Returns:
            dict: Response code saying it's correct or invalid
        """
        try:
            if len(self.data["CustomerId"]) > 10:
                return {"status_code": 400, "message": "The customer ID is longer then 10 digits long, please try again"}
            elif len(self.data["CustomerId"]) < 10:
                return {"status_code": 400, "message": "The customer ID is shorter then 10 digits long, please try again"}
        except TypeError:
            return {"status_code": 400, "message": "The customer ID cannot be an int, please make this a string"}
        
        try:
            int(self.data["CustomerId"])
        except ValueError:
            return {"status_code": 400, "message": "The CustomerId is not a number, please check this and make sure the data is correct"}


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
