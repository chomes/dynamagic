# DynaMagic

DynaMagic is a biased DynamoDB module that is built to provide data validation when performing CRUD operations based on a schema for how the data should be added.

This is kept as minimal as possible due to the fact that it will be used in a lambda function so things like python-schema are not used instead I've created my own make shift one to validate the data myself to be as minimal as possible.

We recommend python 3.8 and above but it **should** run on python 3.6 at a minimum. See requirements.txt for packages (Not all of those are required)


## Notice

I am currently working on changing over to autogenerate your own schema so you do not have to change any code.  For the most part this is now done except for attribute_expression data in dynamodb_api.py
This should be finished soon but for now please use the latest stable release in version until a new version is released.


## What is included in the programme

- Tests for each method
- CRUD operations for dynamoDB

## What we aim to achieve

- Easy to use operations for using dynamoDB with error checking and validations
- Return messages that can be sent to a web server as a JSON file for users to receive feedback on successful and unsuccessful responses
- Lightweight package that can be deployed in lambda functions

## Required to run

- boto3
- time
- [schema](https://pypi.org/project/schema/)

## Required to test

- [moto](http://docs.getmoto.org/en/latest/)
- [schema](https://pypi.org/project/schema/)
- boto3
- unittest
- time

**IMPORTANT** Please note that if you plan to use this in a lambda we recommend cloning the schema module and putting it in the modules folder then un-comment these sections and delete the passes
in dynamodb_client and validation like these examples:

modules/validation.py

```python
try:
    from schema import (
        Schema,
        And,
        SchemaError,
        SchemaMissingKeyError,
        SchemaWrongKeyError,
        Use,
        Optional,
    )
except ModuleNotFoundError:
    from dynamagic.modules.schema import (
        Schema,
        And,
        SchemaError,
        SchemaMissingKeyError,
        SchemaWrongKeyError,
        Use,
        Optional,
    )
```

dynamodb_client.py

```python
try:
    from schema import Schema
except ModuleNotFoundError:
    from dynamagic.modules.schema import Schema
```

## How to use

It is recommended to use the dynamodb_client for anything to do with lambda functions.  
Even using as a python module we still would recommend the client as it would handle error checking and exceptions on your behalf however the dynamodb_api module can work just as well.

To use this module you should have your iam credentials parsed as environment variables in your venv/bin/activate file like this

```bash
export AWS_ACCESS_KEY_ID=KEY_ID
export AWS_SECRET_ACCESS_KEY=ACCESS_KEY
export AWS_DEFAULT_REGION=eu-west-2
```

Once done you then need to instantiate the class with the table and a schema to validate the data you're putting in. The schema should be a dict with key value pairs.

The keys should be the names of the attributes and the values should be the data types you want to use. See the validation module for the supported types we support.

For the table key please use the following attributes:

"key_name" - The name of the key
"key_type" - The type used for the key

An example

```python
from dynamagic.dynamodb_client.py import DynamodbClient

dynamodb_client: DynamodbClient = DynamodbClient(
dynamodb_table="Accounts", table_schema={"key_name": "CustomerId",
    "key_type": str, "name": str, "address": str, "age": str, "car": str})
```

Now you can create items on this table

```python
dynamodb_client.create_item(dynamodb_item={
                "CustomerId": "1482328791",
                "name": "James Joseph",
                "address": "Jeff Bezos Candy land road",
                "age": "32",
                "car": "Black Skoda",
            })
{"statusCode": 200,
             "body": "Created new item with key: 1482328791"}
```

For examples on how to use the modules, please view the tests for the correct format and structures of everything.

### Key vars

In the dynamodb_client.py file I have used the key name from the standard a few times, you will need to replace CustomerId with your own key otherwise this will fail.

## Raising bugs / Feature requests

See a bug in the code? Want something added? Raise an issue with the problem / use case and I'll see if I can add it to the module when possible.
