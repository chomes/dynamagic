# DynaMagic

DynaMagic is a biased DynamoDB module that is built to provide data validation when performing CRUD operations based on a schema for how the data should be added.

This is kept as minimal as possible due to the fact that it will be used in a lambda function so things like python-schema are not used instead I've created my own make shift one to validate the data myself to be as minimal as possible.

We recommend python 3.8 and above but it **should** run on python 3.6 at a minimum.  See requirements.txt for packages (Not all of those are required)


## What is included in the programme

* Tests for each method
* CRUD operations for dynamoDB
## What we aim to achieve

* Easy to use operations for using dynamoDB with error checking  and validations
* Return messages that can be sent to a web server as a JSON file for users to receive feedback on successful and unsuccessful responses
* Lightweight package that can be deployed in lambda functions

## Required to run

* boto3
* time

## Required to test

* moto - http://docs.getmoto.org/en/latest/
* boto3
* unittest
* time

## How to use

It is recommended to use the dynamodb_client for anything to do with lambda functions.  
Even using as a python module we still would recommend the client as it would handle error checking and exceptions on your behalf however the dynamodb_api module can work just as well.

To use this module you should have your iam credentials parsed as environment variables in your venv/bin/activate file like this

```bash
export AWS_ACCESS_KEY_ID=KEY_ID
export AWS_SECRET_ACCESS_KEY=ACCESS_KEY
export AWS_DEFAULT_REGION=eu-west-2
```

Once done you then need to instantiate the class

```python
from dynamagic.dynamodb_client.py import DynamodbClient

dynamodb_client: DynamodbClient = DynamodbClient(aws_region="eu-west-2",
dynamodb_table="Accounts")
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
{"status_code": 200,
             "message": "Created new item with key: 1482328791"}
```
For examples on how to use the modules, please view the tests for the correct format and structures of everything.

## Raising bugs / Feature requests

See a bug in the code?  Want something added?  Raise an issue with the problem / use case and I'll see if I can add it to the module when possible.

