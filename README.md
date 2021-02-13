# DynaMagic

DynaMagic is a python module that is built to check integrity of data when creating, deleting, updating and reading from the dynamodb table.

This is kept as minimal as possible due to the fact that it will be used in a lambda function so things like python-schema are not used instead I've created my own make shift one to validate the data myself to be as minimal as possible.

We recommend python 3.8 and above but it **should** run on python 3.6 at a minimum.  See requirements.txt for packages (Not all of those are required)


## What is included in the programme

* Tests for each method (Although some error messages are not tested as I can't reproduce them, volunteers for options are welcome!)
* CRUD operations for dynamoDB with validations

## What we aim to achieve

* Easy to use operations for using dynamoDB with error checking  and validations
* Return messages that can be sent to a web server as a JSON file for users to receive feedback on successful and unsuccessful responses
* Lightweight package that can be deployed in lambda functions

## Required to run (That's not built in)

* boto3

## Required to test

* moto - http://docs.getmoto.org/en/latest/
* boto3
* unittest

