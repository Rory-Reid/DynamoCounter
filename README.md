# Dynamo Counters Examples

This repository contains demonstrations of implementing auto-incrementing numbers (counters, or serials if you fancy) in different ways in [DynamoDB](https://aws.amazon.com/dynamodb/). It serves as a practical side to the suggestion "Use atomic counters or optimistic locking".

Find my post about this on [Rory Dot Horse](https://rory.horse/posts/counting-on-dynamo/).

This is a set of tests that demonstrate different ways of implementing auto-incrementing numbers in DynamoDB by integrating with a real instance. It works against the [dynamodb-local](https://hub.docker.com/r/amazon/dynamodb-local) container image and will tear the container down after the tests run. You can configure it to leave the container running if you want to inspect the state of the database with some tool after tests run (I use [dynamodb-admin](https://github.com/aaronshaf/dynamodb-admin)). See additional container [usage notes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.UsageNotes.html) in the official AWS docs.

## Pre-requisites

It's dotnet so, all the normal stuff for that, plus docker if you want to run the tests as I use [FluentDocker](https://github.com/mariotoffia/FluentDocker) to host DynamoDB locally when the tests are run.

Port 8000 must be free on your machine or you must be comfortable editing the number "8000" in two places. I'm easy.
