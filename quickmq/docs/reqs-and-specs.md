# Requirements and Specifications

Basic reqs and specs document for the library.

## Previous Attempts and Issues

When building QuickMQ previously I overprioritized making the software general enough for anybody to use, instead of just the SSEC.

## Customers

The customers for this library will soley be the SSEC, and in specific Rick/Jerry.

## User Requirements

1. Callable by Python mainly, bash possibly.
2. Publish messages to RabbitMQ exchange.
3. Publish to multiple servers at a time.
4. Handles loss of network for arbitrary amount of time.
5. Handles RabbitMQ server restart.
6. Handles lack of data for arbitrary amount of time, resumes without failure.
7. Is able to handle SSEC AMQP message/topic definitions.
8. Installable through pip.
9. Handles switching server DNS aliases.

## Use Cases & User Stories

- As a user of the library, I want to be able to publish messages to a RabbitMQ exchange on remote servers.
- As a user of the library, I want to be able to publish messages to multiple servers at once.
- As a user of the library, I want to install the library from the python package index using pip.
- As a user of the library, I want to import it into my python program.
- As a user of the library at the SSEC, I want the library to support the SSEC's AMQP message/topic standards.
- As a user of the library, I want connection drops, server restarts, and DNS changes to not affect my code.
- As a user of the library, I want to be able to see the status of published messages to each server.
- As a user of the library, I want to be able to tune the verbosity of the logs of the library.
- As a developer of quickq, I don't want log messages to stdout/files, instead pass them to calling script.
- As a user of the library, I want to know when a connection is established successfully.
- As a user of the library, I want to know when connections to servers are reconnecting.

## Security Requirements

1. Don't expose passwords in logs or stacktraces.

## System Requirements

1. Linux systems.
2. Python 3.6 or up.

## Specification

### Tech Stack

### Class Diagram

## Standards and Conventions

Please see [CONTRIBUTING](/CONTRIBUTING).
