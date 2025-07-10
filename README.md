# pagopa-ecommerce-cdc-service
This repository is designed to manage and process events related to changes in ecommerce transactions event within the PagoPA ecommerce ecosystem. This service listens to events that signal changes (such as event creation) and ensures that these updates are appropriately handled in a (near) real-time manner.

- [pagopa-ecommerce-cdc-service](#pagopa-ecommerce-cdc-service)
    * [Technology Stack](#technology-stack)
    * [Start Project Locally 🚀](#start-project-locally-)
        + [Prerequisites](#prerequisites)
        + [Run docker container](#run-docker-container)
        + [Integration Testing with pagopa-ecommerce-local](#integration-testing-with-pagopa-ecommerce-local)
    * [Contributors 👥](#contributors-)
        + [Maintainers](#maintainers)

---

## Technology Stack

- Kotlin
- Spring Boot

---

## Start Project Locally 🚀

### Prerequisites

- Docker

### Populate the environment

The microservice needs a valid `.env` file in order to be run.

If you want to start the application without too much hassle, you can just copy `.env.local` with

```shell
$ cp .env.local .env
```

to get a good default configuration.

If you want to customize the application environment, reference this table:

| Variable name                        | Description                                                                                                                                                | type              | default |
|--------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------|---------|
| ROOT_LOGGING_LEVEL                   | Application root logger level                                                                                                                              | string            | INFO    |
| APP_LOGGING_LEVEL                    | it.pagopa logger level                                                                                                                                     | string            | INFO    |
| WEB_LOGGING_LEVEL                    | Web logger level                                                                                                                                           | string            | INFO    |
| MONGO_HOST                           | Host where MongoDB instance used to persist transaction data                                                                                               | hostname (string) |         |
| MONGO_PORT                           | Port where MongoDB is bound to in MongoDB host                                                                                                             | number            |         |
| MONGO_USERNAME                       | MongoDB username used to connect to the database                                                                                                           | string            |         |
| MONGO_PASSWORD                       | MongoDB password used to connect to the database                                                                                                           | string            |         |
| MONGO_SSL_ENABLED                    | Whether SSL is enabled while connecting to MongoDB                                                                                                         | string            |         |
| MONGO_DB_NAME                        | Mongo database name                                                                                                                                        | string            |         |
| MONGO_MIN_POOL_SIZE                  | Min amount of connections to be retained into connection pool. See docs *                                                                                  | string            |         |
| MONGO_MAX_POOL_SIZE                  | Max amount of connections to be retained into connection pool. See docs *                                                                                  | string            |         |
| MONGO_MAX_IDLE_TIMEOUT_MS            | Max timeout after which an idle connection is killed in milliseconds. See docs *                                                                           | string            |         |
| MONGO_CONNECTION_TIMEOUT_MS          | Max time to wait for a connection to be opened. See docs *                                                                                                 | string            |         |
| MONGO_SOCKET_TIMEOUT_MS              | Max time to wait for a command send or receive before timing out. See docs *                                                                               | string            |         |
| MONGO_SERVER_SELECTION_TIMEOUT_MS    | Max time to wait for a server to be selected while performing a communication with Mongo in milliseconds. See docs *                                       | string            |         |
| MONGO_WAITING_QUEUE_MS               | Max time a thread has to wait for a connection to be available in milliseconds. See docs *                                                                 | string            |         |
| MONGO_HEARTBEAT_FREQUENCY_MS         | Hearth beat frequency in milliseconds. This is an hello command that is sent periodically on each active connection to perform an health check. See docs * | string            |         |

(*): for Mongo connection string options see [docs](https://www.mongodb.com/docs/drivers/java/sync/v4.3/fundamentals/connection/connection-options/#connection-options)

### Run docker container

From current project directory run:

```sh
docker build -t pagopa-ecommerce-cdc-service .
docker run --env-file .env -p 8080:8080 pagopa-ecommerce-cdc-service
```

### Integration Testing with pagopa-ecommerce-local

For comprehensive integration testing with the full eCommerce ecosystem, use the [pagopa-ecommerce-local](https://github.com/pagopa/pagopa-ecommerce-local) repository.

The CDC service is integrated into the local development environment and provides:
- Complete eCommerce transaction flow
- Real-time CDC event processing  
- End-to-end testing capabilities
- Monitoring and observability tools

When running with the full environment, the CDC service connects to shared MongoDB and integrates with all other eCommerce services.

---

## Contributors 👥

Made with ❤️ by PagoPA S.p.A.

### Maintainers

See `CODEOWNERS` file
