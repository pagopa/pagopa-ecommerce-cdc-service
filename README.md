# pagopa-ecommerce-cdc-service

This repository is designed to manage and process events related to changes in ecommerce transactions event within the
PagoPA ecommerce ecosystem. This service listens to events that signal changes (such as event creation) and ensures that
these updates are appropriately handled in a (near) real-time manner.

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

- **Language**: Kotlin with Java 21
- **Framework**: Spring Boot 3.5.3 with Spring WebFlux (reactive)
- **Database**: MongoDB Reactive with Change Data Capture
- **Cache/Locking**: Redis with Redisson for distributed locking
- **Build Tool**: Gradle with Kotlin DSL
- **Testing**: JUnit 5
- **Code Quality**: Spotless (formatting), SonarQube, JaCoCo (coverage)
- **Integration**: eCommerce commons library for transaction models

## Architecture Overview

The PagoPA eCommerce CDC Service implements a MongoDB Change Data Capture pattern to maintain materialized transaction views in real-time. The service architecture consists of several key components:

### Core Components

- **EcommerceTransactionsLogEventsStream**: Main CDC consumer that listens to MongoDB Change Streams for transaction events
- **EcommerceCDCEventDispatcherService**: Orchestrates event processing with retry logic and error handling
- **TransactionViewUpsertService**: Performs atomic upsert operations on transaction views with conditional logic
- **CdcLockService**: Manages distributed locking via Redis to prevent concurrent event processing
- **RedisResumePolicyService**: Handles resume tokens for fault-tolerant change stream processing

### Data Flow

1. **Event Detection**: MongoDB Change Streams detect transaction events in the `eventstore` collection
2. **Lock Acquisition**: Distributed locks ensure each event is processed by only one service instance
3. **Event Processing**: Events are dispatched and processed based on event type and chronological order
4. **View Updates**: Transaction views are updated using conditional logic based on event timestamps
5. **Resume Token Persistence**: Resume tokens are saved to Redis for fault-tolerant recovery

### Transaction View Logic

The service implements sophisticated conditional update logic:
- **Event Data Updates**: Always applied regardless of timestamp if the event being processed contains more info (enrichment data)
- **Status Updates**: Only applied if the event is newer than `lastProcessedEventAt` (prevents out-of-order overwrites)
- **Atomic Operations**: MongoDB upsert operations ensure data consistency
- **Error Handling**: Comprehensive retry mechanisms and exception handling

---

## Start Project Locally 🚀

### Prerequisites

- Docker
- GitHub personal access token with `packages:read` permission

### GitHub Token Setup

To access the `pagopa-ecommerce-commons` library from GitHub Packages, you need to set up authentication:

1. Create a GitHub personal access token with `packages:read` permission
2. Set the token as an environment variable:

```shell
export GITHUB_TOKEN=your_github_token_with_packages_read_permission
```

### Populate the environment

The microservice needs a valid `.env` file in order to be run.

If you want to start the application without too much hassle, you can just copy `.env.local` with

```shell
$ cp .env.local .env
```

to get a good default configuration.

If you want to customize the application environment, reference this table:

| Variable name                              | Description                                                                                                                                                | type              | default |
|--------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------|---------|
| ROOT_LOGGING_LEVEL                         | Application root logger level                                                                                                                              | string            | INFO    |
| APP_LOGGING_LEVEL                          | it.pagopa logger level                                                                                                                                     | string            | INFO    |
| WEB_LOGGING_LEVEL                          | Web logger level                                                                                                                                           | string            | INFO    |
| MONGO_HOST                                 | Host where MongoDB instance used to persist transaction data                                                                                               | hostname (string) |         |
| MONGO_PORT                                 | Port where MongoDB is bound to in MongoDB host                                                                                                             | number            |         |
| MONGO_USERNAME                             | MongoDB username used to connect to the database                                                                                                           | string            |         |
| MONGO_PASSWORD                             | MongoDB password used to connect to the database                                                                                                           | string            |         |
| MONGO_SSL_ENABLED                          | Whether SSL is enabled while connecting to MongoDB                                                                                                         | string            |         |
| MONGO_DB_NAME                              | Mongo database name                                                                                                                                        | string            |         |
| MONGO_MIN_POOL_SIZE                        | Min amount of connections to be retained into connection pool. See docs *                                                                                  | string            |         |
| MONGO_MAX_POOL_SIZE                        | Max amount of connections to be retained into connection pool. See docs *                                                                                  | string            |         |
| MONGO_MAX_IDLE_TIMEOUT_MS                  | Max timeout after which an idle connection is killed in milliseconds. See docs *                                                                           | string            |         |
| MONGO_CONNECTION_TIMEOUT_MS                | Max time to wait for a connection to be opened. See docs *                                                                                                 | string            |         |
| MONGO_SOCKET_TIMEOUT_MS                    | Max time to wait for a command send or receive before timing out. See docs *                                                                               | string            |         |
| MONGO_SERVER_SELECTION_TIMEOUT_MS          | Max time to wait for a server to be selected while performing a communication with Mongo in milliseconds. See docs *                                       | string            |         |
| MONGO_WAITING_QUEUE_MS                     | Max time a thread has to wait for a connection to be available in milliseconds. See docs *                                                                 | string            |         |
| MONGO_HEARTBEAT_FREQUENCY_MS               | Hearth beat frequency in milliseconds. This is an hello command that is sent periodically on each active connection to perform an health check. See docs * | string            |         |
| REDIS_HOST                                 | Redis host name                                                                                                                                            | string            |         |
| REDIS_PROTOCOL                             | Redis protocol                                                                                                                                             | string            | rediss  |
| REDIS_PASSWORD                             | Redis password                                                                                                                                             | string            |         |
| REDIS_PORT                                 | Redis port                                                                                                                                                 | string            |         |
| REDIS_SSL_ENABLED                          | Whether SSL is enabled while connecting to  Redis                                                                                                          | string            |         |
| REDIS_SUB_CONN_MIN                         | Redis subscription minimum connection number                                                                                                               | long              |         |
| REDIS_SUB_CONN_MAX                         | Redis subscription maximum connection number                                                                                                               | long              |         |
| REDIS_SLAVE_CONN_MIN                       | Redis slave minimum connection number                                                                                                                      | long              |         |
| REDIS_SLAVE_CONN_MAX                       | Redis slave maximum connection number                                                                                                                      | long              |         |
| REDIS_MASTER_CONN_MIN                      | Redis master minimum connection number                                                                                                                     | long              |         |
| REDIS_MASTER_CONN_MAX                      | Redis master maximum connection number                                                                                                                     | long              |         |
| CDC_LOG_EVENTS_COLLECTION_NAME             | The name of the collection the CDC will listen to                                                                                                          | string            |         |
| CDC_LOG_EVENTS_OPERATION_TYPE              | List of operation type the CDC will handle                                                                                                                 | list of strings   |         |
| CDC_LOG_EVENTS_PROJECT                     | The field provided by the change stream event                                                                                                              | string            |         |
| CDC_SEND_RETRY_MAX_ATTEMPTS                | Max configurable attempts for performing the logic business related to a change event                                                                      | long              | 3       |
| CDC_SEND_RETRY_INTERVAL_IN_MS              | Configurable interval in milliseconds between retries attempts                                                                                             | long              | 1000    |
| CDC_STREAM_RETRY_MAX_ATTEMPTS              | Max configurable attempts for reconnecting to DB                                                                                                           | long              | 5       |
| CDC_STREAM_RETRY_INTERVAL_IN_MS            | Configurable interval in milliseconds between retries attempts                                                                                             | long              | 5000    |
| CDC_REDIS_JOB_LOCK_KEYSPACE                | Prefix used for redis key name                                                                                                                             | string            |         |
| CDC_REDIS_JOB_LOCK_TTL_MS                  | Fallbacks in milliseconds before now in case lock is not released                                                                                          | long              |         |
| CDC_REDIS_JOB_LOCK_WAIT_TIME_MS            | Wait time in milliseconds for lock acquisition before giving up                                                                                            | long              |         |
| CDC_REDIS_RESUME_KEYSPACE                  | Prefix used for redis key name                                                                                                                             | string            |         |
| CDC_REDIS_RESUME_TARGET                    | Target used as suffix for redis key name                                                                                                                   | string            |         |
| CDC_REDIS_RESUME_FALLBACK_IN_MIN           | Fallbacks in minutes before now in case there is no resume token in cache                                                                                  | long              |         |
| CDC_REDIS_RESUME_TTL_IN_MIN                | Time to live in minutes of Redis items                                                                                                                     | long              |         |
| CDC_RESUME_SAVE_INTERVAL                   | Interval with which the CDC saves resume token                                                                                                             | int               |         |
| ECOMMERCE_TRANSACTION_VIEW_COLLECTION_NAME | eCommerce collection name that will host the transaction view documents                                                                                    | string            |         |

(*): for Mongo connection string options
see [docs](https://www.mongodb.com/docs/drivers/java/sync/v4.3/fundamentals/connection/connection-options/#connection-options)

### Run docker container

The easiest way to test the application is through a docker container connected to the dev environment. To do so follow
these steps:

1. Create a `.env.dev` file starting from `.env.local` as example and replace the MongoDB values with dev environment
   settings:
   ```sh
   cp .env.local .env.dev
   ```
   Then update the MongoDB configuration in `.env.dev` to use the dev environment values.

2. Build and run the container:
   ```sh
   export GITHUB_TOKEN=your_github_token_with_packages_read_permission
   docker build --secret id=GITHUB_TOKEN,env=GITHUB_TOKEN -t pagopa-ecommerce-cdc-service:latest .
   docker run --rm -p 8080:8080 \
     --env-file .env.dev \
     pagopa-ecommerce-cdc-service:latest
   ```

The container will listen to dev environment changes and connect to the development MongoDB instance.

### Testing the CDC Service

Once the container is running, you can test the CDC functionality:

1. **Make test payments** using the dev checkout environment at https://dev.checkout.pagopa.it/
2. **Monitor CDC logs** by checking the container logs for event processing:
   ```sh
   docker logs -f <container_id>
   ```
   Look for log lines showing CDC events being processed as transactions flow through the system.

## Project Structure

The service follows a clean architecture with clear separation of concerns:

```
src/
├── main/kotlin/it/pagopa/ecommerce/cdc/
│   ├── PagopaEcommerceCdcServiceApplication.kt         # Main application
│   ├── config/
│   │   ├── RedisConfig.kt                              # Redis configuration
│   │   └── properties/                                 # Configuration properties
│   │       ├── ChangeStreamOptionsConfig.kt
│   │       ├── RedisJobLockPolicyConfig.kt
│   │       ├── RedisResumePolicyConfig.kt
│   │       ├── RetrySendPolicyConfig.kt
│   │       └── RetryStreamPolicyConfig.kt
│   ├── datacapture/
│   │   └── EcommerceTransactionsLogEventsStream.kt     # Main CDC stream consumer
│   ├── exceptions/                                     # Custom exceptions
│   │   ├── CdcEventProcessingLockException.kt
│   │   ├── CdcEventProcessingLockNotAcquiredException.kt
│   │   ├── CdcEventTypeException.kt
│   │   └── CdcQueryMatchException.kt
│   └── services/                                       # Business logic services
│       ├── CdcLockService.kt                           # Distributed locking
│       ├── EcommerceCDCEventDispatcherService.kt       # Event processing
│       ├── RedisResumePolicyService.kt                 # Resume token management
│       ├── ResumePolicyService.kt                      # Resume policy interface
│       ├── TimestampRedisTemplate.kt                   # Redis timestamp operations
│       └── TransactionViewUpsertService.kt             # Transaction view updates
├── main/resources/
│   └── application.properties                          # Configuration
└── test/kotlin/it/pagopa/ecommerce/cdc/                # Test classes
    ├── PagopaEcommerceCdcServiceApplicationTests.kt
    ├── datacapture/
    ├── services/
    └── utils/
```

### Integration Testing with pagopa-ecommerce-local

For comprehensive integration testing with the full eCommerce ecosystem, you should use
the [pagopa-ecommerce-local](https://github.com/pagopa/pagopa-ecommerce-local) repository.

The CDC service is integrated into the local development environment and provides:

- Complete eCommerce transaction flow
- Real-time CDC event processing
- End-to-end testing capabilities
- Monitoring and observability tools

When running with the full environment, the CDC service connects to shared MongoDB and integrates with all other
eCommerce services.

### eCommerce Commons Library

The service uses the `ecommerce-commons` library which is now distributed via GitHub Packages. The library version is configured in `build.gradle.kts`:

This two properties maps `ecommerce-commons` version and git ref:

````
val ecommerceCommonsVersion = "x.y.z" -> valued with ecommerce commons wanted pom version
val ecommerceCommonsGitRef = ecommerceCommonsVersion -> the branch/tag to be checkout.
````

`ecommerceCommonsGitRef` has by default the same value as `ecommerceCommonsVersion`, so that version tagged
with `"x.y.z"` will be checked out and installed locally.

This value was left as a separate property because, during developing phases can be changed to a feature branch
making the local build use a ref branch other than a tag for developing purpose.

```kotlin
val ecommerceCommonsVersion = "3.0.2"
```

The library is automatically downloaded from GitHub Packages during the build process using the configured GitHub token.

### Dependency management 🔧

For support reproducible build this project has the following gradle feature enabled:

- [dependency lock](https://docs.gradle.org/8.1/userguide/dependency_locking.html)
- [dependency verification](https://docs.gradle.org/8.1/userguide/dependency_verification.html)

#### Dependency lock

This feature use the content of `gradle.lockfile` to check the declared dependencies against the locked one.

If a transitive dependencies have been upgraded the build will fail because of the locked version mismatch.

The following command can be used to upgrade dependency lockfile:

```shell
./gradlew dependencies --write-locks 
```

Running the above command will cause the `gradle.lockfile` to be updated against the current project dependency
configuration

#### Dependency verification

This feature is enabled by adding the gradle `./gradle/verification-metadata.xml` configuration file.

Perform checksum comparison against dependency artifact (jar files, zip, ...) and metadata (pom.xml, gradle module
metadata, ...) used during build
and the ones stored into `verification-metadata.xml` file raising error during build in case of mismatch.

The following command can be used to recalculate dependency checksum:

```shell
./gradlew --write-verification-metadata sha256 clean spotlessApply build --no-build-cache --refresh-dependencies
```

In the above command the `clean`, `spotlessApply` `build` tasks where chosen to be run
in order to discover all transitive dependencies used during build and also the ones used during
spotless apply task used to format source code.

The above command will upgrade the `verification-metadata.xml` adding all the newly discovered dependencies' checksum.
Those checksum should be checked against a trusted source to check for corrispondence with the library author published
checksum.

`/gradlew --write-verification-metadata sha256` command appends all new dependencies to the verification files but does
not remove
entries for unused dependencies.

This can make this file grow every time a dependency is upgraded.

To detect and remove old dependencies make the following steps:

1. Delete, if present, the `gradle/verification-metadata.dryrun.xml`
2. Run the gradle write-verification-metadata in dry-mode (this will generate a verification-metadata-dryrun.xml file
   leaving untouched the original verification file)
3. Compare the verification-metadata file and the verification-metadata.dryrun one checking for differences and removing
   old unused dependencies

The 1-2 steps can be performed with the following commands

```Shell
rm -f ./gradle/verification-metadata.dryrun.xml 
./gradlew --write-verification-metadata sha256 clean spotlessApply build --dry-run
```

The resulting `verification-metadata.xml` modifications must be reviewed carefully checking the generated
dependencies checksum against official websites or other secure sources.

If a dependency is not discovered during the above command execution it will lead to build errors.

You can add those dependencies manually by modifying the `verification-metadata.xml`
file adding the following component:

```xml

<verification-metadata>
    <!-- other configurations... -->
    <components>
        <!-- other components -->
        <component group="GROUP_ID" name="ARTIFACT_ID" version="VERSION">
            <artifact name="artifact-full-name.jar">
                <sha256 value="sha value"
                        origin="Description of the source of the checksum value"/>
            </artifact>
            <artifact name="artifact-pom-file.pom">
                <sha256 value="sha value"
                        origin="Description of the source of the checksum value"/>
            </artifact>
        </component>
    </components>
</verification-metadata>
```

Add those components at the end of the components list and then run the

```shell
./gradlew --write-verification-metadata sha256 clean spotlessApply build --no-build-cache --refresh-dependencies
```

that will reorder the file with the added dependencies checksum in the expected order.

Finally, you can add new dependencies both to gradle.lockfile writing verification metadata running

```shell
 ./gradlew dependencies --write-locks --write-verification-metadata sha256 --no-build-cache --refresh-dependencies
```

## Contributors 👥

Made with ❤️ by PagoPA S.p.A.

### Maintainers

See `CODEOWNERS` file
