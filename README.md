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

### Run docker container

Create your environment:

```sh
cp .env.local .env
```

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
