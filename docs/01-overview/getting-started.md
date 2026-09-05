# Getting started

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

This guide runs the demo first, then shows the minimum integration in a Spring Boot application.
Mohs requires Java 25, a `DataSource` and a schema installed before the application starts.

## Run the demo from this repository

```bash
./mvnw -pl mohs-demo -am install -DskipTests
./mvnw -pl mohs-demo spring-boot:run \
  -Dspring-boot.run.arguments="--mohs.api.enabled=true --spring.datasource.hikari.connection-timeout=3000"
```

Open `http://localhost:8080/mohs-ui`. The demo uses embedded H2 and enables the operational API and
dashboard for local exploration. H2 is a development dialect; use PostgreSQL, MySQL 8 or SQL Server
for production.

## Add Mohs to an application

Import the BOM and add the starter plus the driver for your database:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.github.robsonkades</groupId>
      <artifactId>mohs-bom</artifactId>
      <version>${mohs.version}</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>io.github.robsonkades</groupId>
    <artifactId>mohs-spring-boot-starter</artifactId>
  </dependency>
  <dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <scope>runtime</scope>
  </dependency>
</dependencies>
```

For an unreleased checkout, run `./mvnw install -DskipTests` in this repository first and use its
project version. Published consumers should use a released version.

Configure the dialect explicitly. Mohs deliberately does not infer it from the JDBC URL:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/app
    username: app
    password: ${DB_PASSWORD}
    hikari:
      connection-timeout: 3000

mohs:
  jdbc:
    dialect: postgresql
```

The real Hikari `connection-timeout` must be shorter than `mohs.engine.node-lease-ttl`, including
when the `DataSource` is wrapped by a Spring delegate.

## Install the schema

Mohs never runs DDL. Apply the matching `schema-<dialect>.sql` from the
`mohs-store-jdbc` artifact before starting the application. For PostgreSQL:

```bash
psql -U app -d app -f schema-postgresql.sql
```

Existing installations apply the ordered `V*.sql` deltas. See
[installing and upgrading the schema](../06-data/migrations.md) before an upgrade.

## Declare and invoke a job

```java
package com.example.jobs;

import org.springframework.stereotype.Component;
import io.mohs.core.definition.OnDemandJob;

@Component
final class Reports {

    @OnDemandJob(id = "generate-report", retries = 2, timeout = "PT5M")
    void generate(ReportRequest request) {
        // perform idempotent work
    }
}
```

Schedule it through the public facade:

```java
import io.mohs.core.Mohs;

var receipt = mohs.schedule("generate-report", new ReportRequest("monthly"))
        .as("billing-service")
        .idempotencyKey("monthly-2026-09")
        .now();
```

Handlers should tolerate redelivery because execution is at least once when retries are available.
The idempotency key prevents duplicate creation for the same job and key; it does not replace
idempotent side effects inside the handler.

## Enable operations endpoints only when protected

The starter supplies the REST integration. Add `mohs-ui` for the dashboard, then opt in:

```yaml
mohs:
  api:
    enabled: true
```

Both are served by the host application and provide no authentication or authorization themselves.
Put `/api/mohs/**`, `/mohs-ui` and `/mohs-ui/**` behind the host security configuration. Continue
with the [security guide](../08-security/security-overview.md),
[configuration reference](../07-configuration/configuration-reference.md) and
[dashboard guide](../13-operations/dashboard.md).
