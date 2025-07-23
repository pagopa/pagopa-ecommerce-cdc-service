import org.gradle.kotlin.dsl.withType
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

group = "it.pagopa.ecommerce.cdc"

description = "pagopa-ecommerce-cdc-service"

version = "0.0.1-SNAPSHOT"

plugins {
  id("java")
  kotlin("jvm") version "2.2.0"
  kotlin("plugin.spring") version "2.2.0"
  id("org.springframework.boot") version "3.5.3"
  id("io.spring.dependency-management") version "1.1.7"
  id("org.openapi.generator") version "7.14.0"
  id("com.diffplug.spotless") version "7.1.0"
  id("org.sonarqube") version "6.2.0.5505"
  id("com.dipien.semantic-version") version "2.0.0" apply false
  jacoco
  application
}

java { toolchain { languageVersion = JavaLanguageVersion.of(21) } }

configurations { compileOnly { extendsFrom(configurations.annotationProcessor.get()) } }

repositories {
  mavenCentral()
  mavenLocal()
}

dependencyLocking { lockAllConfigurations() }

object Dependencies {
  const val ecsLoggingVersion = "1.6.0"
  const val openTelemetryVersion = "1.37.0"
  // eCommerce commons library version
  const val COMMONS_VERSION = "3.0.1-SNAPSHOT"
}

dependencies {
  implementation("org.springframework.boot:spring-boot-starter-data-mongodb-reactive")
  implementation("org.springframework.boot:spring-boot-starter-actuator")
  implementation("org.springframework.boot:spring-boot-starter-webflux")
  implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")
  implementation("org.jetbrains.kotlin:kotlin-reflect")
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
  implementation("co.elastic.logging:logback-ecs-encoder:${Dependencies.ecsLoggingVersion}")
  // otel api
  implementation("io.opentelemetry:opentelemetry-api:${Dependencies.openTelemetryVersion}")
  // eCommerce commons library
  implementation("it.pagopa:pagopa-ecommerce-commons:${Dependencies.COMMONS_VERSION}")
  compileOnly("org.projectlombok:lombok")
  annotationProcessor("org.projectlombok:lombok")
  testImplementation("org.springframework.boot:spring-boot-starter-test")
  testImplementation("io.projectreactor:reactor-test")
  testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
  testImplementation("org.mockito.kotlin:mockito-kotlin:5.2.1")
  testImplementation("it.pagopa:pagopa-ecommerce-commons:${Dependencies.COMMONS_VERSION}:tests")
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

configurations {
  implementation.configure {
    exclude(module = "spring-boot-starter-web")
    exclude("org.apache.tomcat")
    exclude(group = "org.slf4j", module = "slf4j-simple")
  }
}

kotlin { compilerOptions { freeCompilerArgs.addAll("-Xjsr305=strict") } }

tasks.withType<Test> { useJUnitPlatform() }

tasks.register<Exec>("installLibs") {
  description = "Installs the commons library for this project."
  group = "commons"
  val buildCommons = providers.gradleProperty("buildCommons")
  onlyIf("To build commons library run gradle build -PbuildCommons") { buildCommons.isPresent }
  commandLine("sh", "./pagopa-ecommerce-commons-maven-install.sh", Dependencies.COMMONS_VERSION)
}

tasks.withType<KotlinCompile> { dependsOn("installLibs") }

tasks.register("printCommonsVersion") {
  description = "Prints the referenced commons library version."
  group = "commons"
  doLast { println(Dependencies.COMMONS_VERSION) }
}

tasks
  .register("applySemanticVersionPlugin") { dependsOn("prepareKotlinBuildScriptModel") }
  .apply { apply(plugin = "com.dipien.semantic-version") }

configure<com.diffplug.gradle.spotless.SpotlessExtension> {
  kotlin {
    toggleOffOn()
    targetExclude("build/**/*")
    ktfmt().kotlinlangStyle()
  }
  kotlinGradle {
    toggleOffOn()
    targetExclude("build/**/*.kts")
    ktfmt().googleStyle()
  }
  java {
    target("**/*.java")
    targetExclude("build/**/*")
    eclipse().configFile("eclipse-style.xml")
    toggleOffOn()
    removeUnusedImports()
    trimTrailingWhitespace()
    endWithNewline()
  }
}

tasks.test {
  useJUnitPlatform()
  finalizedBy(tasks.jacocoTestReport) // report is always generated after tests run
}

tasks.jacocoTestReport {
  dependsOn(tasks.test) // tests are required to run before generating the report

  classDirectories.setFrom(
    files(
      classDirectories.files.map {
        fileTree(it).matching {
          exclude("it/pagopa/ecommerce/cdc/PagopaEcommerceCdcServiceApplicationKt.class")
        }
      }
    )
  )

  reports { xml.required.set(true) }
}

/**
 * Task used to expand application properties with build specific properties such as artifact name
 * and version
 */
val projectName = project.name
val projectVersion = project.version

tasks.processResources {
  filesMatching("application.properties") {
    expand(mapOf("project.artifactId" to projectName, "project.version" to projectVersion))
  }
}
