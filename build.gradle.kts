/*
 * Copyright 2025 The Oxia Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
    java
    application
    id("com.gradleup.shadow") version "9.0.0-beta12"
    id("com.diffplug.spotless") version "7.0.2"
}

group = "io.oxia"
version = "0.1.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

dependencies {
    // CLI
    implementation("info.picocli:picocli:4.7.7")
    annotationProcessor("info.picocli:picocli-codegen:4.7.7")

    // YAML config
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.18.3")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.3")

    // Latency tracking
    implementation("org.hdrhistogram:HdrHistogram:2.2.2")

    // Prometheus metrics
    implementation("io.prometheus:prometheus-metrics-core:1.3.6")
    implementation("io.prometheus:prometheus-metrics-exporter-httpserver:1.3.6")
    implementation("io.prometheus:prometheus-metrics-instrumentation-jvm:1.3.6")

    // Rate limiting
    implementation("com.google.guava:guava:33.4.0-jre")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.17")
    implementation("ch.qos.logback:logback-classic:1.5.18")

    // Drivers
    implementation("io.github.oxia-db:oxia-client:0.7.4")
    implementation("io.etcd:jetcd-core:0.8.6")
    implementation("org.apache.zookeeper:zookeeper:3.9.3")

    // Test
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testImplementation("org.assertj:assertj-core:3.27.3")
    testImplementation("org.awaitility:awaitility:4.2.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass = "io.oxia.benchmark.BenchmarkMain"
}

tasks.jar {
    manifest {
        attributes("Main-Class" to "io.oxia.benchmark.BenchmarkMain")
    }
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--add-opens", "java.base/java.lang=ALL-UNNAMED")
}

spotless {
    java {
        googleJavaFormat()
        licenseHeaderFile("license-header.txt")
    }
}

tasks.named("build") {
    dependsOn("shadowJar")
}
