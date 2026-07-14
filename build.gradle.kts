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
version = "1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories {
    mavenLocal()
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

    // Lombok
    compileOnly("org.projectlombok:lombok:1.18.36")
    annotationProcessor("org.projectlombok:lombok:1.18.36")

    // Logging
    implementation("io.github.merlimat.slog:slog:0.9.5")
    implementation("org.apache.logging.log4j:log4j-api:2.24.3")
    implementation("org.apache.logging.log4j:log4j-core:2.24.3")
    implementation("org.apache.logging.log4j:log4j-slf4j2-impl:2.24.3")

    // Drivers
    implementation(platform("io.opentelemetry:opentelemetry-bom:1.63.0"))
    implementation("io.github.oxia-db:oxia-client:0.9.4-readwin")
    implementation("io.etcd:jetcd-core:0.8.6")
    implementation("org.tikv:tikv-client-java:3.3.5")
    implementation("org.apache.zookeeper:zookeeper:3.9.3")
    implementation("io.lettuce:lettuce-core:6.5.1.RELEASE")

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
        licenseHeader("/*\n" +
            " * Copyright \u00a9 \$YEAR The Oxia Authors\n" +
            " *\n" +
            " * Licensed under the Apache License, Version 2.0 (the \"License\");\n" +
            " * you may not use this file except in compliance with the License.\n" +
            " * You may obtain a copy of the License at\n" +
            " *\n" +
            " *     http://www.apache.org/licenses/LICENSE-2.0\n" +
            " *\n" +
            " * Unless required by applicable law or agreed to in writing, software\n" +
            " * distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
            " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
            " * See the License for the specific language governing permissions and\n" +
            " * limitations under the License.\n" +
            " */")
        googleJavaFormat("1.28.0")
        importOrder()
        removeUnusedImports()
        leadingSpacesToTabs(2)
        leadingTabsToSpaces(4)
        targetExclude("build/**")
    }
}

tasks.shadowJar {
    // grpc and jetcd register NameResolver/LoadBalancer providers via ServiceLoader files;
    // without merging, only one jar's META-INF/services file survives and (depending on
    // merge order) jetcd's ip:/// resolver disappears at runtime.
    mergeServiceFiles()

    // tikv-client-java is a pre-shaded fat jar that bundles its own copies of the jetcd
    // API classes (io.etcd.jetcd.*) with grpc references relocated to org.tikv.shade.*.
    // When those copies win the merge over jetcd-core's real classes, the etcd driver
    // fails at runtime (NoSuchMethodError: KVGrpc.newFutureStub(org.tikv.shade...Channel)).
    // No org.tikv class references the bundled copies (verified against 3.3.5), so take
    // tikv out of the normal merge and re-add it without its io/etcd entries.
    dependencies {
        exclude(dependency("org.tikv:tikv-client-java:.*"))
    }
    from(provider {
        project.configurations.getByName("runtimeClasspath").files
            .filter { it.name.startsWith("tikv-client-java") }
            .map { zipTree(it) }
    }) {
        exclude("io/etcd/**")
    }
}

tasks.named("build") {
    dependsOn("shadowJar")
}