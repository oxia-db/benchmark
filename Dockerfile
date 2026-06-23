# Copyright 2025 The Oxia Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Build the (architecture-independent) jar on the native build platform so the
# Gradle build never runs under QEMU emulation; only the runtime stage is
# resolved per target architecture.
FROM --platform=$BUILDPLATFORM bellsoft/liberica-openjdk-alpine:17 AS builder
WORKDIR /app
COPY . .
RUN ./gradlew shadowJar --no-daemon

# Run on the latest Java (25). The jar is Java 17 bytecode (per the Gradle
# toolchain) and runs unchanged on the newer JVM. The flags keep the JDK 25
# runtime warning-free: Unsafe-memory access (as in bin/oxia-benchmark) plus
# native access for Netty's loadLibrary.
FROM bellsoft/liberica-openjre-alpine:25
WORKDIR /bench
COPY --from=builder /app/build/libs/*-all.jar /bench/oxia-benchmark.jar
COPY conf /bench/conf
ENTRYPOINT ["java", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "--add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED", \
    "--sun-misc-unsafe-memory-access=allow", \
    "--enable-native-access=ALL-UNNAMED", \
    "-jar", "/bench/oxia-benchmark.jar"]
