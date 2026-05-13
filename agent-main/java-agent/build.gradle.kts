import com.google.protobuf.gradle.id

plugins {
    `java-library`
    id("com.google.protobuf") version "0.9.4"
}

val grpcVersion = "1.64.0"
val protobufVersion = "3.25.5"
val oshiVersion = "6.6.5"
val jacksonVersion = "2.17.2"
val junitVersion = "5.13.4"

dependencies {
    api("io.grpc:grpc-netty-shaded:$grpcVersion")
    api("io.grpc:grpc-protobuf:$grpcVersion")
    api("io.grpc:grpc-stub:$grpcVersion")
    api("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("com.github.oshi:oshi-core:$oshiVersion")
    implementation("org.jetbrains.pty4j:pty4j:0.13.5")
    implementation("org.yaml:snakeyaml:2.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("org.slf4j:slf4j-simple:2.0.13")
    compileOnly("javax.annotation:javax.annotation-api:1.3.2")
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }
    generateProtoTasks {
        all().configureEach {
            plugins {
                id("grpc")
            }
        }
    }
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}
