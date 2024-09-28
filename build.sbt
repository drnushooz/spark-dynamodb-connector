organization := "com.github.drnushooz"

name := "spark-dynamodb-connector"

version := "0.1.0-SNAPSHOT"

description := "Amazon DynamoDB connector for Apache Spark"

lazy val scala212 = "2.12.20"
lazy val scala213 = "2.13.15"

scalaVersion := scala213

val awsSdkVersion = "2.28.10"
val dynamoDbLocalVersion = "2.5.2"
val log4j2Version = "2.24.1"
val rateLimiterVersion = "2.2.0"
val scalaCollectionCompatVersion = "2.12.0"
val scalaTestVersion = "3.2.19"
val sparkVersion = "3.5.3"
val slf4jApiVersion = "2.0.16"

val dynamoDbLocalDep = (
    "com.amazonaws" % "DynamoDBLocal" % dynamoDbLocalVersion
    exclude("com.google.guava", "guava")
    exclude("org.antlr", "antlr4-runtime")
) % "test"

val dependencies = Seq(
    "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
    "io.github.resilience4j" % "resilience4j-ratelimiter" % rateLimiterVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.slf4j" % "slf4j-api" % slf4jApiVersion,
    "software.amazon.awssdk" % "sts" % awsSdkVersion,
    "software.amazon.awssdk" % "dynamodb" % awsSdkVersion
)

val testDependencies = Seq(
    dynamoDbLocalDep,
    "org.apache.logging.log4j" % "log4j-api" % log4j2Version % "test",
    "org.apache.logging.log4j" % "log4j-core" % log4j2Version % "test",
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4j2Version % "test",
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    "software.amazon.awssdk" % "url-connection-client" % awsSdkVersion % "test",
)

libraryDependencies ++= dependencies ++ testDependencies

retrieveManaged := true

scalafmtOnCompile := true

Global / onChangedBuildSource := ReloadOnSourceChanges

Compile / scalacOptions += "-deprecation"

Test / fork := true

Test / javaOptions ++= Seq(
    // To avoid IllegalArgumentException with StorageUtils in Java 17+
    "\"--add-exports java.base/sun.nio.ch=ALL-UNNAMED\""
)

val scalaMajorMinor: Def.Initialize[String] = Def.setting {
    scalaVersion.value.substring(0, scalaVersion.value.lastIndexOf('.'))
}

val outputJarName: Def.Initialize[String] = Def.setting {
    s"${name.value}_${scalaMajorMinor.value}-${version.value}.jar"
}

assembly / assemblyOption ~= { _.withIncludeScala(false).withIncludeDependency(true) }
assembly / assemblyJarName := outputJarName.value
assembly / assemblyMergeStrategy := {
    case PathList("META-INF", _ @_*) | "module-info.class" => MergeStrategy.discard
    case PathList("reference-overrides.conf") => MergeStrategy.concat
    case PathList(ps @ _*) if ps.last.endsWith(".conf") => MergeStrategy.last
    case o =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(o)
}

publishMavenStyle := true
Test / publishArtifact := false
pomIncludeRepository := { _ => false }
publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
pomExtra := <url>https://github.com/drnushooz/spark-dynamodb-connector</url>
    <licenses>
        <license>
            <name>Apache License, Version 2.0</name>
            <url>https://opensource.org/licenses/apache-2.0</url>
        </license>
    </licenses>
    <scm>
        <url>git@github.com:drnushooz/spark-dynamodb-connector.git</url>
        <connection>scm:git:git//github.com/drnushooz/spark-dynamodb-connector.git</connection>
        <developerConnection>scm:git:ssh://github.com:drnushooz/spark-dynamodb-connector.git</developerConnection>
    </scm>
