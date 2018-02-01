scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.4", "2.11.6")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

organization := "com.holdenkarau"

name := "spark-validator"

publishMavenStyle := true

version := "0.0.3"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

spName := "holdenk/spark-validator"

sparkVersion := "2.2.1"

sparkComponents ++= Seq("core", "sql")

parallelExecution in Test := false

// additional libraries
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.7.2" % "test")

scalacOptions ++= Seq("-deprecation", "-unchecked")

pomIncludeRepository := { x => false }

resolvers ++= Seq(
   "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
   "Spray Repository" at "http://repo.spray.cc/",
   "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
   "Akka Repository" at "http://repo.akka.io/releases/",
   "Twitter4J Repository" at "http://twitter4j.org/maven2/",
   "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
   "Twitter Maven Repo" at "http://maven.twttr.com/",
   "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
   Resolver.sonatypeRepo("public")
)

// publish settings
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

homepage := Some(url("https://github.com/holdenk/spark-validator"))

pomExtra := (
  <scm>
    <url>git@github.com:holdenk/spark-validator.git</url>
    <connection>scm:git@github.com:holdenk/spark-validator.git</connection>
  </scm>
  <developers>
    <developer>
      <id>holdenk</id>
      <name>Holden Karau</name>
      <url>http://www.holdenkarau.com</url>
      <email>holden@pigscanfly.ca</email>
    </developer>
  </developers>
)

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
