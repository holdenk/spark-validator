name := "spark_validator"

version := "0.0.1"

scalaVersion := "2.10.3"

// protocol buffer support
seq(sbtprotobuf.ProtobufPlugin.protobufSettings: _*)

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.0.0"
)

resolvers ++= Seq(
   "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
   "Spray Repository" at "http://repo.spray.cc/",
   "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
   "Akka Repository" at "http://repo.akka.io/releases/",
   "Twitter4J Repository" at "http://twitter4j.org/maven2/",
   "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
   "Twitter Maven Repo" at "http://maven.twttr.com/",
   "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools"
)
