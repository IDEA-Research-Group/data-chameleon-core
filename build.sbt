name := "Data-Chameleon"

version := "0.2"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"

//val url = url("http://estigia.lsi.us.es:1681/artifactory/libs-release")

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "compile"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "compile "
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.3.1" % "compile"
libraryDependencies += "joda-time" % "joda-time" % "2.10" % "compile"
// https://mvnrepository.com/artifact/com.google.code.gson/gson
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.5"
// https://mvnrepository.com/artifact/org.kitesdk/kite-data-core
libraryDependencies += "org.kitesdk" % "kite-data-core" % "1.1.0"
// https://mvnrepository.com/artifact/com.fasterxml.jackson.dataformat/jackson-dataformat-xml
libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % "2.9.8"
libraryDependencies += "javax.xml.bind" % "jaxb-api" % "2.2.6"
libraryDependencies += "Spex" % "Spex" % "1" from "http://estigia.lsi.us.es:1681/artifactory/libs-release/Spex/Spex/1/Spex-1.jar"
libraryDependencies += "OpenXES" % "OpenXES" % "20160212" from "http://estigia.lsi.us.es:1681/artifactory/libs-release/OpenXES/OpenXES/20160212/OpenXES-20160212.jar"


//resolvers += Resolver.url("Artifactory-releases", url("http://estigia.lsi.us.es:1681/artifactory/libs-release"))(Resolver.ivyStylePatterns)
//resolvers += "Artifactory-releases" at "http://estigia.lsi.us.es:1681/artifactory/libs-release"

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}