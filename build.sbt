name := "fup_spark"
version := "1.0"
scalaVersion := "2.12.3"

resolvers += "OSChina Maven Repository" at "http://maven.aliyun.com/nexus/content/groups/public/"
resolvers += "UK Maven Repository" at "http://uk.maven.org/maven2"
resolvers += "hortonworks Maven Repository" at "http://repo.hortonworks.com/content/repositories/releases/"
resolvers += "SparkPackages" at "http://dl.bintray.com/spark-packages/maven/"
externalResolvers := Resolver.withDefaultResolvers(resolvers.value, mavenCentral = false)

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided" excludeAll ExclusionRule(organization = "javax.servlet")
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.1"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.1"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.6.1"
libraryDependencies += "org.apache.hbase" % "hbase" % "1.1.2"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.2"  % "provided" excludeAll ExclusionRule(organization = "org.mortbay.jetty")

libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-13_2.10" % "5.0.0"

libraryDependencies += "com.lucidworks.spark" % "spark-solr" % "2.0.0-alpha-3"
libraryDependencies += "graphframes" % "graphframes" % "0.5.0-spark1.6-s_2.10"