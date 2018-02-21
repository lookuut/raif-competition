name := "Ru translit metaphone"

version := "0.1.0"

scalaVersion := "2.11.6"
resolvers += "bintray/meetup" at "http://dl.bintray.com/meetup/maven"

libraryDependencies += "org.specs2" %% "specs2-core" % "4.0.2" % "test"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.18.0"
libraryDependencies += "joda-time" % "joda-time" % "2.9.9"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.1"
libraryDependencies += "com.meetup" %% "archery" % "0.4.0"


scalacOptions in Test ++= Seq("-Yrangepos")
