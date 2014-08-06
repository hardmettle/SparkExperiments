name := "sparktest2"

version := "1.0"

libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.10" % "1.0.1",
  "com.github.scopt" %% "scopt" % "3.2.0",
              "org.apache.spark" % "spark-mllib_2.10" % "1.0.1"
)