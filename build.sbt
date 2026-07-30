name         := "jpp"
version      := "1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
)

// baselines/ and datagen/ live outside src/main/scala; compile them too.
Compile / unmanagedSourceDirectories ++= Seq(
  baseDirectory.value / "baselines",
  baseDirectory.value / "datagen"
)
