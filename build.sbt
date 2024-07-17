resolvers ++= Resolver.sonatypeOssRepos("public")

organization := "com.lightbend.uprotocol"

scalaVersion := Dependencies.Scala213

enablePlugins(KalixPlugin, JavaAppPackaging, DockerPlugin)

dockerBaseImage := "ghcr.io/graalvm/graalvm-community:17"
dockerUpdateLatest := false

dockerUsername := sys.props.get("docker.username")
dockerRepository := sys.props.get("docker.registry")
dockerUpdateLatest := false
dockerBuildCommand := {
  val arch = sys.props("os.arch")
  if (arch != "amd64" && !arch.contains("x86")) {
    dockerExecCommand.value ++ Seq("buildx", "build", "--platform=linux/amd64", "--load") ++ dockerBuildOptions.value :+ "."
  } else dockerBuildCommand.value
}

ThisBuild / dynverSeparator := "-"
run / fork := true
run / envVars += "HOST" -> "0.0.0.0"
run / javaOptions ++= Seq(
  "-Dlogback.configurationFile=logback-dev-mode.xml"
)

Compile / scalacOptions ++= Seq(
  "-release:11",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlog-reflective-calls",
  "-Xlint",
)
Compile / javacOptions ++= Seq(
  "-Xlint:unchecked",
  "-Xlint:deprecation",
  "-parameters",
)

libraryDependencies ++= Seq(
  Dependencies.Scalatest,
  Dependencies.Uprotocol,
  Dependencies.Scalamock,
)
