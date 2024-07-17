import sbt._

// Remove some settings from scalacOptions for console: REPLs are meant to be a little bit "fast and loose"
object UnusedInConsole extends AutoPlugin {
  import Keys._

  override def requires = plugins.JvmPlugin
  override def trigger = allRequirements

  override lazy val projectSettings = Seq(
    Compile / console / scalacOptions ~= filter,
    Test / console / scalacOptions ~= filter
  )

  val excludedOptions = Set("Ywarn-unused-import", "Ywarn-unused", "Xfatal-warnings", "Xlint", "Ywarn-numeric-widen").map(p => s"-$p")
  val filter: Seq[String] => Seq[String] = _.filterNot(excludedOptions)
}
