import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}

ThisBuild / scalaVersion := "2.13.0"

lazy val commonSettings = Seq(
  organization := "com.gubbns",
  homepage := Some(url(s"https://slakah.github.io/${name.value}/")),
  licenses += "MIT" -> url("http://opensource.org/licenses/MIT"),
  scmInfo := Some(ScmInfo(
    url(s"https://github.com/Slakah/${name.value}"),
    s"scm:git@github.com:Slakah/${name.value}.git"
  )),
  // https://scalacenter.github.io/scalafix/docs/users/installation.html
  addCompilerPlugin(scalafixSemanticdb),
  scalacOptions ++= scalacOpts :+ "-Yrangepos"
)

lazy val publishSettings = Seq(
  autoAPIMappings := true,
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  useGpg := false,
  pgpPassphrase ~= (_.orElse(sys.env.get("PGP_PASSPHRASE").map(_.toCharArray))),
  pgpPublicRing := file(s"./pubring.asc"),
  pgpSecretRing := file(s"./secring.asc"),
  apiURL := Some(url(s"https://slakah.github.io/fastparse-parsers/api/latest/${name.value}/")),
  pomExtra := {
    <developers>
      <developer>
        <id>slakah</id>
        <name>James Collier</name>
        <url>https://github.com/Slakah</url>
      </developer>
    </developers>
  }
)

lazy val betterMonadicForVersion = "0.3.1"
lazy val monixVersion = "3.0.0-RC5"
lazy val utestVersion = "0.7.1"
lazy val scalafixNoinferVersion = "0.1.0-M1"

ThisBuild / scalafixDependencies +=
  "com.eed3si9n.fix" %% "scalafix-noinfer" % scalafixNoinferVersion

ThisBuild / libraryDependencies +=
  compilerPlugin("com.olegpy" %% "better-monadic-for" % betterMonadicForVersion)

ThisBuild / releaseEarlyWith := SonatypePublisher
ThisBuild / releaseEarlyEnableLocalReleases := true
ThisBuild / organization := "com.gubbns"

publishArtifact := false

addCommandAlias("format", Seq(
  "scalafix", "scalafmt",
  "test:scalafix", "test:scalafmt"
).mkString(";", ";", ""))
addCommandAlias("lint", Seq(
  "compile:scalafix --check", "scalafmtCheck",
  "test:scalafix --check", "test:scalafmtCheck"
).mkString(";", ";", ""))
addCommandAlias("validate", Seq(
  "lint",
  "test:compile",
  "+test"
).mkString(";", ";", ""))

lazy val kinesis = crossProject(JVMPlatform)
  .in(file("modules/kafka"))
  .settings(
    commonSettings,
    publishSettings,
    name := "monix-kinesis",
    testFrameworks += new TestFramework("utest.runner.Framework"),
    doctestTestFramework := DoctestTestFramework.MicroTest,
    libraryDependencies ++= Seq(
      "io.monix" %% "monix-reactive" % monixVersion
    ) ++ Seq(
      "com.lihaoyi" %%% "utest" % utestVersion
    ).map(_ % "test")
  )

lazy val scalacOpts = Seq(
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-explaintypes", // Explain type errors in more detail.
  "-feature", // Emit warning and location for usages of features that should be imported explicitly.
  "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
  "-language:experimental.macros", // Allow macro definition (besides implementation and application)
  "-language:higherKinds", // Allow higher-kinded types
  "-language:implicitConversions", // Allow definition of implicit functions called views
  "-unchecked", // Enable additional warnings where generated code depends on assumptions.
  "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
  "-Xfatal-warnings", // Fail the compilation if there are any warnings.
  "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
  "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
  "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
  "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
  "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
  "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
  "-Xlint:nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
  "-Xlint:option-implicit", // Option.apply used implicit view.
  "-Xlint:package-object-classes", // Class or object defined in package object.
  "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
  "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
  "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
  "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
  "-Ywarn-dead-code", // Warn when dead code is identified.
  "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
  "-Ywarn-numeric-widen", // Warn when numerics are widened.
  "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
  "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
  "-Ywarn-unused:locals", // Warn if a local definition is unused.
  "-Ywarn-unused:params", // Warn if a value parameter is unused.
  "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
  "-Ywarn-unused:privates", // Warn if a private member is unused.
  "-Ywarn-value-discard", // Warn when non-Unit expression results are unused.
  "-Ybackend-parallelism", "8", // Enable paralellisation — change to desired number!
  "-Ycache-plugin-class-loader:last-modified", // Enables caching of classloaders for compiler plugins
  "-Ycache-macro-class-loader:last-modified" // and macro definitions. This can lead to performance improvements.
)

lazy val noPublishSettings = Seq(
  publish / skip := true,
  PgpKeys.publishSigned := {},
  PgpKeys.publishLocalSigned := {},
  publishArtifact := false
)
