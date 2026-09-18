ThisBuild / scalaVersion := "2.12.18"

ThisBuild / evictionErrorLevel := Level.Warn

lazy val root = (project in file("."))
	.settings(
		name := "UCanDo",
		version := "0.1.0",
	

		libraryDependencies ++= Seq(
			"com.softwaremill.sttp.client3" %% "core" % "3.9.8",
			"com.softwaremill.sttp.client3" %% "circe" % "3.9.8",
			"io.circe" %% "circe-generic" % "0.14.10",
			"co.fs2" %% "fs2-core" % "3.11.0",
			"co.fs2" %% "fs2-io" % "3.11.0",
			"org.typelevel" %% "cats-effect" % "3.5.7",
			"org.http4s" %% "http4s-ember-client" % "0.23.30",
			"org.http4s" %% "http4s-circe" % "0.23.30",
			"io.circe" %% "circe-generic" % "0.14.10",
			"org.apache.pekko" %% "pekko-actor" % "1.1.2",
			"org.apache.pekko" %% "pekko-stream" % "1.1.2",
			"org.apache.pekko" %% "pekko-http" % "1.1.0",
			"org.apache.pekko" %% "pekko-http-spray-json" % "1.1.0",
			"org.apache.kafka" % "kafka-clients" % "3.9.1",
			"org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2",
			"com.github.luben" % "zstd-jni" % "1.5.6-4",
			"com.github.luben" % "zstd-jni" % VersionScheme.Always,
			"org.apache.spark" %% "spark-sql" % "3.5.0"
		)
	)