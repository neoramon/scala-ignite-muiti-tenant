val cacheApi 		  =	"javax.cache" 			  % 	"cache-api" 	%	"1.0.0"
val igniteCore 		=	"org.apache.ignite" 	% 	"ignite-core" 	%	"1.5.0.final"


lazy val commonSettings = Seq(
  organization := "com.neogrid.poc.cache",
  version := "0.1.0",
  scalaVersion := "2.11.6"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "scala-ignite-muiti-tenant",
    libraryDependencies ++= Seq(
	    cacheApi,
	    igniteCore
    )
  )