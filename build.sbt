import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

organization := "com.groupon.cde"

name := "VisaAssignment"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-hive" % "2.4.0"
)