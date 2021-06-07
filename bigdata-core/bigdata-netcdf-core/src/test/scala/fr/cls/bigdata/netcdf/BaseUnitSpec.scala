package fr.cls.bigdata.netcdf

import org.scalamock.scalatest.MockFactory
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, TableDrivenPropertyChecks}
import org.scalatest.{FunSpec, Matchers}

abstract class BaseUnitSpec extends FunSpec with Matchers
  with TableDrivenPropertyChecks with GeneratorDrivenPropertyChecks
  with MockFactory

