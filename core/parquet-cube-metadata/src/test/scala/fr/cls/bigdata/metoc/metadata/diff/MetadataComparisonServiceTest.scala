package fr.cls.bigdata.metoc.metadata.diff

import fr.cls.bigdata.metoc.metadata.UnitTestData
import org.scalatest.{FunSpec, Matchers}

class MetadataComparisonServiceTest extends FunSpec with Matchers  with UnitTestData {

  describe("MetadataComparisonService") {

    describe(".compare()") {
      val rootDataset = Dataset.metadata

      it("should return no changes for identical metadata") {
        // perform
        val diff = MetadataComparisonService.compare(rootDataset, rootDataset)

        // validate
        diff.nonBreakingChanges shouldBe 'empty
        diff.breakingChanges shouldBe 'empty
      }

      describe("for dimensions") {
        testCase("on left only dimensions")(UnitTestDiffData.Cases.Dimension.OnlyLeftChangeCase)

        testCase("on right only dimensions")(UnitTestDiffData.Cases.Dimension.OnlyRightChangeCase)

        testCase("on data type change")(UnitTestDiffData.Cases.Dimension.TypeChangeCase)

        describe("for dimension attributes") {
          testCase("on left only attributes")(UnitTestDiffData.Cases.Dimension.Attributes.OnlyLeftChangeCase)

          testCase("on right only attributes")(UnitTestDiffData.Cases.Dimension.Attributes.OnlyRightChangeCase)

          testCase("on attributes type change")(UnitTestDiffData.Cases.Dimension.Attributes.TypeChangeCase)

          testCase("on attributes value change")(UnitTestDiffData.Cases.Dimension.Attributes.ValuesChangeCase)

        }
      }

      describe("for variables") {

        testCase("on left only variables")(UnitTestDiffData.Cases.Variables.OnlyLeftChangeCase)

        testCase("on right only variables")(UnitTestDiffData.Cases.Variables.OnlyRightChangeCase)

        testCase("on data type change")(UnitTestDiffData.Cases.Variables.TypeChangeCase)

        testCase("on dependent dimensions change")(UnitTestDiffData.Cases.Variables.DependentDimensionsChangeCase)


        describe("for dimension attributes") {
          testCase("on left only attributes")(UnitTestDiffData.Cases.Variables.Attributes.OnlyLeftChangeCase)

          testCase("on right only attributes")(UnitTestDiffData.Cases.Variables.Attributes.OnlyRightChangeCase)

          testCase("on attributes type change")(UnitTestDiffData.Cases.Variables.Attributes.TypeChangeCase)

          testCase("on attributes value change")(UnitTestDiffData.Cases.Variables.Attributes.ValuesChangeCase)

        }
      }

      describe("for dataset attributes") {
        testCase("on left only attributes")(UnitTestDiffData.Cases.DatasetAttributes.OnlyLeftChangeCase)

        testCase("on right only attributes")(UnitTestDiffData.Cases.DatasetAttributes.OnlyRightChangeCase)

        testCase("on attributes type change")(UnitTestDiffData.Cases.DatasetAttributes.TypeChangeCase)

        testCase("on attributes value change")(UnitTestDiffData.Cases.DatasetAttributes.ValuesChangeCase)
      }
    }
  }


  private def testCase(description: String)(diffCase: UnitTestDiffData.ChangeCase): Unit = {
    describe(description) {
      it("should detect it") {
        // prepare
        val metadata1 = diffCase.metadata1
        val metadata2 = diffCase.metadata2

        // perform
        val diff = MetadataComparisonService.compare(metadata1, metadata2)

        // validate
        diff.nonBreakingChanges should contain theSameElementsAs diffCase.changes.collect {
          case x: MetadataChange.NonBreakingChange => x
        }

        diff.breakingChanges should contain theSameElementsAs diffCase.changes.collect {
          case x: MetadataChange.BreakingChange => x
        }
      }

      diffCase match {
        case nonBreakingChangeCase: UnitTestDiffData.NonBreakingChangeCase =>
          it("should infer common root from the left side") {
            // prepare
            val originalMetadata = diffCase.metadata1
            val changes = nonBreakingChangeCase.changes.collect {
              case x: MetadataChange.NonBreakingChange => x
            }
            val expectedCommonRoot = nonBreakingChangeCase.commonRoot

            // perform
            val commonRoot = MetadataComparisonService.inferCommonRoot(originalMetadata, changes)

            // validate
            commonRoot shouldBe expectedCommonRoot
          }

          it("should infer common root from the right side") {
            // prepare
            val originalMetadata = diffCase.metadata2
            val changes = nonBreakingChangeCase.changes.collect {
              case x: MetadataChange.NonBreakingChange => x
            }
            val expectedCommonRoot = nonBreakingChangeCase.commonRoot

            // perform
            val commonRoot = MetadataComparisonService.inferCommonRoot(originalMetadata, changes)

            // validate
            commonRoot shouldBe expectedCommonRoot
          }

        case _ =>
      }
    }
  }
}
