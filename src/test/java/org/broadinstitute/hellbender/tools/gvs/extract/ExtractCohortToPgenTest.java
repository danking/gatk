package org.broadinstitute.hellbender.tools.gvs.extract;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.testutils.ArgumentsBuilder;
import org.broadinstitute.hellbender.testutils.IntegrationTestSpec;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;


public class ExtractCohortToPgenTest extends CommandLineProgramTest {
  private final String prefix = getToolTestDataDir();
  private final String quickstart10mbRefRangesAvroFile = prefix + "quickstart_10mb_ref_ranges.avro";
  private final String quickstart10mbVetAvroFile = prefix + "quickstart_10mb_vet.avro";
  private final String quickstartSampleListFile = prefix + "quickstart.sample.list";

  @AfterTest
  public void afterTest() {
    try {
      String [] filesToCleanUp = {"anything", "anything.idx"};
      for (String file : filesToCleanUp) {
        Files.deleteIfExists(Paths.get(file));
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed cleaning up 'anything' outputs: ", e);
    }
  }

  @Test
  public void testFinalVQSRLitePgenfromRangesAvro() throws Exception {
    // To generate the Avro input files, create a table for export using the GVS QuickStart Data
    //
    // CREATE OR REPLACE TABLE `spec-ops-aou.terra_test_1.ref_ranges_for_testing` AS
    // SELECT * FROM `spec-ops-aou.terra_test_1.ref_ranges_001`
    // WHERE location >= (20 * 1000000000000) + 10000000 - 1001 AND location <= (20 * 1000000000000) + 20000000;
    //
    // Then export in GUI w/ Avro + Snappy
    //
    // And the same for the VET data:
    // CREATE OR REPLACE TABLE `spec-ops-aou.terra_test_1.vet_for_testing` AS
    // SELECT * FROM `spec-ops-aou.terra_test_1.vet_001`
    // WHERE location >= (20 * 1000000000000) + 10000000 - 1001 AND location <= (20 * 1000000000000) + 20000000
    //
    final File expectedPgen = getTestFile("ranges_extract.expected_vqsr_lite.pgen");
    final File expectedPsam = getTestFile("ranges_extract.expected_vqsr_lite.psam");
    final File expectedPvar = getTestFile("ranges_extract.expected_vqsr_lite.pvar");

    // create temporary files (that will get cleaned up after the test has run) to hold the output data in
    final File outputPgen = createTempFile("extract_output", "pgen");
    final File outputPsam = createTempFile("extract_output", "psam");
    final File outputPvar = createTempFile("extract_output", "pvar");

    final ArgumentsBuilder args = new ArgumentsBuilder();
    args
            .add("ref-version", 38)
            .add("R", hg38Reference)
            .add("O", outputPgen.getAbsolutePath())
            .add("local-sort-max-records-in-ram", 10000000)
            .add("ref-ranges-avro-file-name", quickstart10mbRefRangesAvroFile)
            .add("vet-avro-file-name", quickstart10mbVetAvroFile)
            .add("sample-file", quickstartSampleListFile)
            .add("L", "chr20:10000000-20000000");

    runCommandLine(args);
    Assert.assertEquals(Files.mismatch(outputPgen.toPath(), expectedPgen.toPath()), -1L);
    IntegrationTestSpec.assertEqualTextFiles(outputPsam, expectedPsam);
    IntegrationTestSpec.assertEqualTextFiles(outputPvar, expectedPvar);
  }

  @Test
  public void testFinalVQSRClassicPgenfromRangesAvro() throws Exception {
    // To generate the Avro input files, create a table for export using the GVS QuickStart Data
    //
    // CREATE OR REPLACE TABLE `spec-ops-aou.terra_test_1.ref_ranges_for_testing` AS
    // SELECT * FROM `spec-ops-aou.terra_test_1.ref_ranges_001`
    // WHERE location >= (20 * 1000000000000) + 10000000 - 1001 AND location <= (20 * 1000000000000) + 20000000;
    //
    // Then export in GUI w/ Avro + Snappy
    //
    // And the same for the VET data:
    // CREATE OR REPLACE TABLE `spec-ops-aou.terra_test_1.vet_for_testing` AS
    // SELECT * FROM `spec-ops-aou.terra_test_1.vet_001`
    // WHERE location >= (20 * 1000000000000) + 10000000 - 1001 AND location <= (20 * 1000000000000) + 20000000
    //
    final File expectedPgen = getTestFile("ranges_extract.expected_vqsr_lite.pgen");
    final File expectedPsam = getTestFile("ranges_extract.expected_vqsr_lite.psam");
    final File expectedPvar = getTestFile("ranges_extract.expected_vqsr_lite.pvar");

    // create temporary files (that will get cleaned up after the test has run) to hold the output data in
    final File outputPgen = createTempFile("extract_output", "pgen");
    final File outputPsam = createTempFile("extract_output", "psam");
    final File outputPvar = createTempFile("extract_output", "pvar");

    final ArgumentsBuilder args = new ArgumentsBuilder();
    args
        .add("use-vqsr-classic-scoring", true)
        .add("ref-version", 38)
        .add("R", hg38Reference)
        .add("O", outputPgen.getAbsolutePath())
        .add("local-sort-max-records-in-ram", 10000000)
        .add("ref-ranges-avro-file-name", quickstart10mbRefRangesAvroFile)
        .add("vet-avro-file-name", quickstart10mbVetAvroFile)
        .add("sample-file", quickstartSampleListFile)
        .add("L", "chr20:10000000-20000000");

    runCommandLine(args);
    Assert.assertEquals(Files.mismatch(outputPgen.toPath(), expectedPgen.toPath()), -1L);
    IntegrationTestSpec.assertEqualTextFiles(outputPsam, expectedPsam);
    IntegrationTestSpec.assertEqualTextFiles(outputPvar, expectedPvar);
  }

  @Test(expectedExceptions = UserException.class)
  public void testThrowFilterErrorVQSRLite() throws Exception {
    final ArgumentsBuilder args = new ArgumentsBuilder();
    args
            .add("ref-version", 38)
            .add("R", hg38Reference)
            .add("O", "anything")
            .add("local-sort-max-records-in-ram", 10000000)
            .add("ref-ranges-avro-file-name", quickstart10mbRefRangesAvroFile)
            .add("vet-avro-file-name", quickstart10mbVetAvroFile)
            .add("sample-file", quickstartSampleListFile)
            .add("filter-set-info-table", "something")
            .add("filter-set-name", "something")
            .add("emit-pls", false);
    runCommandLine(args);
  }
  @Test(expectedExceptions = UserException.class)
  public void testThrowFilterErrorVQSRClassic() throws Exception {
    final ArgumentsBuilder args = new ArgumentsBuilder();
    args
        .add("use-vqsr-classic-scoring", true)
        .add("ref-version", 38)
        .add("R", hg38Reference)
        .add("O", "anything")
        .add("local-sort-max-records-in-ram", 10000000)
        .add("ref-ranges-avro-file-name", quickstart10mbRefRangesAvroFile)
        .add("vet-avro-file-name", quickstart10mbVetAvroFile)
        .add("sample-file", quickstartSampleListFile)
        .add("filter-set-info-table", "something")
        .add("filter-set-name", "something")
        .add("emit-pls", false);
    runCommandLine(args);
  }

  @Test(expectedExceptions = UserException.class)
  public void testNoFilteringThresholdsErrorVQSRLite() throws Exception {
    final ArgumentsBuilder args = new ArgumentsBuilder();
    args
            .add("ref-version", 38)
            .add("R", hg38Reference)
            .add("O", "anything")
            .add("local-sort-max-records-in-ram", 10000000)
            .add("ref-ranges-avro-file-name", quickstart10mbRefRangesAvroFile)
            .add("vet-avro-file-name", quickstart10mbVetAvroFile)
            .add("sample-file", quickstartSampleListFile)
            .add("emit-pls", false)
            .add("filter-set-info-table", "foo")
            .add("vqsr-score-filter-by-site", true);
    runCommandLine(args);
  }
  @Test(expectedExceptions = UserException.class)
  public void testNoFilteringThresholdsErrorVQSRClassic() throws Exception {
    final ArgumentsBuilder args = new ArgumentsBuilder();
    args
        .add("use-vqsr-classic-scoring", true)
        .add("ref-version", 38)
        .add("R", hg38Reference)
        .add("O", "anything")
        .add("local-sort-max-records-in-ram", 10000000)
        .add("ref-ranges-avro-file-name", quickstart10mbRefRangesAvroFile)
        .add("vet-avro-file-name", quickstart10mbVetAvroFile)
        .add("sample-file", quickstartSampleListFile)
        .add("emit-pls", false)
        .add("filter-set-info-table", "foo")
        .add("vqsr-score-filter-by-site", true);
    runCommandLine(args);
  }

  @Test(expectedExceptions = UserException.class)
  public void testFakeFilteringErrorVQSRLite() throws Exception {
    final ArgumentsBuilder args = new ArgumentsBuilder();
    // No filterSetInfoTableName included, so should throw a user error with the performSiteSpecificVQSLODFiltering flag
    args
            .add("ref-version", 38)
            .add("R", hg38Reference)
            .add("O", "anything")
            .add("local-sort-max-records-in-ram", 10000000)
            .add("ref-ranges-avro-file-name", quickstart10mbRefRangesAvroFile)
            .add("vet-avro-file-name", quickstart10mbVetAvroFile)
            .add("sample-file", quickstartSampleListFile)
            .add("emit-pls", false)
            .add("filter-set-name", "foo")
            .add("vqsr-score-filter-by-site", true);
    runCommandLine(args);
  }

  @Test(expectedExceptions = UserException.class)
  public void testFakeFilteringErrorVQSRClassic() throws Exception {
    final ArgumentsBuilder args = new ArgumentsBuilder();
    // No filterSetInfoTableName included, so should throw a user error with the performSiteSpecificVQSLODFiltering flag
    args
        .add("use-vqsr-classic-scoring", true)
        .add("ref-version", 38)
        .add("R", hg38Reference)
        .add("O", "anything")
        .add("local-sort-max-records-in-ram", 10000000)
        .add("ref-ranges-avro-file-name", quickstart10mbRefRangesAvroFile)
        .add("vet-avro-file-name", quickstart10mbVetAvroFile)
        .add("sample-file", quickstartSampleListFile)
        .add("emit-pls", false)
        .add("filter-set-name", "foo")
        .add("vqsr-score-filter-by-site", true);
    runCommandLine(args);
  }
}