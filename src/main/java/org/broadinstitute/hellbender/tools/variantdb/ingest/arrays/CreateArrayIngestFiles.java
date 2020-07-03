package org.broadinstitute.hellbender.tools.variantdb.ingest.arrays;

import com.google.cloud.bigquery.TableResult;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.ShortVariantDiscoveryProgramGroup;
import org.broadinstitute.hellbender.engine.*;
import org.broadinstitute.hellbender.tools.variantdb.ChromosomeEnum;
import org.broadinstitute.hellbender.tools.variantdb.ExtractCohortBQ;
import org.broadinstitute.hellbender.tools.variantdb.ingest.IngestConstants;
import org.broadinstitute.hellbender.tools.variantdb.ingest.IngestUtils;
import org.broadinstitute.hellbender.tools.variantdb.ingest.MetadataTsvCreator;
import org.broadinstitute.hellbender.utils.bigquery.BigQueryUtils;
import org.broadinstitute.hellbender.utils.bigquery.QueryAPIRowReader;
import org.broadinstitute.hellbender.utils.bigquery.TableReference;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

/**
 * Ingest variant walker
 */
@CommandLineProgramProperties(
        summary = "Ingest tool for the Joint Genotyping in Big Query project",
        oneLineSummary = "Ingest tool for BQJG",
        programGroup = ShortVariantDiscoveryProgramGroup.class,
        omitFromCommandLine = true
)
public final class CreateArrayIngestFiles extends VariantWalker {
    static final Logger logger = LogManager.getLogger(CreateArrayIngestFiles.class);

    private ArrayMetadataTsvCreator metadataTsvCreator;
    private RawArrayTsvCreator tsvCreator;

    private String sampleName;
    private String sampleId;

    @Argument(fullName = "sample-name-mapping",
            shortName = "SNM",
            doc = "Sample name to sample id mapping",
            optional = true)
    public File sampleMap;

    @Argument(fullName = "sample-id",
            shortName = "SID",
            doc = "Sample id",
            optional = true)
    public Long sampleIdParam;

    @Argument(fullName = "probe-info",
            shortName = "PI",
            doc = "Fully qualified table name for the probe info table",
            optional = true)
    public String probeFQTablename;

    @Argument(
        fullName = "probe-info-csv",
        doc = "Filepath to CSV export of probe-info table",
        optional = true)
    private String probeCsvExportFile = null;

    @Argument(
            fullName = "ref-version",
            doc = "Remove this option!!!! only for ease of testing. Valid options are 37 or 38",
            optional = true)
    private String refVersion = "37";


    @Override
    public void onTraversalStart() {

        // Get sample name
        final VCFHeader inputVCFHeader = getHeaderForVariants();
        sampleName = IngestUtils.getSampleName(inputVCFHeader);
	if (sampleIdParam == null && sampleMap == null) {
            throw new IllegalArgumentException("One of sample-id or sample-name-mapping must be specified");
        }
	if (sampleIdParam != null) {
            sampleId = String.valueOf(sampleIdParam);
        } else {
            sampleId = IngestUtils.getSampleId(sampleName, sampleMap);
        }

        // Mod the sample directories
        int sampleTableNumber = IngestUtils.getTableNumber(sampleId, IngestConstants.partitionPerTable);
        String tableNumberPrefix = String.format("%03d_", sampleTableNumber);

        metadataTsvCreator = new ArrayMetadataTsvCreator();
        metadataTsvCreator.createRow(sampleName, sampleId, tableNumberPrefix);

        Map<String, ProbeInfo> probeNameMap;
        if (probeCsvExportFile == null) {
            probeNameMap = ExtractCohortBQ.getProbeNameMap(probeFQTablename, false);
        } else {
            probeNameMap = ProbeInfo.getProbeNameMap(probeCsvExportFile);
        }

        // TableReference probeInfoTable = new TableReference(probeFQTablename, ProbeInfoSchema.PROBE_INFO_FIELDS);
        // String q = "SELECT " + StringUtils.join(ProbeInfoSchema.PROBE_INFO_FOR_INGEST_FIELDS,",") + " FROM " + probeInfoTable.getFQTableName();
        // TableResult tr = BigQueryUtils.executeQuery(BigQueryUtils.getBigQueryEndPoint(), probeInfoTable.tableProject, probeInfoTable.tableDataset, q);

        // Map<String, ProbeInfo> probeData = ProbeInfo.createProbeDataForIngest(new QueryAPIRowReader(tr));
        // Set reference version
        ChromosomeEnum.setRefVersion(refVersion);

        tsvCreator = new RawArrayTsvCreator(sampleName, sampleId, tableNumberPrefix, probeNameMap);
    }


    @Override
    public void apply(final VariantContext variant, final ReadsContext readsContext, final ReferenceContext referenceContext, final FeatureContext featureContext) {
        tsvCreator.apply(variant, readsContext, referenceContext, featureContext);
    }

    @Override
    public Object onTraversalSuccess() {
        return 0;
    }

    @Override
    public void closeTool() {
        if (tsvCreator == null) {
            logger.info("tsvCreator is null when closing tool");
        }
        if (tsvCreator != null) {
            tsvCreator.closeTool();
        }
        if (metadataTsvCreator != null) {
            metadataTsvCreator.closeTool();
        }
    }
}