package org.broadinstitute.hellbender.tools.gvs.extract;

import htsjdk.io.HtsPath;
import htsjdk.variant.variantcontext.*;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.ShortVariantDiscoveryProgramGroup;
import org.broadinstitute.hellbender.engine.GATKPath;
import org.broadinstitute.pgen.PgenWriter;


@SuppressWarnings("unused")
@CommandLineProgramProperties(
        summary = "(\"ExtractCohortToPgen\") - Filter and extract variants from BigQuery to a PGEN+PSAM+PVAR output.",
        oneLineSummary = "Tool to extract variants from BigQuery to a PGEN+PSAM+PVAR output for a subset of samples.",
        programGroup = ShortVariantDiscoveryProgramGroup.class
)
@DocumentedFeature
public class ExtractCohortToPgen extends ExtractCohort {
    @Argument(
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            doc = "Output PGEN file to which annotated variants should be written."
    )
    protected GATKPath outputPgenPath = null;

    @Argument(
            shortName = "wm",
            fullName = "writeMode",
            doc = "Write mode for the PGEN writer.",
            optional = true
    )
    protected PgenWriter.PgenWriteMode writeMode = PgenWriter.PgenWriteMode.PGEN_FILE_MODE_WRITE_AND_COPY;

    @Argument(
            fullName = "maxAltAlleles",
            shortName = "maa",
            doc = "Maximum alt alleles to write.",
            maxValue = PgenWriter.PLINK2_MAX_ALTERNATE_ALLELES,
            optional = true
    )
    private int maxAltAlleles = PgenWriter.PLINK2_MAX_ALTERNATE_ALLELES;

    protected PgenWriter pgenWriter = null;

    @Override
    protected void onStartup() {
        super.onStartup();

        pgenWriter = new PgenWriter(outputPgenPath, header, writeMode, PgenWriter.VARIANT_COUNT_UNKNOWN, maxAltAlleles);
    }

    @Override
    protected void apply(VariantContext variantContext) {
        if (variantContext != null) {
            // Add the variant contexts that aren't filtered or add everything if we aren't excluding anything
            if (variantContext.isNotFiltered() || !excludeFilteredSites) {
                try {
                    // Add missing genotypes if necessary
                    // TODO: talk to Chris about moving this part into the writer
                    if (variantContext.getNSamples() < header.getNGenotypeSamples()) {
                        GenotypesContext newGC = GenotypesContext.copy(variantContext.getGenotypes());
                        for(String sample : header.getGenotypeSamples()) {
                            Genotype g = variantContext.getGenotype(sample);
                            if (g == null) {
                                newGC.add(GenotypeBuilder.createMissing(sample, 2));
                            }
                        }
                        VariantContext newVC = new VariantContextBuilder(variantContext).genotypes(newGC).make();
                        pgenWriter.add(newVC);
                    }
                    else {
                        pgenWriter.add(variantContext);
                    }

                }
                catch(IllegalStateException e) {
                    logger.error("Encountered an error.  Here's some debug info:\n" +
                            "ID: " + variantContext.getID() + "\n" +
                            "NAlleles: " + variantContext.getNAlleles() + "\n" +
                            "Contig: " + variantContext.getContig() + "\n" +
                            "Start: " + variantContext.getStart() + "\n" +
                            "End: " + variantContext.getEnd() + "\n" +
                            "NoCallCount: " + variantContext.getNoCallCount() + "\n" +
                            "HomRefCount: " + variantContext.getHomRefCount() + "\n" +
                            "HetCount: " + variantContext.getHetCount() + "\n" +
                            "HomVarCount: " + variantContext.getHomVarCount() + "\n" +
                            "MixedCount: " + variantContext.getMixedCount() + "\n" +
                            "NSamples: " + variantContext.getNSamples() + "\n" +
                            "Genotypes.size(): " + variantContext.getGenotypes().size());
                    throw e;
                }
            }
            progressMeter.update(variantContext);
        }
    }

    @Override
    protected void onShutdown() {
        super.onShutdown();

        // Close up our writer if we have to:
        if (pgenWriter != null) {
            pgenWriter.close();
        }
    }
}