version 1.0

workflow CompareFilesWorkflow {
    input{
        Array[File] actual
        Array[File] expected
    }

    call CompareFiles {
        input:
            actual = actual,
            expected = expected
    }

    output {
        Array[File] diffs = CompareFiles.diffs
    }
}

task CompareFiles {

    input{
        Array[File] actual
        Array[File] expected
    }

    Int disk_in_gb = 2 * ceil(10 + size(actual, "GB") + size(expected, "GB"))

    command <<<
        set -e
        ACTUAL_ARRAY=(~{sep=" " actual})
        EXPECTED_ARRAY=(~{sep=" " expected})
        for i in "${!ACTUAL_ARRAY[@]}"
        do
            echo "expected: ${EXPECTED_ARRAY[$i]} , actual: ${ACTUAL_ARRAY[$i]}"
            gunzip "${EXPECTED_ARRAY[$i]}"
            OUTPUT_FILE="$(basename ${ACTUAL_ARRAY[$i]}).diff"
            java -jar -Xmx5g /comparator/pgen_vcf_comparator.jar "${ACTUAL_ARRAY[$i]}" "${EXPECTED_ARRAY[$i]%.gz}" > ${OUTPUT_FILE}
        done
    >>>

    output {
        Array[File] diffs = glob("*.diff")
    }

    runtime {
        docker: "us.gcr.io/broad-dsde-methods/klydon/pgen_vcf_comparator:test"
        memory: "6 GB"
        disks: "local-disk ${disk_in_gb} HDD"
        preemptible: 3
        cpu: 1
    }
}