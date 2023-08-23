version 1.0

import "ComparePgenAndVcf.wdl" as ComparePgenAndVcf

workflow ComparePgensAndVcfsScattered {
    input {
        Array[File] pgens
        Array[File] pvars
        Array[File] psams
        Array[File] vcfs
    }


    scatter(idx in range(length(pgens))) {
        call ComparePgenAndVcf.ComparePgensAndVcfs {
            input:
                pgens = [pgens[idx]],
                pvars = [pvars[idx]],
                psams = [psams[idx]],
                vcfs = [vcfs[idx]],
        }
    }

    call Report {
        input:
            diff_files = flatten(ComparePgensAndVcfs.diffs)
    }

    output {
        Array[File] diffs = flatten(ComparePgensAndVcfs.diffs)
        File report = Report.report
        Int count = Report.count
    }
}

# Generates a report file based on the input diff files that lists the files with differences and the count of those
# files
task Report {
    input {
        Array[File] diff_files
    }

    Int disk_in_gb = ceil(10 + size(diff_files, "GB"))

    command <<<
        touch report.txt
        DIFF_ARRAY=(~{sep=" " diff_files})
        count=0
        for diff_file in "${DIFF_ARRAY[@]}"
        do
        if [ -s ${diff_file} ]
        then
        count=$((count+1))
        echo -e "${diff_file}" >> report.txt
        fi
        done
        echo -e "${count} files with differences" >> report.txt
        touch count.txt
        echo -e "${count}" > count.txt
    >>>

    output {
        File report = "report.txt"
        Int count = read_int("count.txt")
    }

    runtime {
        docker: "ubuntu:22.04"
        memory: "3 GB"
        disks: "local-disk ${disk_in_gb} HDD"
        preemptible: 3
        cpu: 1
    }
}