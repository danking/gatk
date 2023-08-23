version 1.0

import "ComparePgenAndVcf.wdl" as ComparePgenAndVcf

workflow ComparePgensAndVcfsScattered {
    input {
        Array[File] pgens
        Array[File] pvars
        Array[File] psams
        Array[File] vcfs

        Int chunk_length = 20
    }

    call ChunkArray as pgen_chunk {
        input:
            array_to_chunk = pgens,
            chunk_length = chunk_length
    }

    call ChunkArray as pvar_chunk {
        input:
            array_to_chunk = pvars,
            chunk_length = chunk_length
    }

    call ChunkArray as psam_chunk {
        input:
            array_to_chunk = psams,
            chunk_length = chunk_length
    }

    call ChunkArray as vcf_chunk {
        input:
            array_to_chunk = vcfs,
            chunk_length = chunk_length
    }


    scatter(idx in range(length(pgen_chunk.array_chunks))) {
        call ComparePgenAndVcf.ComparePgensAndVcfs {
            input:
                pgens = read_lines(pgen_chunk.array_chunks[idx]),
                pvars = read_lines(pvar_chunk.array_chunks[idx]),
                psams = read_lines(psam_chunk.array_chunks[idx]),
                vcfs = read_lines(vcf_chunk.array_chunks[idx]),
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

# Splits the supplied array into an Array of files containing chunk_length lines each of elements from the initial
# array
task ChunkArray {
    input {
        Array[String] array_to_chunk
        Int chunk_length
    }

    command <<<
        echo "~{sep='\n' array_to_chunk}" > file_to_split.txt
        split -a 5 -d --additional-suffix=".txt" -l ~{chunk_length} file_to_split.txt chunk_
    >>>

    output {
        Array[File] array_chunks = glob("chunk_*")
    }

    runtime {
        docker: "ubuntu:22.04"
        memory: "3 GB"
        disks: "local-disk 10 HDD"
        preemptible: 3
        cpu: 1
    }
}