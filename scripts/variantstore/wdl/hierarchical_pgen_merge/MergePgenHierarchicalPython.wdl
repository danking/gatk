version 1.0

workflow MergePgenWorkflow {
    input {
        Array[File] pgen_files
        Array[File] pvar_files
        Array[File] psam_files
        String merge_docker
        String output_file_base_name
        Int? split_width
        Int? split_depth
    }

    call MergePgen {
        input:
            pgen_files = pgen_files,
            pvar_files = pvar_files,
            psam_files = psam_files,
            merge_docker = merge_docker,
            output_file_base_name = output_file_base_name,
            width = split_width,
            depth = split_depth
    }

    output {
        File pgen_file = MergePgen.pgen_file
        File pvar_file = MergePgen.pvar_file
        File psam_file = MergePgen.psam_file
    }
}

task MergePgen {
    input {
        Array[File] pgen_files
        Array[File] pvar_files
        Array[File] psam_files
        String merge_docker
        String output_file_base_name
        Int width = 2
        Int depth = 3
    }

    Int cpu = width + 1
    Int disk_in_gb = ceil(50 + 2 * (size(pgen_files, "GB") + size(pvar_files, "GB") + size(psam_files, "GB")))

    command <<<
        set -e
        PGEN_ARRAY=(~{sep=" " pgen_files})
        touch mergelist.txt
        count=0
        for pgen in "${PGEN_ARRAY[@]}"
        do
            if [ -s ${pgen} ]
            then
                count=$((count+1))
                echo -e "${pgen%.pgen}" >> mergelist.txt
            fi
        done

        case $count in
        0)
            echo "No pgen files so creating empty ones"
            touch ~{output_file_base_name}.pgen
            touch ~{output_file_base_name}.pvar
            touch ~{output_file_base_name}.psam
            ;;
        1)
            echo "Only one pgen file so renaming the files for output"
            pgen_basename=$(cat mergelist.txt | xargs)
            mv ${pgen_basename}.pgen ~{output_file_base_name}.pgen
            mv ${pgen_basename}.psam ~{output_file_base_name}.psam
            mv ${pgen_basename}.pvar ~{output_file_base_name}.pvar
            ;;
        *)
            echo "${count} pgen files, merging"
            python3 hierarchical_plink_merge.py -d ~{depth} -w ~{width} mergelist.txt -o ~{output_file_base_name}
            ;;
        esac

    >>>

    output {
        File pgen_file = "${output_file_base_name}.pgen"
        File pvar_file = "${output_file_base_name}.pvar"
        File psam_file = "${output_file_base_name}.psam"
    }

    runtime {
        docker: "${merge_docker}"
        memory: "12 GB"
        disks: "local-disk ${disk_in_gb} HDD"
        bootDiskSizeGb: 15
        cpu: "${cpu}"
    }
}
