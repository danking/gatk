version 1.0

task MergePgen {
    input {
        Array[File] pgen_files
        Array[File] pvar_files
        Array[File] psam_files
        String plink_docker
        String output_file_base_name
    }

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
                echo -e -n "${pgen%.pgen}" >> mergelist.txt
                if [ $count -lt ${#PGEN_ARRAY[@]} ]
                then
                    echo -e "\n" >> mergelist.txt
                fi
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
            plink2 --pmerge-list mergelist.txt --out ~{output_file_base_name}
            ;;
        esac

    >>>

    output {
        File pgen_file = "${output_file_base_name}.pgen"
        File pvar_file = "${output_file_base_name}.pvar"
        File psam_file = "${output_file_base_name}.psam"
    }

    runtime {
        docker: "${plink_docker}"
        memory: "12 GB"
        disks: "local-disk ${disk_in_gb} HDD"
        bootDiskSizeGb: 15
        cpu: 2
    }
}
