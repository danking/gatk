version 1.0

workflow MergePgenWorkflow {
    input {
        Array[File] pgen_files
        Array[File] pvar_files
        Array[File] psam_files
        String plink_docker
        String output_file_base_name
        Int? threads
        Int merge_disk_size
        Int split_count
    }

    call MakeFileLists as MakeListsForSplit {
        input:
            pgen_files = pgen_files,
            psam_files = psam_files,
            pvar_files = pvar_files
    }

    call SplitFileLists {
        input:
            pgen_list = MakeListsForSplit.pgen_list,
            psam_list = MakeListsForSplit.psam_list,
            pvar_list = MakeListsForSplit.pvar_list,
            split_count = split_count
    }

    scatter(i in range(length(SplitFileLists.pgen_lists))) {
        call MergePgen as ScatterMerge {
            input:
                pgen_list = SplitFileLists.pgen_lists[i],
                psam_list = SplitFileLists.psam_lists[i],
                pvar_list = SplitFileLists.pvar_lists[i],
                plink_docker = plink_docker,
                output_file_base_name = "${output_file_base_name}${i}",
                threads = threads,
                disk_in_gb = merge_disk_size
           }
    }

    call MakeFileLists as MakeListsForFinal {
        input:
            pgen_files = ScatterMerge.pgen_file,
            psam_files = ScatterMerge.psam_file,
            pvar_files = ScatterMerge.pvar_file
    }

    call MergePgen as FinalMerge {
        input:
            pgen_list = MakeListsForFinal.pgen_list,
            psam_list = MakeListsForFinal.psam_list,
            pvar_list = MakeListsForFinal.pvar_list,
            plink_docker = plink_docker,
            output_file_base_name = output_file_base_name,
            threads = threads,
            disk_in_gb = merge_disk_size
    }

    output {
        File pgen_file = FinalMerge.pgen_file
        File pvar_file = FinalMerge.pvar_file
        File psam_file = FinalMerge.psam_file
    }
}

task MergePgen {
    input {
        File pgen_list
        File psam_list
        File pvar_list
        String plink_docker
        String output_file_base_name
        Int threads = 1
        Int disk_in_gb
    }

    Int cpu = threads + 1

    command <<<
        set -e

        # Download files using gsutil
        mkdir pgen_dir
        cat ~{pgen_list} | gsutil -m cp -I pgen_dir
        cat ~{psam_list} | gsutil -m cp -I pgen_dir
        cat ~{pvar_list} | gsutil -m cp -I pgen_dir

        # Create a file with a list of all the pgen basenames for merging
        touch mergelist.txt
        count=0
        for pgen in pgen_dir/*.pgen
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
            plink2 --pmerge-list mergelist.txt --threads ~{threads} --out ~{output_file_base_name}
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
        cpu: "${cpu}"
    }
}

task MakeFileLists {
    input {
        Array[File] pgen_files
        Array[File] pvar_files
        Array[File] psam_files
    }

    parameter_meta {
        pgen_files: {
            localization_optional: true
        }
        pvar_files: {
            localization_optional: true
        }
        psam_files: {
            localization_optional: true
        }
    }

    command <<<
        touch pgen_list.txt
        touch psam_list.txt
        touch pvar_list.txt

        PGEN_ARRAY=(~{sep=" " pgen_files})
        PSAM_ARRAY=(~{sep=" " psam_files})
        PVAR_ARRAY=(~{sep=" " pvar_files})

        for i in "${PGEN_ARRAY[@]}"
        do
            echo "${PGEN_ARRAY[$i]}" >> pgen_list.txt
            echo "${PSAM_ARRAY[$i]}" >> psam_list.txt
            echo "${PVAR_ARRAY[$i]}" >> pvar_list.txt
        done

    >>>

    output {
        File pgen_list = "pgen_list.txt"
        File psam_list = "psam_list.txt"
        File pvar_list = "pvar_list.txt"
    }

    runtime {
        docker: "ubuntu:latest"
        memory: "1GB"
        bootDiskSizeGb: 15
    }
}

task SplitFileLists {
    input {
        File pgen_list
        File psam_list
        File pvar_list

        Int split_count
    }

    command <<<
        # Get the count of files and divide by split count (rounded up) to get number of files per split list
        FILE_COUNT=$(wc -l < ~{pgen_list})
        SPLIT_LINES=$(((FILE_COUNT+~{split_count}-1)/~{split_count}))
        # Split the lists
        split -l ${SPLIT_LINES} ~{pgen_list} split_pgen_files
        split -l ${SPLIT_LINES} ~{psam_list} split_psam_files
        split -l ${SPLIT_LINES} ~{pvar_list} split_pvar_files
    >>>

    output {
        Array[File] pgen_lists = glob("split_pgen_files*")
        Array[File] psam_lists = glob("split_psam_files*")
        Array[File] pvar_lists = glob("split_pvar_files*")
    }

    runtime {
        docker: "ubuntu:latest"
        memory: "1GB"
        bootDiskSizeGb: 15
    }
}