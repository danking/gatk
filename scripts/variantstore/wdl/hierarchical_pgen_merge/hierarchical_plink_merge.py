import uuid
import subprocess
import os
from threading import Thread
import math
import argparse

class ThreadWithReturnValue(Thread):
    """
    A subclass of Thread that lets you get the return value from the target function
    This code is from here: https://stackoverflow.com/a/6894023
    Which, according to StackOverflow's terms, is licensed with this: https://creativecommons.org/licenses/by-sa/4.0/
    """
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args,
                                        **self._kwargs)
    def join(self, *args):
        Thread.join(self, *args)
        return self._return


def hierarchical_merge(file_list, level, chunks):
    """
    A recursive function that splits `file_list` into the number of chunks specified by `chunks`
    and calls merge on each of the chunks.  If `level` is 1, uses plink2 to merge the files in
    `file_list`.  Otherwise, spawns a thread for each chunk and calls this function for that chunk,
    then using plink to merge the results.
    """
    # If there's only one file in the list, return that
    if len(file_list) == 1:
        return file_list[0]
    # If we're at the bottom of the pyramid, merge and return
    if level == 1:
        return plink_merge(file_list)
    # Otherwise, we'll keep splitting and then merge the results
    # Split file list into chunks
    chunk_size = math.ceil(len(file_list)/chunks)
    split_list = split(file_list, chunk_size)
    # Call hierarchical merge for each in its own thread
    threads = []
    for mergelist in split_list:
        if len(mergelist) > 0:
            threads.append(ThreadWithReturnValue(target = hierarchical_merge, args = (mergelist, level-1, chunks)))
    # Start all the threads
    for thread in threads:
        thread.start()
    # Join them all and get the ids of the merged files
    merged_file_ids = []
    for thread in threads:
        merged_file_ids.append(thread.join())
    # Now merge all these files together and return the id of the newly merged file
    merged_file_id = plink_merge(merged_file_ids)
    # Delete the intermediate files
    for file_id in merged_file_ids:
        # If there is a log file for a file_id, then it was created by a merge and is therefore an intermediate file
        # that can be deleted
        if os.path.exists(f"{file_id}.log"):
            os.remove(f"{file_id}.pgen")
            os.remove(f"{file_id}.psam")
            os.remove(f"{file_id}.pvar")
            os.remove(f"{file_id}.log")
    return merged_file_id


def plink_merge(file_list):
    """
    Runs plink2 merge on the files specified by the file basenames in `file_list` and returns
    a uuid basename for the merged file results
    """
    # Write list to mergelist file
    id_for_this_merge = str(uuid.uuid4())
    mergelist_filename = f"{id_for_this_merge}_mergelist.txt"
    with open(mergelist_filename, "wt") as mergelist:
        mergelist.writelines(file_id + '\n' for file_id in file_list)
    # Make plink merge the files
    subprocess.run(["plink2", "--silent", "--pmerge-list", mergelist_filename, "--out", id_for_this_merge])
    # Delete the mergelist file
    os.remove(mergelist_filename)
    # Return the id for this merge so we can reference the files
    return id_for_this_merge


def split(file_list, chunk_size):
    split_list = []
    for i in range(0, len(file_list), chunk_size):
        split_list.append(file_list[i: i + chunk_size])
    return split_list

def configure_cli():
    parser = argparse.ArgumentParser(
        prog='HierarchicalPgenMerge',
        description='Calls plink --pmerge-list multiple times hierarchically to merge the files in the specified list'
    )
    parser.add_argument('mergelist_file')
    parser.add_argument('-d', '--depth', help='How many tiers of merging to do', default=3)
    parser.add_argument('-w', '--width', help='How many chunks to split the file list into per tier', default=2)
    parser.add_argument('-o', '--output_basename', help='The basename of the output pgen, psam, and pvar files', default='merged')
    return parser

def main():
    cli_parser = configure_cli()
    args = cli_parser.parse_args()
    # Get the list of files from the mergelist_file
    mergelist = []
    with open(args.mergelist_file, "rt") as mergelist_file:
        # We're doing read().splitlines() here instead of readlines() because we don't want to include the newlines
        mergelist = mergelist_file.read().splitlines()
    # Merge
    merged_file_id = hierarchical_merge(mergelist, int(args.depth), int(args.width))
    # Rename merged files
    os.rename(f"{merged_file_id}.pgen", f"{args.output_basename}.pgen")
    os.rename(f"{merged_file_id}.psam", f"{args.output_basename}.psam")
    os.rename(f"{merged_file_id}.pvar", f"{args.output_basename}.pvar")
    os.remove(f"{merged_file_id}.log")

if __name__ == "__main__":
    main()