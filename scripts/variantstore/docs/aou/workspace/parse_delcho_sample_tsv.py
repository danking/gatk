import argparse
import sys

def parse_delcho_tsv(input_file, output_file):
    eprint(f"Parsing: {input_file}")
    with (
        open(output_file, 'w') as output,
        open(input_file) as input,
    ):

        # Read the header
        line = input.readline()
        # write it to the output
        output.write(line)
        tokens = line.rstrip().split("\t")
        filter_columns = set()
        for i in range(0, len(tokens)):
            if tokens[i].startswith("flt_") and tokens[i] != 'flt_is_missing_from_cdr':
                filter_columns.add(i)

        # Iterate over the rest of the file
        while line := input.readline():
            tokens = line.rstrip().split("\t")
            include = True
            for i in filter_columns:
                if tokens[i] != 'False':
                    include = False
                    break
            if include:
                output.write(line)

def eprint(*the_args, **kwargs):
    print(*the_args, file=sys.stderr, **kwargs)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(allow_abbrev=False,
                                     description='A script to parse the \"delcho\" tsv file provided by Lee to generate a list of 380K samples for the Echo scale test')

    parser.add_argument('--input', type=str, help='Input tsv', required=True)
    parser.add_argument('--output', type=str, help='Output tsv', required=True)
    args = parser.parse_args()

    parse_delcho_tsv(args.input, args.output)
