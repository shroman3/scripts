import os
import sys

MEGA=1048576

def generate_random_file(output_filename, megas):
    with open(output_filename, 'wb') as fout:
        fout.write(os.urandom(MEGA*megas))

    
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print 'usage: generate.py output_file megabytes'
        sys.exit(2)
    generate_random_file(sys.argv[1], int(sys.argv[2]))