#!/usr/bin/python

import sys
from discodb import DiscoDB

def read_data(instream):
    for line in instream:
        try:
            (key, value) = line.rstrip().split("\t")
            yield(key, value)
        except:
            pass

db = DiscoDB(read_data(open(sys.argv[1], 'r') if (len(sys.argv) > 1 and sys.argv[1] != '-') else sys.stdin))

db.dump(file(sys.argv[2] if len(sys.argv) > 2 else 'out.discodb', 'w'))
