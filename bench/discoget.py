#!/usr/bin/python

import sys
from discodb import DiscoDB

db = DiscoDB.load(file(sys.argv[1], 'r'))

for key in map(str.rstrip, sys.stdin):
    inq = db.get(key)
    if inq and len(inq) > 0:
        print iter(inq).next()
