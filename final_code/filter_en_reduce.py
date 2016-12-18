#!/usr/bin/python

import sys

for line in sys.stdin:
    try:
        final_line = line.strip()
        print "%s" % final_line
    except:
        pass
