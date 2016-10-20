#!/usr/bin/python

import sys

for line in sys.stdin:
    
    values = line.strip().split(' ')

    lang = values[0]
    article = values[1]
    views = values[2]

    key = lang + '::' + article
    print line
    print '%s\t%s' % (key,views)
    		