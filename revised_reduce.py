#!/usr/bin/python

from operator import itemgetter
import sys

counter = {}
for line in sys.stdin:
    key, value = line.strip().split('\t')

    try:
    	counter[key] = counter.get(key, 0) + int(value)
    except ValueError:
    	pass

sorted_counter = sorted(counter.items(), key = itemgetter(1), reverse = True)

for key, value in sorted_counter:

	print '%s\t%s' % (key, value)
