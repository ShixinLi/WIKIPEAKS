#!/usr/bin/python

import sys

current_article = None

for line in sys.stdin:
    try:
        values_list = line.strip().split("\t")
        article = values_list[0]
        num_value = values_list[1]

        if not current_article:
            current_article = article

        if article != current_article:
            print "%s" % article
            current_article = article
    except IndexError:
        pass

print "%s" % current_article
