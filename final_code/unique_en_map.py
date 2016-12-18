#!/usr/bin/python

import sys

for line in sys.stdin:

    try:

        values = line.strip().split('\t')

        if len(values) > 1:
            lan_article = values[0].split('::')
            article = lan_article[1]

            print '%s\t%s' % (article, 1)

    except IndexError:
        pass
    except:
        pass

