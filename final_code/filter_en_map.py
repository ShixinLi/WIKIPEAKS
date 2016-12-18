#!/usr/bin/python

import sys

lang_list = ['en']
for line in sys.stdin:
    try:
        values = line.strip().split('::')

        if len(values) > 1:
            lang_code = values[0]

            if lang_code == 'en':
                try:
                    article = values[1].split('\t')[0]
                    article.decode('ascii')

                    print "%s" % line.strip()
                except UnicodeDecodeError:
                    pass
    except IndexError:
        pass
