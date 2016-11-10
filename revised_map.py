#!/usr/bin/python

import sys

lang_list = ['zh', 'en', 'fr', 'es']
for line in sys.stdin:
    
    values = line.strip().split(' ')

    lang = values[0]

    lang_value = lang.strip().split('.')
    if len(lang_value) > 1:
    	lang_code = lang_value[0]
    else:
    	lang_code = lang


    article = values[1]
    views = values[2]

    if lang_code in lang_list:
    	key = lang_code + '::' + article

    	print '%s\t%s' % (key,views)
    		