# -*- coding: utf-8 -*-
#/usr/bin/python
import com.xhaus.jyson.JysonCodec as json
import urllib2
import urllib
import time
import traceback

BASE_URL = "http://music-parent-main/query?singers=%s&title=%s"

@outputSchema("meta_json:bag{t:tuple(artist:chararray,name:chararray)}")
def batchVerifyAndGetSongMeta(bag):
    output = []
    for item in bag:
        artist = ""
        meta_dict =  item[0]
        if 'query' in meta_dict:
            query = meta_dict['query']
        if 'name' in meta_dict:
            name = meta_dict['name']
        if query and name:
            de = u'的'
            right = de + name
            left = u'播放'
            right_query = query.rstrip(right)
            artist = right_query.lstrip(left)
            tup = (artist, name)
            output.append(tup)
            print output
    return output


