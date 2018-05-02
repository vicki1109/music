# -*- coding: utf-8 -*-
#/usr/bin/python
import com.xhaus.jyson.JysonCodec as json
import urllib2
import urllib
import time
import traceback

BASE_URL = "http://music-parent-main/query?singers=%s&title=%s"

@outputSchema("music_json:bag{t:tuple(name:chararray)}")
def batchVerifyAndGetImageMeta(bag):
    output = []
    for item in bag:
        artist_song_str = item[0]
        artist_song = json.loads(artist_song_str)
        outJson = verifyAndGetMetaData(artist_song)
        if outJson is None:
            continue
        tup = (outJson)
        output.append(tup)
    return output

def verifyAndGetMetaData(artist_song):
    if not artist_song:
        return
    singers = artist_song['artist']
    song = artist_song['song']
    singers = singers.strip()
    song = song.strip()
    esinger = urllib.quote(singers.encode('utf-8'))
    esong = urllib.quote(song.encode('utf-8'))

    # 验证音乐是否有效
    success = 0
    valid = False
    query = u'播放' + singers + u'的' + song
    valid_url = ''
    try:
        valid_url = 'http://m.mobvoi.com/search/qa/?output=lite&appkey=com.mobvoi.home&query=' + urllib.quote(
            query.encode('utf-8'))
        response = urllib2.urlopen(valid_url)
        data = response.read()
        line_dict = json.loads(data)
        if 'clientAction' in line_dict:
            domain = line_dict['domain']
            clientActionDict = line_dict['clientAction']
            if len(clientActionDict) == 0:
                return
            action = clientActionDict['action']
            if 'public.music' == domain and 'com.mobvoi.semantic.action.TENCENT.MEDIA.PLAY' == action:
                valid = True
    except Exception, e:
        traceback.print_exc()  
        return
    if not valid:
        return

    # 调用url获取结果
    url = BASE_URL % (esinger, esong)
    request = urllib2.Request(url=url)
    response = urllib2.urlopen(request)
    content = response.read()
    resp = json.loads(content)
    if len(resp) == 0:
        return
    if not 'content' in resp: return
    items = resp['content']
    imageUrl = ''
    for item in items:
        item = json.dumps(item, ensure_ascii=False)
        item = item.decode('utf-8')
        item = json.loads(item)
        title = item.get('title', '')
        if title == song:
            imageUrl = item.get('image', '')
            meta = item
            break
    if imageUrl:
        success += 1
    else:
        return
    timeStamp = long(time.time())
    albumObj = {}
    albumObj["artist"] = singers
    albumObj["song"] = song
    albumObj["meta"] = meta
    albumObj["image"] = imageUrl
    albumObj["query"] = query
    albumObj["data_gen_time"] = timeStamp

    outJson = json.dumps(albumObj, ensure_ascii=False)
    return outJson
