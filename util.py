#/usr/bin/python

import hashlib
import com.xhaus.jyson.JysonCodec as json
from datetime import datetime

@outputSchema("m:map[]")
def extract_score_features(score_features_json):
   jobj = json.loads(score_features_json, accept_any_primary_datum=True)
   feat_map = {}
   num_actions = long(jobj["num_actions"])
   last_action_timestamp_second = long(jobj["last_action_timestamp_second"])
   first_action_timestamp_second = long(jobj["first_action_timestamp_second"])
   feat_map["num_actions"] = num_actions
   feat_map["last_action_timestamp_second"] = last_action_timestamp_second
   feat_map["first_action_timestamp_second"] = first_action_timestamp_second
   return feat_map

@outputSchema("tag_bag:bag{t:tuple(name:chararray,score:double)}")
def tag_json_to_bag(tag_json):
   jobj = json.loads(tag_json, accept_any_primary_datum=True)
   outBag = []
   for obj in jobj:
       key = obj["tag"]
       val = float(obj["score"])
       tup=(key, val)
       outBag.append(tup)
   return outBag


@outputSchema("hashval:long")
def strHash(input_str):
   input_utf8 = input_str.encode('utf-8')
   hasher = hashlib.sha1()
   hasher.update(input_utf8)
   return long(hasher.hexdigest(), 16) % (10 ** 8)

@outputSchema("val:long")
def extractLong(input_str):
   ret = -1L
   try:
        ret = long(float(input_str))
   except:
        ret = -1L
   return ret

@outputSchema("val:int")
def isValidAction(action):
   actions = set(['action_dial_phone',
                  'public.navigation',
                  'action_open_on_phone',
                  'expand_detail',
                  'save_to_qc'])
   if action in actions:
      return 1
   return 0

@outputSchema("val:chararray")
def extractKey(request_json, key):
    if (isinstance(request_json, dict)):
        if key in request_json:
            return request_json[key]
    else:
        jobj = json.loads(request_json, accept_any_primary_datum=True)
        if key in jobj:
           return jobj[key]
    return ''

# If user id is not empty, use user id
# Else if watch device id is not emtpy, use watch device id
# else use phone_device_id
# esoe return empty string
@outputSchema("val:chararray")
def genUserKey(userid, watch_device_id, phone_device_id):
   if userid is not None and userid.strip():
       return userid.strip()
   if watch_device_id is not None and watch_device_id.strip():
       return watch_device_id.strip()
   if phone_device_id is not None and phone_device_id.strip():
       return phone_device_id.strip()
   return ''

@outputSchema("val:chararray")
def generateText(name, category, tag):
    list = [name, category, tag]
    return ':'.join(list)

@outputSchema("val:double")
def getActionScore(action):
    score = dict({'action_dial_phone':1.0,
                  'public.navigation':0.8,
                  'action_open_on_phone':0.5,
                  'expand_detail':0.2,
                  'save_to_qc':1.0})
    return score[action]

@outputSchema("val:chararray")
def getPriceInterval(price):
    if price < 20:
        return '0-19'
    if price < 50:
        return '20-49'
    if price < 70:
        return '50-69'
    if price < 100:
        return '70-99'
    if price < 200:
        return '100-199'
    return '200+'

@outputSchema("val:tuple(chararray,chararray)")
def getHourDay(time_seconds):
    dt = datetime.fromtimestamp(time_seconds)
    hour = dt.hour
    weekday = dt.weekday()
    return (str(weekday), str(hour))

@outputSchema("val:map[]")
def convertJsonStrToDict(json_str):
    feature_map = json.loads(json_str, accept_any_primary_datum=True)
    feature_dict = {}
    for k,v in feature_map.items():
        feature_dict[k] = v
    return feature_dict

@outputSchema("val:chararray")
def convertDictToJsonStr(input):
    dumps = json.dumps(input)
    return dumps 

@outputSchema("val:chararray")
def genTichomeMusicSongId(singer_key, song_key, singer, song):
    dict = {singer_key : singer, song_key: song}
    dumps = json.dumps(dict)
    return dumps 

@outputSchema("val:boolean")
def checkJson(json_str):
    ret = True
    try:
       feature_map = json.loads(json_str, accept_any_primary_datum=True)
       if feature_map is None:
          ret = False
    except:
        ret = False
    return ret
