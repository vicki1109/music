REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/jyson-1.0.2.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/json-simple.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/json-20140107.jar;

REGISTER './get_music_meta.py' USING jython AS myfuncs;
music_event_data = LOAD '$INPUT_MUSIC_EVENT_DATA' AS (wwid:chararray, songid:chararray, domain:chararray, subdomain:chararray, meta:chararray, score:double);

meta = FOREACH music_event_data GENERATE meta;
meta = DISTINCT meta;
meta = FOREACH meta GENERATE meta,  ROUND(RANDOM() * 20) AS id;

-- Parse lines using UDF
music_meta = GROUP meta BY (id%20);
result = FOREACH music_meta {
             GENERATE myfuncs.batchVerifyAndGetImageMeta(meta);
        };

flattend_result = FOREACH result GENERATE
                     FLATTEN(music_json);

output_result = FOREACH flattend_result GENERATE FLATTEN($0);

STORE output_result INTO '$OUTPUT';


