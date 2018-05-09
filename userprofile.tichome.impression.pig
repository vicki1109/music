/*
 * Pig script to load user event log and user events
 * to build user's profile
 */

-- Register the JAR files
-- Site to download elephant bird jar files: http://www.java2s.com/Code/Jar/e/Downloadelephantbirdpig41jar.htm
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/elephant-bird-pig.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/elephant-bird-core.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/elephant-bird-hadoop-compat.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/json-simple.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/commons-lang3-3.0.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/hbase-common-0.96.2-hadoop2.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/hbase-client-0.96.2-hadoop2.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/hbase-protocol-0.96.2-hadoop2.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/htrace-core-2.04.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/json-20140107.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/jedis-2.7.3.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/protobuf-java-format-1.2.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/jyson-1.0.2.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/piggybank-0.15.0.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/fastjson-1.2.3.jar;
REGISTER /home/jianwang/user_profile_offline/user_profile/offline/jars/httpclient-4.5.4.jar;

REGISTER '/home/lingli/music/get_all.py' USING jython AS myfuncs;
REGISTER '/home/lingli/music/util.py' using jython as utils;

SET mapred.create.symlink yes

%default MIN_TIMESTAMP 1200000000
%default MAX_TIMESTAMP 2200000000

analytics_log = LOAD '$INPUT_LOG'
                USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad')
                ;

-- Step 1: Load user event log
analytics_items = FOREACH analytics_log
                  GENERATE
                    ($0#'product_type') AS product_type,
                    ($0#'appkey') AS appkey,
                    ($0#'event') AS event,
                    ($0#'media_type') AS media_type,
                    ($0#'dimensions') AS dimensions,
                    ($0#'properties') AS properties,
                    ($0#'other_id') AS other_id,
                    ($0#'timestamp') AS timestamp,
                    ($0#'timezone') AS timezone
                 ;

analytics_items = FILTER analytics_items
                  BY
                    product_type IS NOT NULL AND
                    appkey IS NOT NULL AND
                    media_type IS NOT NULL AND
                    dimensions IS NOT NULL AND
                    properties IS NOT NULL AND
                    other_id IS NOT NULL AND
                    timestamp IS NOT NULL
                 ;

raw_event_item = FILTER analytics_items BY
                    product_type == 'Tichome'
                    AND appkey == 'com.mobvoi.home'
                 ;

raw_event_user = FOREACH raw_event_item
                 GENERATE
                     product_type,
                     event,
                     media_type,
                     ((chararray)(other_id#'wwid')) AS user_id,
                     ((chararray)(properties#'msg_id')) AS msg_id,
                     (properties#'response') AS response,
                     utils.extractLong((chararray)timestamp) AS timestamp,
                     ((chararray)timezone) AS timezone
                 ;

-- Step 2: pre-processing
filtered_event_user = FILTER raw_event_user BY
                         ((user_id IS NOT NULL) AND (SIZE(user_id) > 0) AND (user_id != 'null'))
                         AND (event == 'music_recommend_impression')
                         AND (media_type == 'music')
                         AND response IS NOT NULL
                       --  AND timestamp > $MIN_TIMESTAMP AND timestamp < $MAX_TIMESTAMP
                      ;

-- Step 3: get (song,artist) pair and flatten
impression_event_user = FOREACH filtered_event_user
                        GENERATE
                           user_id,
                           product_type,
                           event,
                           media_type,
                           msg_id,
                           timestamp,
                           timezone,
                           myfuncs.batchVerifyAndGetSongMeta(response)
                        ;

flattened_impression_detail = FOREACH impression_event_user {
                               GENERATE
                                  user_id,
                                  product_type,
                                  event,
                                  media_type,
                                  msg_id,
                                  timestamp,
                                  timezone,
                                  FLATTEN(meta_json) AS (artist, song);
};

STORE flattened_impression_detail INTO '$OUTPUT/flattened_impression_detail';

impression_event_with_domain_tag = FOREACH flattened_result
                                   GENERATE
                                      user_id,
                                      timestamp,
                                      'tichome' AS domain,
                                      media_type AS subdomain,
                                      utils.genTichomeMusicSongId('artist', 'song', artist, song) AS name,
                                      1.0 AS score
                                    ;

STORE impression_event_with_domain_tag  INTO '$OUTPUT/impression_event_with_domain_tag';

