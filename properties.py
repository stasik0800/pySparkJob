import config as c


class Quries:
    # prepare valied dataSet for analytics
    prepareData = {"name": "df",
                   "path": None,
                   "query": f"select *,getPattern(url) as pattern   from {c.baseTblName}  where isNumeric(user_id) <> 'False'"}

    # output invalid records to log
    invalidData = {"name": "invalidData" + c.csvFormat,
                   "path": c.errorlogOut,
                   "query": f"select *  from {c.baseTblName}  where isNumeric(user_id) = 'False' "}

    # answer to questions
    q1 = {"name": "Top_ten_users_convert_the_most" + c.csvFormat,
          "path": c.resultsOut,
          "query": f""" select user_id,count(*) as conversion_count
                      from {c.analyticsDf} where type ='conversion'
                      group by 1
                      order by 2 desc
                      limit 10 """}

    q2 = {"name": "fast_converting_users" + c.csvFormat,
          "path": c.resultsOut,
          "query": f"""
            -- Prepare the data combine timestamp of events 
            WITH dataset as (      
                    select  start_events.user_id,conversion_ts,start_ts,conversion_ts - start_ts as diff
                        from (
                             select user_id,min(to_timestamp(timestamp)) as start_ts,sessionid from {c.analyticsDf}
                             where type='start_session'
                             group by user_id,sessionid
                        )start_events
                        join (
                                select user_id,min(to_timestamp(timestamp)) as conversion_ts,sessionid from {c.analyticsDf}
                                where  type='conversion'
                                group by user_id,sessionid
                        ) latest_conv
                        on start_events.user_id = latest_conv.user_id and start_events.sessionid = latest_conv.sessionid
                  )

                  select df.user_id,count(*) as distance from {c.analyticsDf} as df
                    join (  
                        -- !!! unique records min diff over dataset !!!
                            select dataset.user_id,start_ts,conversion_ts from dataset 
                            join (select min(diff) as mdiff,user_id  from dataset group by user_id) sub 
                            on dataset.diff = sub.mdiff and  dataset.user_id = sub.user_id 

                      ) tb on df.user_id = tb.user_id and to_timestamp(df.timestamp) between tb.start_ts and tb.conversion_ts
                  where type not in ('conversion','start_session')
                  group by 1
                  order by 2 desc  """}


    q3 = {"name": "average_converting_distance" + c.csvFormat,
          "path": c.resultsOut,
          "query": f"""
               select avg(amnt_events) as Average_converting_distance from (
                   select df.user_id,count(df.type) amnt_events 
                   from {c.analyticsDf} as df
                   join (
                       select user_id,max(timestamp) as last_conv_ts from {c.analyticsDf}
                       where type  in ('conversion')
                       group by user_id
                   ) sub on sub.user_id = df.user_id and df.timestamp <=sub.last_conv_ts
                   where df.type not in ('start_session','end_session','conversion')
                   group by 1
              ) tb  """}

    q4 = {"name": "patterns_based_on_the_url" + c.csvFormat,
          "path": c.resultsOut,
          "query": f""" select user_id ,count(*) as amt_pattern_event_per_user from (
                           select pattern as base_event,
                            lead(pattern,1) over (partition by user_id order by timestamp ) as next_event,
                            lead(lead(pattern,1) over (partition by user_id order by timestamp ),1) over (partition by user_id order by timestamp )as next_next_event,
                            user_id,timestamp
                           from df
                        ) tb
                       where base_event='/search-products'
                         and next_event='/display-product/1'
                         and next_next_event='/buy-product'
                        group by user_id
                      """}

    _debug = {"name": "test" + c.csvFormat, "landingPath": c.resultsOut, "query": """select 1 """}

    @property
    def questionsDict(self):
        return [Quries.q1, Quries.q2, Quries.q3, Quries.q4]
