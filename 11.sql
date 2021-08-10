DROP VIEW IF EXISTS VT_EBK_FULL_ORGANIC_AD_LIST;
CREATE TEMPORARY VIEW VT_EBK_FULL_ORGANIC_AD_LIST AS 
SELECT
AD.CLSFD_AD_ID
,AD.CLSFD_SITE_ID
,CAST(CAST(ad.ad_orgnl_start_dt AS CHAR(10))||' '||CAST(ad.ad_orgnl_start_tm AS CHAR(10)) AS TIMESTAMP) AS ad_starttime
,CAST(CAST(U.REG_DT AS CHAR(10))||' '||CAST(U.REG_TM AS CHAR(10)) AS TIMESTAMP) AS user_regtime
,sum(AD.ASK_PRICE_LC) AS ASK_PRICE_LC
FROM  CLSFD_ACCESS_VIEWS.DW_CLSFD_AD AD
INNER JOIN CLSFD_ACCESS_VIEWS.CLSFD_CATEG_LKP categ
    ON ad.CLSFD_CATEG_REF_ID = categ.clsfd_categ_ref_id
    and ad.CLSFD_SITE_ID=categ.CLSFD_SITE_ID
LEFT JOIN CLSFD_ACCESS_VIEWS.DW_CLSFD_USER U 
    ON cast (AD.CLSFD_USER_ID as string)=cast (U.CLSFD_USER_ID as string)
    and AD.CLSFD_SITE_ID=U.USER_CLSFD_SITE_ID
-- WHERE ad.SRC_CRE_DT BETWEEN add_months(date_sub(current_date,(day(current_date)-1)),-1) AND CURRENT_DATE   /*use hst.AD_STATUS_START_DATE instead of ad.SRC_CRE_DT*/
WHERE ad.SRC_CRE_DT BETWEEN '2021-04-01' AND CURRENT_DATE
    AND AD.CLSFD_SITE_ID IN (9021)
    AND AD.CLSFD_PROXY_ID IS NULL -- Only organic ads
    and categ.clsfd_meta_categ_id not in (102,195,130,400) and categ.clsfd_lvl2_categ_id not in (216,211,241,222,218,306) -- exclude non-horizontal categories
GROUP BY 1,2,3,4;


-- keep only ended ads
-- added Cheryl
DROP VIEW IF EXISTS VT_EBK_END_AD_LIST;
CREATE TEMPORARY VIEW VT_EBK_END_AD_LIST AS
SELECT
    AD.CLSFD_AD_ID,
    AD.CLSFD_SITE_ID,
    AD.AD_STARTTIME,
    AD.USER_REGTIME,
    AD.ASK_PRICE_LC
FROM VT_EBK_FULL_ORGANIC_AD_LIST AD
INNER JOIN CLSFD_ACCESS_VIEWS.CLSFD_AD_STS_CHNG_HST HST 
ON CAST(AD.CLSFD_AD_ID as string) = CAST(HST.CLSFD_AD_ID as string)
AND AD.CLSFD_SITE_ID = HST.CLSFD_SITE_ID
AND HST.AD_STATUS_END_DATE = '2099-12-31'
AND HST.AD_STATUS_ID IN (2,3,12)
WHERE HST.CLSFD_SITE_ID IN (9021);
        


-- prepare reply data 
-- pull in conversation, message
DROP VIEW IF EXISTS VT_EBK_REPLY_MSG;
CREATE TEMPORARY VIEW VT_EBK_REPLY_MSG AS
SELECT
    REPLY.CLSFD_AD_ID
    ,REPLY.CLSFD_SITE_ID
    ,MSG.CLSFD_CNVRSTN_ID
    ,REPLY.SENDER_USER_ID clsfd_user_id
    ,REPLY.RECIPIENT_USER_ID
    ,MSG.CLSFD_MSG_ID
    ,MSG.MSG_DRCTN
    ,MSG.MSG_STATUS_ID
    ,REPLY.SRC_CRE_DT REPLY_DT
    ,cast ( concat(concat(cast(msg.src_cre_dt as CHAR(10)),' '),cast(msg.src_cre_tm as CHAR(10))) as TIMESTAMP) + INTERVAL '2' HOUR  as cre_tm 
FROM CLSFD_ACCESS_VIEWS.DW_CLSFD_REPLY REPLY
INNER JOIN CLSFD_ACCESS_VIEWS.CLSFD_MSG MSG
    ON cast (REPLY.CLSFD_REPLY_ID as string)=cast (MSG.CLSFD_CNVRSTN_ID as string)
    and REPLY.CLSFD_SITE_ID=MSG.CLSFD_SITE_ID    
WHERE REPLY.CLSFD_SITE_ID IN (9021) 
    -- AND REPLY.SRC_CRE_DT BETWEEN add_months(date_sub(current_date,(day(current_date)-1)),-1) AND CURRENT_DATE   /*use hst.AD_STATUS_START_DATE instead of ad.SRC_CRE_DT*/
    AND REPLY.SRC_CRE_DT BETWEEN '2021-04-01' AND CURRENT_DATE
    AND REPLY.CLSFD_AD_ID IS NOT NULL
    AND MSG.MSG_STATUS_ID<>-99;


-- pull ads and cnvstn together, find first b2s message at cnvrstn level
DROP VIEW IF EXISTS VT_EBK_NEW_CONNECT_AD;
CREATE TEMPORARY VIEW VT_EBK_NEW_CONNECT_AD AS
 SELECT  
      ad.clsfd_ad_id
   ,ad.user_regtime
   ,msg.clsfd_cnvrstn_id
   ,ad.clsfd_site_id
   ,ad.ad_starttime
   ,coalesce(min(case  when lower(msg_drctn)=lower('B2S') then cre_tm else NULL end ),cast ( '2099-12-31 00:00:00' as TIMESTAMP)) as first_b2s_tm
   ,coalesce(max(case  when lower(msg_drctn)=lower('B2S') then cre_tm else NULL end ),cast ( '1970-01-01 00:00:00' as TIMESTAMP)) as last_b2s_tm
   ,coalesce(min(case  when lower(msg_drctn)=lower('S2B') then cre_tm else NULL end ),cast ( '2099-12-31 00:00:00' as TIMESTAMP)) as first_s2b_tm
   ,coalesce(max(case  when lower(msg_drctn)=lower('S2B') then cre_tm else NULL end ),cast ( '1970-01-01 00:00:00' as TIMESTAMP)) as last_s2b_tm
   ,sum(case  when  (msg_status_id in( -99,8) and lower(msg_drctn)=lower('B2S')) then 1 else 0 end ) as b2s_cnt
   ,sum(case  when  (msg_status_id in( -99,8) and lower(msg_drctn)=lower('S2B')) then 1 else 0 end ) as s2b_cnt 
   ,sum(ad.ASK_PRICE_LC) as ASK_PRICE_LC
--  FROM vt_EBK_full_organic_ad_list as ad
FROM VT_EBK_END_AD_LIST as ad
left join vt_EBK_reply_msg as msg
    on cast (ad.clsfd_ad_id as string)= cast (msg.clsfd_ad_id as string)
 GROUP BY 1,2,3,4,5;


--- cache table VT_EBK_NEW_CONNECT_AD;

-- find first b2s message at ad level
DROP VIEW IF EXISTS AD_LVL_FIRST_B2S;
CREATE TEMPORARY VIEW AD_LVL_FIRST_B2S AS
SELECT  
  ad.clsfd_ad_id 
   ,ad.clsfd_site_id
   ,ad.ad_starttime
   ,min(first_b2s_tm) as first_b2s_tm_ad
   ,max(last_b2s_tm) as last_b2s_tm_ad
   ,min(first_s2b_tm) as first_s2b_tm_ad
   ,max(last_s2b_tm) as last_b2s_tm_ad_ad
 FROM VT_EBK_NEW_CONNECT_AD as ad
 GROUP BY 1,2,3;



-- find sold flag 
DROP VIEW IF EXISTS vt_EBK_hit_trans_est;
CREATE TEMPORARY VIEW vt_EBK_hit_trans_est AS
select     
        clsfd_user_id,
        clsfd_ad_id,
        clsfd_event_categ,
        clsfd_event_action,
        clsfd_event_label,
      CASE 
            WHEN lower(clsfd_event_label)=lower('sold_on_ebayk') or lower(clsfd_event_label)=lower('soldonebayk') or lower(clsfd_event_label)=lower('SoldAtEBayK') 
            or lower(clsfd_event_label)=lower('sold;SoldAtEBayK') or lower(clsfd_event_label)=lower('delete;SoldAtEBayK') -- Cheryl add
            or lower(clsfd_event_label)=lower('DeleteAd;SoldAtEBayK') THEN 1 -- Cheryl add 
            
            WHEN lower(clsfd_event_label)=lower('sold_elsewhere') or lower(clsfd_event_label)=lower('soldsomewhereelse') or lower(clsfd_event_label)=lower('sold;SoldSomewhereElse') 
            or lower(clsfd_event_label)=lower('delete;SoldSomewhereElse') or lower(clsfd_event_label)=lower('DeleteAd;SoldSomewhereElse') THEN 0 -- Cheryl add
            
        ELSE null end  as sold_on_ebayk,
        case  when lower(clsfd_event_label)=lower('sold_elsewhere') or lower(clsfd_event_label)=lower('soldsomewhereelse')then 1 else 0 end  as sold_elsewhere,
        case  when lower(clsfd_event_label)=lower('no_reason') or lower(clsfd_event_label)=lower('deletewithoutreason') then 1 else 0 end  as no_reason, 
        count(distinct clsfd_ad_id) cntClsfdAdId,
        count(distinct ga_vstr_id) cntUsers,
        count(distinct ecg_session_id) cntSessions,
        count(*) cntPageviews
from     clsfd_access_views.clsfd_ecg_hit
-- WHERE    ecg_session_start_dt BETWEEN add_months(date_sub(current_date,(day(current_date)-1)),-1) AND CURRENT_DATE   /*use hst.AD_STATUS_START_DATE instead of ad.SRC_CRE_DT*/
WHERE    ecg_session_start_dt BETWEEN "2021-04-01" AND CURRENT_DATE
and     clsfd_site_id = 9021
and        hit_type = 'EVENT'
and        clsfd_event_action = 'DeleteAdSuccess'
group by 1,2,3,4,5,6,7,8;


-- find time between 1st message and post, bring in dlt_reason
DROP VIEW IF EXISTS vt_EBK_connections;
CREATE TEMPORARY VIEW vt_EBK_connections 
AS
 SELECT  
  cnct.clsfd_ad_id
   ,cnct.clsfd_cnvrstn_id
   ,cnct.clsfd_site_id
   ,cnct.user_regtime
   ,fin.clsfd_event_label
   ,fin.sold_on_ebayk
   ,fin.sold_elsewhere
   ,fin.no_reason
   ,CASE WHEN frst.first_b2s_tm_ad='2099-12-31 00:00:00' THEN NULL ELSE unix_timestamp(cast ( frst.first_b2s_tm_ad as TIMESTAMP))-unix_timestamp(cast ( cnct.ad_starttime as TIMESTAMP)) END as timebtwnpost1stmsg
   ,case when cnct.b2s_cnt > 0 and cnct.s2b_cnt>0 then 1 else 0 end as connections
   ,frst.first_b2s_tm_ad
   ,cnct.ad_starttime
   ,cnct.first_b2s_tm
   ,cnct.last_b2s_tm
   ,cnct.first_s2b_tm
   ,cnct.last_s2b_tm
   ,cnct.b2s_cnt
   ,cnct.s2b_cnt
   ,cnct.ASK_PRICE_LC
 FROM vt_EBK_new_connect_ad as cnct
-- inner join vt_EBK_hit_trans_est as fin -- join hit_trans table to keep ended ads only 
-- changed by Cheryl
LEFT join vt_EBK_hit_trans_est as fin -- join hit_trans table to keep ended ads only
    on cast (cnct.clsfd_ad_id as string) =cast (fin.clsfd_ad_id  as string)
LEFT JOIN AD_LVL_FIRST_B2S frst
    ON cast (frst.clsfd_ad_id as string)=cast (cnct.clsfd_ad_id as string);


-- create ebayk feature table -- 
DROP VIEW IF EXISTS EK_EST_TRANS_FIN;
CREATE TEMPORARY view  EK_EST_TRANS_FIN
AS
 SELECT  
  c.clsfd_ad_id
  ,c.clsfd_site_id
   ,c.ad_starttime
   ,unix_timestamp(cast (current_timestamp() as TIMESTAMP))-unix_timestamp(cast ( c.user_regtime as TIMESTAMP)) as seller_tenure
   ,cnvrstns.conversations as numconversations
   ,cnvrstns.msg_cnt as nummessagestotal
   ,sold_on_ebayk
   ,sold_elsewhere
   ,no_reason
   ,min(c.timebtwnpost1stmsg) as time_ad_post_until_first_connection
   ,sum(c.connections) as numconversationswithconnection
   ,sum(c.s2b_cnt) as nummessagesseller
   ,sum(c.ask_price_lc) as ask_price_lc 
 FROM vt_EBK_connections as c
inner join
(SELECT  
  clsfd_ad_id
   ,count(distinct clsfd_cnvrstn_id)  as conversations
   ,(sum(b2s_cnt)+sum(s2b_cnt)) as msg_cnt 
 FROM vt_EBK_connections 
 GROUP BY 1 ) as cnvrstns
    on cast (cnvrstns.clsfd_ad_id as string)=cast (c.clsfd_ad_id as string) 
GROUP BY 1,2,3,4,5,6,7,8,9;


DROP VIEW IF EXISTS RESULT;
CREATE TEMPORARY view  RESULT
AS
select
    a.clsfd_ad_id as clsfd_ad_id,
    a.ad_starttime as ad_strt_tm,
    b.AD_RMVL_DT as ad_rmvl_dt,
    a.seller_tenure as slr_tenure_tm,
    a.numconversations as convr_cnt,
    a.numconversationswithconnection as convr_with_conn_cnt,
    a.nummessagestotal as msg_cnt,
    a.time_ad_post_until_first_connection as ad_post_until_first_conn_tm,
    a.ask_price_lc as ask_price_amt,
    a.nummessagesseller as  slr_msg_cnt,
    a.sold_on_ebayk as sold_flag,
    null as pymnt_flag,
    CURRENT_DATE() as cre_date,
    CURRENT_USER() as cre_user,
    NULL as upd_date,
    NULL as upd_user,
    a.clsfd_site_id as clsfd_site_id
from EK_EST_TRANS_FIN a
LEFT JOIN CLSFD_ACCESS_VIEWS.DW_CLSFD_AD b 
ON a.clsfd_ad_id = b.CLSFD_AD_ID and a.clsfd_site_id = b.CLSFD_SITE_ID;


insert overwrite table P_CHERYL_T.clsfd_prdct_trans_src partition(clsfd_site_id)
 select
    clsfd_ad_id as clsfd_ad_id,
    ad_strt_tm,
    ad_rmvl_dt,
    slr_tenure_tm,
    convr_cnt,
    convr_with_conn_cnt,
    msg_cnt,
    ad_post_until_first_conn_tm,
    ask_price_amt,
    slr_msg_cnt,
    sold_flag,
    pymnt_flag,
    cre_date,
    cre_user,
    upd_date,
    upd_user,
    clsfd_site_id as clsfd_site_id
from RESULT
distribute by clsfd_site_id
;
