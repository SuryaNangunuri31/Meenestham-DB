DROP FUNCTION if exists connecthub.fn_get_task_details;

CREATE OR REPLACE FUNCTION connecthub.fn_get_task_details(client_id integer, task_id character varying)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
declare 
	_tasklist json;
	_comments json;
	_history json;
	_return json;

--exception variables
    _state   TEXT;
    _msg     TEXT;
    _detail  TEXT;
    _hint    TEXT;
    _context TEXT;
BEGIN
   
	select json_agg(row_to_json(t) order by coalesce("LastUpdatedOn","CreatedOn") desc ) into _tasklist 
	from (
	select t.*, ui."UserName" as "AssignedName", tc."TaskCategory"
	from connecthub."Task" t 
	left join connecthub."UserInfo" ui on ui."ClientKey" = t."ClientKey" and ui."Lang" = t."Lang" and t."AssignedTo"::text = ui."UserKey"
	left join masters."TaskCategory" tc on tc."ClientKey" = t."ClientKey" and tc."Lang" = t."Lang" and tc."TaskCategoryID" = t."TaskCategoryID"
	where "TaskId" = task_id and t."ClientKey" = client_id 
	) t;
	select json_agg(row_to_json(c) order by coalesce("LastUpdatedOn","CreatedOn") desc ) into _comments 
	from
	(select 
	c.comment_id "CommentID", c."TaskId",c."ClientKey",c."Lang",commenttext "CommentText",c."CreatedOn",c."LastUpdatedOn","CreatedBy"
	, u."UserName" "CreatedBy"  from connecthub."TaskComments" c 
	left join connecthub."UserInfo" as u on u."UserKey" = c.createdby and u."ClientKey" = c."ClientKey"
	order by c."CreatedOn" desc) as c
	where "TaskId" = task_id and "ClientKey" = client_id ;
	select json_agg(row_to_json(h) order by coalesce("LastUpdatedOn","CreatedOn") desc ) into _history 
	from (
	select t.*, ui."UserName" as "CreatedByName" from connecthub."TaskHistory" t
	left join connecthub."UserInfo" ui on ui."ClientKey" = t."ClientKey" and ui."Lang" = t."Lang" and t."createdby"::text = ui."UserKey"
	where "TaskId" = task_id and t."ClientKey" = client_id 
	) h
	;


	if _tasklist is not null then 
		_return = json_build_object('Status', 'Success', 'Details', json_build_object('Tasks',_tasklist, 'Comments', coalesce(_comments, '[]'::json), 'History', coalesce(_history,'[]'::json)));
	else 
		_return = json_build_object('Status','Failed', 'Details', json_build_object('msg','Task ID does not exits'));
	end if ;

	return _return;

exception when others then 

    get stacked diagnostics
        _state   = returned_sqlstate,
        _msg     = message_text,
        _detail  = pg_exception_detail,
        _hint    = pg_exception_hint,
        _context = pg_exception_context;
       
            
     insert into connecthub."EXCEPTION_LOG" ("procedure", "state","msg", "detail", "hint", "context")
     values('fn_get_task_details', _state, _msg, _detail, _hint, _context);
    
     _return = json_build_object('Status','Failed','Details', json_build_object('State',_state, '_msg', _msg , '_detail', _detail, '_hint', _hint, '_context', _context));

	return _return;
	
END;
$function$
;

DROP FUNCTION if exists connecthub.fn_list_task;

CREATE OR REPLACE FUNCTION connecthub.fn_list_task(p_clientkey integer, p_assignedto uuid, p_limit integer, p_offset integer, p_status character varying, p_cuserid uuid)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
declare 
	_return json;
--exception variables
    _state   TEXT;
    _msg     TEXT;
    _detail  TEXT;
    _hint    TEXT;
    _context TEXT;
   _isMLA integer;
begin
	
	select case when count(*) is null then 0 else count(*) end into _isMLA from connecthub."UserProfiles" up 
	where "UserKey" = p_AssignedTo::text
	and "ProfileKey" = 1
	and "ClientKey" = p_ClientKey;

	if _isMLA = 1 then
	
	select json_agg(taskobj)
--	filter (where "SNO" >p_offset and "SNO" <=p_offset+p_limit )
	into _return from 
    (SELECT 
    	row_number() over(order by coalesce("LastUpdatedOn", t."CreatedOn") desc) "SNO",
    	json_build_object(
    	'TaskID', "TaskId",
    	'ClientKey', t."ClientKey",
    	'Title', "Title",
    	'Description', "Description",
    	'AssignedTo', "AssignedTo",
    	'AssignedName', ass."UserName",
    	'DueDate', "DueDate",
    	'Attachments', "Attachments",
    	'Priority', "Priority",
    	'CreatedOn', t."CreatedOn",
    	'LastUpdatedOn', "LastUpdatedOn",
    	'CreatedBy', t."CreatedBy",
    	'CreatedByName', crt."UserName",
    	'TaskStatus', "TaskStatus",
		'TaskCategoryID', t."TaskCategoryID",
		'TaskCategory', tc."TaskCategory"
    	) taskobj
    FROM connecthub."Task" t
    left join connecthub."UserInfo" ass on ass."UserKey" = t."AssignedTo"::text and ass."ClientKey" = t."ClientKey"
    left join connecthub."UserInfo" crt on crt."UserKey" = t."CreatedBy" and crt."ClientKey" = t."ClientKey"
	left join masters."TaskCategory" tc on tc."ClientKey" = t."ClientKey" and tc."Lang" = t."Lang" and tc."TaskCategoryID" = t."TaskCategoryID"
    WHERE t."ClientKey" = p_ClientKey
--      and t."AssignedTo" = p_AssignedTo
	and "mark_for_deletion" = false
	and case 
			when p_status is null and p_cuserid is not null then 1=1 and t."AssignedTo" = p_cuserid
			when p_status = 'Overdue' and p_cuserid is not null then ("TaskStatus" != 'Completed' AND "DueDate" < CURRENT_DATE and t."AssignedTo" = p_cuserid)
			when p_status is not null and p_cuserid is null then "TaskStatus" = p_status
			when p_status is not null and p_cuserid is not null then "TaskStatus" = p_status and t."AssignedTo" = p_cuserid
			else 1=1
		end
    order by coalesce("LastUpdatedOn", t."CreatedOn") desc
	) as a ;

else

	select json_agg(taskobj) 
--	filter (where "SNO" >p_offset and "SNO" <=p_offset+p_limit )
	into _return from 
    (SELECT 
    	row_number() over(order by coalesce("LastUpdatedOn", t."CreatedOn") desc) "SNO",
    	json_build_object(
    	'TaskID', "TaskId",
    	'ClientKey', t."ClientKey",
    	'Title', "Title",
    	'Description', "Description",
    	'AssignedTo', "AssignedTo",
    	'AssignedName', ass."UserName",
    	'DueDate', "DueDate",
    	'Attachments', "Attachments",
    	'Priority', "Priority",
    	'CreatedOn', t."CreatedOn",
    	'LastUpdatedOn', "LastUpdatedOn",
    	'CreatedBy', t."CreatedBy",
    	'CreatedByName', crt."UserName",
    	'TaskStatus', "TaskStatus",
		'TaskCategoryID', t."TaskCategoryID",
		'TaskCategory', tc."TaskCategory"
    	) taskobj
    FROM connecthub."Task" t
    left join connecthub."UserInfo" ass on ass."UserKey" = t."AssignedTo"::text and ass."ClientKey" = t."ClientKey"
    left join connecthub."UserInfo" crt on crt."UserKey" = t."CreatedBy" and crt."ClientKey" = t."ClientKey"
	left join masters."TaskCategory" tc on tc."ClientKey" = t."ClientKey" and tc."Lang" = t."Lang" and tc."TaskCategoryID" = t."TaskCategoryID"
    WHERE t."ClientKey" = p_ClientKey
      and t."AssignedTo" = p_AssignedTo
	and "mark_for_deletion" = false
	and case 
			when p_status is null and p_cuserid is not null then 1=1 and t."AssignedTo" = p_cuserid
			when p_status = 'Overdue' and p_cuserid is not null then ("TaskStatus" != 'Completed' AND "DueDate" < CURRENT_DATE and t."AssignedTo" = p_cuserid)
			when p_status is not null and p_cuserid is null then "TaskStatus" = p_status
			when p_status is not null and p_cuserid is not null then "TaskStatus" = p_status and t."AssignedTo" = p_cuserid
			else 1=1
		end
    order by coalesce("LastUpdatedOn", t."CreatedOn") desc
	) as a ;
end if;

	_return = json_build_object('Status', 'Success', 'Details', _return);
	
	return _return;
	
exception when others then 

    get stacked diagnostics
        _state   = returned_sqlstate,
        _msg     = message_text,
        _detail  = pg_exception_detail,
        _hint    = pg_exception_hint,
        _context = pg_exception_context;
       
            
     insert into connecthub."EXCEPTION_LOG" ("procedure", "state","msg", "detail", "hint", "context")
     values('fn_list_task', _state, _msg, _detail, _hint, _context);
    
     _return = json_build_object('Status','Failed','Details', json_build_object('State',_state, '_msg', _msg , '_detail', _detail, '_hint', _hint, '_context', _context));

	return _return;
   
END;
$function$
;



DROP function if exists connecthub."ProcHistory";

CREATE OR REPLACE FUNCTION connecthub."ProcHistory"(_clientkey integer, _lang character, _limit integer, _offset integer, _requestid character varying)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
declare 
--variable declaration
	_return json; 

--exception variables
    _state   TEXT;
    _msg     TEXT;
    _detail  TEXT;
    _hint    TEXT;
    _context TEXT;

begin 

	select 
	json_agg(row_to_json(a) order by "ChangedAt" desc) into _return
	from 
(select "UserName","Info","MFColumn",
case when oldvalue = 'null' then '' when "MFColumn" in ('RequestedBy','RequestedByInfo','ReferedByInfo', 'RequestedForInfo', 'RequestedFor', 'ReferedBy', 'AdditionalInfo','LocationKey','Attachments','PartyCadreDetails') then '' else replace(oldvalue,'"', '') end "OldValue",
case when newvalue ='null' then '' when "MFColumn" in ('RequestedBy','RequestedByInfo','ReferedByInfo', 'RequestedForInfo', 'RequestedFor', 'ReferedBy') then 'Modifed Requestors List' 
when newvalue ='null' then '' when "MFColumn" in ('LocationKey') then 'Modifed Geographics' 
when "MFColumn" in ('Attachments') then 'New Attachments are Uploaded'
when "MFColumn" in ('PartyCadreDetails') then 'Modifed Party Cadre Details'
else replace(newvalue,'"','') end "NewValue",a."CreatedBy",a."ModifiedBy", "ChangedAt" "CreatedOn","OperationType", "ChangedAt" 
from (
SELECT 'Created the Request' "Info", '' "MFColumn", '' oldvalue, '' newvalue, "CreatedBy", coalesce("ChangedBy","CreatedBy") "ModifiedBy",coalesce("ChangedAt","CreatedOn") "CreatedOn", "OperationType", "ChangedAt" FROM connecthub."GrievanceInfo_log"
where 
"OperationType" = 'INSERT' and
"ClientKey" = _clientKey and 
"GrievanceKey" = _RequestID
union 
select 
"Info","MFColumn",case when "MFColumn" = 'AssignedTo' and oldvalue = '"0 - 0 - 0 - 0 - 0"' then 'UnAssigned' else coalesce(ui."UserName",oldvalue) end oldvalue,coalesce(u."UserName",newvalue) newvalue,a."CreatedBy",a."ModifiedBy",a."CreatedOn","OperationType","ChangedAt"
from 
(SELECT distinct "ClientKey",
case when "OldRecord"::json->>'GrievanceStatus' = '7' and "NewRecord"::json->>'GrievanceStatus' = '1' then 'Created the Request ' else 'Updated the '||pre.key end "Info",
case when "OldRecord"::json->>'GrievanceStatus' = '7' and "NewRecord"::json->>'GrievanceStatus' = '1' then '' else pre.key end "MFColumn", 
case when "OldRecord"::json->>'GrievanceStatus' = '7' and "NewRecord"::json->>'GrievanceStatus' = '1' then '' else pre.value::text end oldvalue, 
case when "OldRecord"::json->>'GrievanceStatus' = '7' and "NewRecord"::json->>'GrievanceStatus' = '1' then '' else post.value::text end newvalue,
 "CreatedBy", coalesce("ChangedBy","CreatedBy") "ModifiedBy", coalesce("ChangedAt","CreatedOn") "CreatedOn","OperationType", "ChangedAt" FROM connecthub."GrievanceInfo_log", jsonb_each(to_jsonb("NewRecord")) AS post(key, value), jsonb_each(to_jsonb("OldRecord")) AS pre(key, value)
where 
"OperationType" = 'UPDATE' and
"ClientKey" = _clientKey and 
"GrievanceKey" = _RequestID
and pre.key = post.key
and pre.value IS DISTINCT FROM post.value ) as a
left join connecthub."UserInfo" ui on ui."UserKey" = replace(a."oldvalue", '"','') and ui."ClientKey" = a."ClientKey" and "MFColumn" = 'AssignedTo'
left join connecthub."UserInfo" u on u."UserKey" = replace(a."newvalue",'"','') and u."ClientKey" = a."ClientKey" and "MFColumn" = 'AssignedTo'
) a
left join connecthub."UserInfo" ui on ui."UserKey" = coalesce(a."ModifiedBy",a."CreatedBy")
union
select "UserName",'Added a Comment ' "Info" ,'' "MFColumn",'' "OldValue", '' "NewValue",cil."CreatedBy",cil."ModifiedBy",cil."CreatedOn", 'COMMENT' "TG_OP", cil."CreatedOn" "ChangedAt" from connecthub."CommentsInfo" cil 
join connecthub."UserInfo" ui on ui."ClientKey" = cil."ClientKey" and ui."UserKey"= coalesce(cil."ModifiedBy",cil."CreatedBy") 
where cil."ClientKey" = _clientKey and "RequestID" = _RequestID ) as a
--where  "MFColumn" not in ('ModifiedOn','ModifiedBy','ModifiedUserName','GrievanceStatus', 'GrievanceType', 'CreatedBy','SummarizedText','Tags');
--where  "MFColumn" not in ('GrievanceStatus', 'GrievanceType','SummarizedText','Tags','IsActive','CreatedOn','CreatedBy','CreatedUserName','ModifiedOn','ModifiedBy','ModifiedUserName','OperationType','ChangedAt','ChangedBy','IsLocationSpecific','LocationKey')
where  "MFColumn" in ('GrievanceText', 'Attachments', 'RequestedFor', 'RequestedBy', 'ReferedBy', 'AdditionalInfo', 'Priority', 'Status', 'AssignedTo', 'PartyCadreStatus', 'PartyCadreDetails', 'Department','')
and "MFColumn" not like '%Code%';
	
	
--select
--json_agg(row_to_json(a) order by "CreatedOn" desc) into _return
--from (
--
--	select "UserName","Info", "MFColumn","OldValue","NewValue", 
--	"UserName" "CreatedBy", a."ModifiedBy",a."CreatedOn","TG_OP" from 	
--	(select 'Created the Request' "Info", '' "MFColumn",'' "OldValue",'' "NewValue", 
--	"CreatedBy",'' "ModifiedBy","CreatedOn","TG_OP" from connecthub."RequestRecordsLog" rrl 
--	where "RequestID" = _RequestID and "TG_OP" = 'INSERT'
--	) a
--	join connecthub."UserInfo" ui on ui."UserKey" = a."CreatedBy"
--	union
--	select "UserName",a.* from (
--	select 'Updated the '||(jsonb_array_elements("ModifiedField")->>'ModifiedColumn')::text, jsonb_array_elements("ModifiedField")->>'ModifiedColumn' "MFColumn",
--	jsonb_array_elements("ModifiedField")->>'OldValue'  "OldValue", jsonb_array_elements("ModifiedField")->>'NewValue' "NewValue", 
--	"CreatedBy","ModifiedBy","CreatedOn","TG_OP" from connecthub."RequestRecordsLog" as a
--	where "RequestID" = _RequestID and "TG_OP" = 'UPDATE'
--	union 
--
--	select 'Updated the '||(jsonb_array_elements("ModifiedFields")->>'ModifiedColumn')::text, jsonb_array_elements("ModifiedFields")->>'ModifiedColumn' "MFColumn",jsonb_array_elements("ModifiedFields")->>'OldValue' "OldValue",jsonb_array_elements("ModifiedFields")->>'NewValue' "NewValue", "CreatedBy","ModifiedBy","CreatedOn","TG_OP" from connecthub."RequestInfoLog" ril 
--	where "RequestID" = _RequestID and "TG_OP" = 'UPDATE' ) as a
--	join connecthub."UserInfo" ui on ui."UserKey" = a."ModifiedBy" 
--union
--select "UserName","Info","MFColumn",oldvalue,newvalue,a."CreatedBy",a."ModifiedBy",a."CreatedOn","OperationType" from (
--SELECT 'Created the Request' "Info", '' "MFColumn", '' oldvalue, '' newvalue, "CreatedBy",'' "ModifiedBy","CreatedOn", "OperationType" FROM connecthub."GrievanceInfo_log"
--where 
--"OperationType" = 'INSERT' and
--"GrievanceKey" = _RequestID
--union 
--SELECT 'Updated the '||pre.key "Info",pre.key "MFColumn", pre.value::text oldvalue, post.value::text newvalue, "CreatedBy",'' "ModifiedBy","CreatedOn","OperationType" FROM connecthub."GrievanceInfo_log", jsonb_each(to_jsonb("NewRecord")) AS post(key, value), jsonb_each(to_jsonb("OldRecord")) AS pre(key, value)
--where 
--"OperationType" = 'UPDATE' and
--"GrievanceKey" = _RequestID
--and pre.key = post.key
--and pre.value IS DISTINCT FROM post.value
--) a
--left join connecthub."UserInfo" ui on ui."UserKey" = a."CreatedBy"
--union
--select "UserName",'Added a Comment ' "Info" ,'' "MFColumn","Comment" "OldValue", '' "NewValue",cil."CreatedBy",cil."ModifiedBy",cil."CreatedOn", 'COMMENT' "TG_OP" from connecthub."CommentsInfo" cil 
--join connecthub."UserInfo" ui on ui."UserKey"= cil."CreatedBy" 
--where "RequestID" = _RequestID
--) a
--where "MFColumn" not in ('ModifiedOn','ModifiedBy','RequestedBy','ModifiedUserName','RequestedByInfo','GrievanceStatus');	 


_return = json_build_object('Status','Success', 'Details', _return);

	return _return;	

	exception when others then 

    get stacked diagnostics
        _state   = returned_sqlstate,
        _msg     = message_text,
        _detail  = pg_exception_detail,
        _hint    = pg_exception_hint,
        _context = pg_exception_context;
       
     insert into connecthub."EXCEPTION_LOG" ("procedure", "state","msg", "detail", "hint", "context")
     values('ProcHistory', _state, _msg, _detail, _hint, _context);
    
	_return = json_build_object('Status','Failed','Details', json_build_object('State',_state, '_msg', _msg , '_detail', _detail, '_hint', _detail, '_context', _context));
--  
	return _return;
	
end;
$function$
;

drop function if exists connecthub.fn_task_overview;

CREATE OR REPLACE FUNCTION connecthub.fn_task_overview(_clientkey integer, _lang character varying, _assignedto text)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
declare 
	_return json;
	_isMLA integer;
--exception variables
    _state   TEXT;
    _msg     TEXT;
    _detail  TEXT;
    _hint    TEXT;
    _context TEXT;
   
begin
	
/*select * from connecthub."ProcURLRouting"('{"params":{"UserID":"11e39daa-70c1-70e0-5e23-be4fd6867366","Offset":0,"Limit":10,"Form":"TaskOverview"},"requestURI":"TaskOverview","global":{"ClientKey":1,"Lang":"en"}}')*/

	select case when count(*) is null then 0 else count(*) end into _isMLA from connecthub."UserProfiles" up 
	where "UserKey" = _AssignedTo::text
	and "ProfileKey" = 1
	and "ClientKey" = _ClientKey;

	if _isMLA = 1 then
	
		select json_agg(row_to_json(a))
			into _return
		from (
		SELECT 
			t."ClientKey",
			t."Lang",
		    "AssignedTo",
		    ass."UserName" "AssignedName",
		    COUNT("TaskId") AS "TotalTasks",
		    SUM(CASE WHEN "TaskStatus" = 'InProgress' THEN 1 ELSE 0 END) AS "Pending",
		    SUM(CASE WHEN "TaskStatus" = 'Completed' THEN 1 ELSE 0 END) AS "Closed",
		    SUM(CASE WHEN "TaskStatus" = 'Hold' THEN 1 ELSE 0 END) AS "Hold",
		    SUM(CASE WHEN "TaskStatus" != 'Completed' AND "DueDate" < CURRENT_DATE THEN 1 ELSE 0 END) AS "OverDue", -- Overdue tasks
		    ROUND(
		        (SUM(CASE WHEN "TaskStatus" = 'Completed' THEN 1 ELSE 0 END) * 100.0) / COUNT("TaskId"),
		        2
		    ) AS "CompletionPercentage"
		FROM 
		    connecthub."Task" t
		    left join connecthub."UserInfo" ass on ass."UserKey" = t."AssignedTo"::text and ass."ClientKey" = t."ClientKey" and ass."IsActive" = true
		    where t."ClientKey" = _clientkey and t."Lang" = _lang and mark_for_deletion = False  --and t."AssignedTo" = 
		GROUP BY 
			t."ClientKey",
			t."Lang",
			ass."UserName",
			"AssignedTo") a;

	else
		
		select json_agg(row_to_json(a))
			into _return
		from (
		SELECT 
			t."ClientKey",
			t."Lang",
		    "AssignedTo",
		    ass."UserName" "AssignedName",
		    COUNT("TaskId") AS "TotalTasks",
		    SUM(CASE WHEN "TaskStatus" = 'InProgress' THEN 1 ELSE 0 END) AS "Pending",
		    SUM(CASE WHEN "TaskStatus" = 'Completed' THEN 1 ELSE 0 END) AS "Closed",
		    SUM(CASE WHEN "TaskStatus" = 'Hold' THEN 1 ELSE 0 END) AS "Hold",
		    SUM(CASE WHEN "TaskStatus" != 'Completed' AND "DueDate" < CURRENT_DATE THEN 1 ELSE 0 END) AS "OverDue", -- Overdue tasks
		    ROUND(
		        (SUM(CASE WHEN "TaskStatus" = 'Completed' THEN 1 ELSE 0 END) * 100.0) / COUNT("TaskId"),
		        2
		    ) AS "CompletionPercentage"
		FROM 
		    connecthub."Task" t
		    left join connecthub."UserInfo" ass on ass."UserKey" = t."AssignedTo"::text and ass."ClientKey" = t."ClientKey" and ass."IsActive" = true
		    where t."ClientKey" = _clientkey and t."Lang" = _lang and t."AssignedTo" = _AssignedTo and mark_for_deletion = False
		GROUP BY 
			t."ClientKey",
			t."Lang",
			ass."UserName",
			"AssignedTo") a;

	end if;

	_return = json_build_object('Status', 'Success', 'Details', _return);
	
	return _return;
	
exception when others then 

    get stacked diagnostics
        _state   = returned_sqlstate,
        _msg     = message_text,
        _detail  = pg_exception_detail,
        _hint    = pg_exception_hint,
        _context = pg_exception_context;
       
            
     insert into connecthub."EXCEPTION_LOG" ("procedure", "state","msg", "detail", "hint", "context")
     values('fn_task_overview', _state, _msg, _detail, _hint, _context);
    
     _return = json_build_object('Status','Failed','Details', json_build_object('State',_state, '_msg', _msg , '_detail', _detail, '_hint', _hint, '_context', _context));

	return _return;
   
END;
$function$
;