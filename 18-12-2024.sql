INSERT INTO connecthub."PageGroup"
("ClientKey", "PageGroupKey", "PageGroup", "OrdinalPosition")
select 1, unnest(array[4,5,6,7,8]) "PageGroupKey",unnest(array['Overview','Issues','People','Work','Resource']) as "PageGroup"
union
select 3, unnest(array[4,5,6,7,8]) "PageGroupKey",unnest(array['Overview','Issues','People','Work','Resource']) as "PageGroup"
union
select 4, unnest(array[4,5,6,7,8]) "PageGroupKey",unnest(array['Overview','Issues','People','Work','Resource']) as "PageGroup";

update connecthub."PageGroup"
set "OrdinalPosition" = 0
where "PageGroupKey" = 0;

update connecthub."PageGroup"
set "OrdinalPosition" = 2
where "PageGroupKey" = 1;

update connecthub."PageGroup"
set "OrdinalPosition" = 7
where "PageGroupKey" = 2;

update connecthub."PageList" 
set "PageGroupKey" = 4
where "PageKey" = 7;

update connecthub."PageList" 
set "PageGroupKey" = 5
where "PageKey" in (1,11);

update connecthub."PageList" 
set "PageGroupKey" = 6
where "PageKey" in (12,13,14);

update connecthub."PageList" 
set "PageGroupKey" = 7
where "PageKey" in (9,10,16);

update connecthub."PageList" 
set "PageGroupKey" = 8
where "PageKey" in (3,15);


update connecthub."PageList_Lang" 
set "PageGroupKey" = 4
where "PageKey" = 7;

update connecthub."PageList_Lang" 
set "PageGroupKey" = 5
where "PageKey" in (1,11);

update connecthub."PageList_Lang" 
set "PageGroupKey" = 6
where "PageKey" in (12,13,14);

update connecthub."PageList_Lang" 
set "PageGroupKey" = 7
where "PageKey" in (9,10,16);

update connecthub."PageList_Lang" 
set "PageGroupKey" = 8
where "PageKey" in (3,15);


update connecthub."PageRoleAccess" 
set "PageGroupKey" = 4
where "PageKey" = 7;

update connecthub."PageRoleAccess" 
set "PageGroupKey" = 5
where "PageKey" in (1,11);

update connecthub."PageRoleAccess" 
set "PageGroupKey" = 6
where "PageKey" in (12,13,14);

update connecthub."PageRoleAccess" 
set "PageGroupKey" = 7
where "PageKey" in (9,10,16);

update connecthub."PageRoleAccess" 
set "PageGroupKey" = 8
where "PageKey" in (3,15);


DROP FUNCTION connecthub."ProcAfterLoggedIn";

CREATE OR REPLACE FUNCTION connecthub."ProcAfterLoggedIn"(_clientkey integer, _lang character, _pagekey integer, _userkey character varying, _advfilters json DEFAULT NULL::json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
declare
-- variable declaration
	_return json;
	_UserInfo json;
	_Pages json;
	_UiElements json;
	_UserName text;
	_AllPages json;
	_AllActions json;
	_adv text;
	_CommonElements json;
	_Actions json;
  _dropdown_json json;
  r record;
 _status_list json;
 filters text;
 query text;
	_rolekey int;
	_Photo text;
	_Pname varchar(50);
	_theme varchar(20);
	_palette json;
--exception variables
    _state   TEXT;
    _msg     TEXT;
    _detail  TEXT;
    _hint    TEXT;
    _context TEXT;
   _spartycadretab boolean;
begin
-- stored procedure body
	
	select "FullName","Photo" into _UserName, _Photo 
--	from hubviews.vw_userinfo where "Lang" = _Lang and "UserKey"= _UserKey and "ClientKey" = _ClientKey;
	from hubviews.fn_userinfo() where "Lang" = _Lang and "UserKey"= _UserKey and "ClientKey" = _ClientKey;
	
	select  case when _Lang = 'en' then "Profile" else "ProfileTe" end 
	into _Pname from 
	connecthub."UserProfiles" as a
	join connecthub."ProfileInfo" as b on a."ClientKey" = b."ClientKey" and a."ProfileKey" = b."ProfileKey" and b."IsActive" = true
	where  "UserKey"= _UserKey and a."ClientKey" = _ClientKey;

	select jsonb_build_object('UserInfo', (json_build_object(
				'UserKey',a."UserKey",
				'Email',"Email",
				'Lang',"Lang",
				'FullName',"FullName",
				'ProfileKey', p."ProfileKey",
				'Profile' , case when _Lang = 'en' then "Profile" else "ProfileTe" end,
				'Mobile',"Contact",
				'ImgURL',"Photo",
				'ActiveSince',a."CreatedOn"::date
				))) 
	into _UserInfo		
--	from hubviews.vw_userinfo as a
	from hubviews.fn_userinfo() as a
	join connecthub."UserProfiles" b on a."ClientKey" = b."ClientKey" and a."UserKey" = b."UserKey" and a."Lang" = _Lang and b."IsActive" = true
	left join connecthub."ProfileInfo" p on p."ClientKey" = b."ClientKey"  and p."ProfileKey" = b."ProfileKey" and p."IsActive" = true
	where a."ClientKey" = _ClientKey and a."Lang"= _Lang and a."IsActive" = true 
	and a."UserKey" = _UserKey;

	

	select json_object_agg("PgGroupKeyOrder","pg") 
		into _Pages	
		from 
		(select 
		"PageGroupKey", "PgGroupKeyOrder",json_build_object(
			'PageGroupKey',"PageGroupKey",
			'PageGroup',"PageGroup",
		--	"PageKey","Page","Title","Description" 
			'pages', json_agg(json_build_object( 
							'PageKey', "PageKey",
							'Page', "Page", 
--							'Title', case when "PageKey" = 1 and _UserInfo->'UserInfo'->>'ProfileKey' = '1' then 'Grievances' else "Title" end, 
							'Title', "Title", 
							'PageTitle', "PageTitle",
							'Description', "Description",
							'Icon',"Icon", 
							'Image',"Image", 
							'Class',"Class", 
							'CSS',"CSS"::json, 
							'TagConfig',"TagConfig"::json, 
							'ElementType',"ElementType",
							'OrdinalPosition', "OrdinalPosition"
							))
			) pg
		from 
		(select distinct a."PageGroupKey",c."PageGroup",c."PageKey",c."Page",c."Title",c."PageTitle",c."Description",
		c."Icon", c."Image", c."Class", c."CSS"::text, c."TagConfig"::text, c."ElementType", c."OrdinalPosition", "PgGroupKeyOrder"
		from connecthub."PageRoleAccess" as a
		join hubviews.vw_pagelist as c on c."ClientKey" = a."ClientKey" and c."PageGroupKey" = c."PageGroupKey" and c."PageKey" = a."PageKey" and c."Lang" = _Lang and c."IsActive" = true
		where a."ClientKey" = _ClientKey 
		and a."IsActive" = true
		and "RoleKey" in 
		(
		select "RoleKey" from connecthub."Profileroles" where "ClientKey" = _ClientKey and "IsActive" = true 
		and "ProfileKey" in (
							select "ProfileKey" from connecthub."UserProfiles" where "ClientKey" = _ClientKey and "IsActive" = true and "UserKey" = _UserKey
							)
		) and case when _PageKey is null then 1 else  a."PageKey" end = case when _PageKey is null then 1 else _PageKey end ) as a 
		group by "PageGroupKey", "PageGroup", "PgGroupKeyOrder") as a;

	
	if _PageKey is null then 
	
		_PageKey = 8;
		
	end if;

	select json_object_agg("PropertyType","el") 
		into _UiElements
		from 
		(select "PropertyType",
			json_object_agg("ElementKey", json_build_object(
					'ElementKey',"ElementKey",
					'Label',"Label",
					'TagContent',"TagContent",
					'Description',"Description",
					'Icon',"Icon",
					'Image',case when "ElementKey" = 15 then _Photo else "Image" end,
					'Class',"Class",
					'CSS',"CSS"::json,
					'TagConfig',"TagConfig"::json,
					'ElementType',"ElementType",
					'Count',(
					case when "ElementType" = 'tab' then (select ("ProGetRequestCategoriesList"->>'TotalRecords')::int from connecthub."ProGetRequestCategoriesList"(_ClientKey, _Lang, _UserKey, "ElementKey", null ,null, 1,0, _advfilters))
					else 0 end
						)
					) order by "ElementKey") el
		from 
		(select 
		"ElementKey",case when "ElementKey" = 16 then _UserName when "ElementKey" = 18 then _Pname else "Label" end "Label","TagContent" ,"Description" ,"Icon","Image","Class","CSS"::text ,"TagConfig"::text,"ElementType",  "PropertyType"
--		from hubviews.vw_uielementslist where "ClientKey"=_ClientKey and "Lang"= _Lang and "IsPublic" = true and "IsActive" = true and "PageKey" = _PageKey
		from hubviews.fn_uielementslist() where "ClientKey"=_ClientKey and "Lang"= _Lang and "IsPublic" = true and "IsActive" = true and "PageKey" = _PageKey
		union
		select a."ElementKey", b."Label", b."TagContent", b."Description", b."Icon", b."Image", b."Class", b."CSS"::text, b."TagConfig"::text, b."ElementType", "PropertyType" from connecthub."AuthenticationElements" as a 
--		join hubviews.vw_uielementslist as b on a."ClientKey" = b."ClientKey" and a."PageKey" = b."PageKey" and a."ElementKey" = b."ElementKey" and b."PageKey" = _PageKey
		join hubviews.fn_uielementslist() as b on a."ClientKey" = b."ClientKey" and a."PageKey" = b."PageKey" and a."ElementKey" = b."ElementKey" and b."PageKey" = _PageKey
		where a."RoleKey" in ( 	 	 	
		select "RoleKey" from connecthub."Profileroles" where "ClientKey" = _ClientKey and "IsActive" = true 
		and "ProfileKey" in (
							select "ProfileKey" from connecthub."UserProfiles" where "ClientKey" = _ClientKey and "IsActive" = true and "UserKey" = _UserKey
							)
		) and a."PageKey" = _PageKey and a."View" = true) as a 
		group by "PropertyType") as a;
	
	select json_object_agg("PropertyType","el") 
		into _CommonElements
		from 
		(select "PropertyType",
			json_object_agg("ElementKey", json_build_object(
					'ElementKey',"ElementKey",
					'Label',"Label",
					'TagContent',"TagContent",
					'Description',"Description",
					'Icon',"Icon",
					'Image',"Image",
					'Class',"Class",
					'CSS',"CSS"::json,
					'TagConfig',"TagConfig"::json,
					'ElementType',"ElementType",
					'Count',(
					case when "ElementType" = 'tab' then (select ("ProGetRequestCategoriesList"->>'TotalCount')::int from connecthub."ProGetRequestCategoriesList"(_ClientKey, _Lang, _UserKey, "ElementKey", null ,null, 1,0, null::json))
					else 0 end
						)
					) order by "ElementKey") el
			
		from 
		(select distinct a."ElementKey", b."Label", b."TagContent", b."Description", b."Icon", b."Image", b."Class", b."CSS"::text, b."TagConfig"::text, b."ElementType", "PropertyType" from connecthub."AuthenticationElements" as a 
--		join hubviews.vw_uielementslist as b on a."ClientKey" = b."ClientKey" and a."PageKey" = b."PageKey" and a."ElementKey" = b."ElementKey" and b."PageKey" = 0 and b."PageGroupKey" = 3 and b."Lang" = _Lang and b."ElementType" = 'tab'
		join hubviews.fn_uielementslist() as b on a."ClientKey" = b."ClientKey" and a."PageKey" = b."PageKey" and a."ElementKey" = b."ElementKey" and b."PageKey" = 0 and b."PageGroupKey" = 3 and b."Lang" = _Lang and b."ElementType" = 'tab'
		where a."RoleKey" in (
		select "RoleKey" from connecthub."Profileroles" where "ClientKey" = _ClientKey and "IsActive" = true 
		and "ProfileKey" in (
							select "ProfileKey" from connecthub."UserProfiles" where "ClientKey" = _ClientKey and "IsActive" = true and "UserKey" = _UserKey
							)
		) and a."PageKey" = 0 and a."PageGroupKey" = 3 and a."View" = true) as a 
		group by "PropertyType") as a;
	
	drop table if exists tempactions;
	 create temp table tempactions("PropertyType" text, obj json);
    
     for r in 
     	select distinct a."ElementKey", b."MasterQuery", b."QueryOrder","ElementType"
                 from connecthub."AuthenticationElements" as a
--                 join hubviews.vw_uielementslist as b on a."ClientKey" = b."ClientKey" and a."PageKey" = b."PageKey" and a."PageGroupKey" = b."PageGroupKey" and a."ElementKey" = b."ElementKey" and b."PageKey" = 0 and b."PageGroupKey" = 0 and b."Lang" = _Lang and b."PropertyType" = 'Action'
                 join hubviews.fn_uielementslist() as b on a."ClientKey" = b."ClientKey" and a."PageKey" = b."PageKey" and a."PageGroupKey" = b."PageGroupKey" and a."ElementKey" = b."ElementKey" and b."PageKey" = 0 and b."PageGroupKey" = 0 and b."Lang" = _Lang and b."PropertyType" = 'Action'
                 where a."RoleKey" in (
                 select "RoleKey" from connecthub."Profileroles" where "ClientKey" = _ClientKey and "IsActive" = true
                 and "ProfileKey" in (
                                     select "ProfileKey" from connecthub."UserProfiles" where "ClientKey" = _ClientKey and "IsActive" = true and "UserKey" = _UserKey
                                     )
                 ) and a."PageKey" = 0 and a."PageGroupKey" = 0 and a."View" = true
     loop
         if r."ElementType" in ('dropdown','checkbox', 'radio') then
                filters = ' where "ClientKey" = '||_ClientKey;
                query = 'select json_agg(obj) from (select json_build_object(''ID'',"ID", ''Value'', "Value", ''Color'' , "Color", ''Relation'', "Relation",''Action'', "Action", ''Role'', "rolerelation" '||
                        ' ) obj from ('||coalesce(r."MasterQuery",'')||
                        filters||' order by '||coalesce(r."QueryOrder",'')||') as a) as a' ;
                execute query into _dropdown_json;
            else
                _dropdown_json ='{}'::json;
         end if;
        
        
         insert into tempactions
         	 select "PropertyType",
             json_object_agg("ElementKey", json_build_object(
                     'ElementKey',"ElementKey",
                     'Label',"Label",
                     'TagContent',"TagContent",
                     'Description',"Description",
                     'Icon',"Icon",
                     'Image',"Image",
                     'Class',"Class",
                     'CSS',"CSS"::json,
                     'TagConfig',"TagConfig"::json,
                     'ElementType',"ElementType",
                     'ListValues',_dropdown_json
                     ) order by "ElementKey") el
         from
         (select distinct a."ElementKey", b."Label", b."TagContent", b."Description", b."Icon", b."Image", b."Class", b."CSS"::text, b."TagConfig"::text, b."ElementType", "PropertyType" from connecthub."AuthenticationElements" as a
--         join hubviews.vw_uielementslist as b on a."ClientKey" = b."ClientKey" and a."PageKey" = b."PageKey" and a."ElementKey" = b."ElementKey" and b."PageKey" = 0 and b."PageGroupKey" = 0 and a."PageGroupKey" = b."PageGroupKey" and b."Lang" = _Lang
         join hubviews.fn_uielementslist() as b on a."ClientKey" = b."ClientKey" and a."PageKey" = b."PageKey" and a."ElementKey" = b."ElementKey" and b."PageKey" = 0 and b."PageGroupKey" = 0 and a."PageGroupKey" = b."PageGroupKey" and b."Lang" = _Lang
         where a."ElementKey" = r."ElementKey") 
         group by "PropertyType";
     end loop;
    
     select json_build_object("PropertyType", json_agg(obj::json)) into _Actions  from tempactions
    group by "PropertyType";
   
   select json_agg(row_to_json(a))
   into _status_list
   from
   (select "StatusKey" , "Status","Color","Relation","Action",rolerelation   from masters."Status" where "ClientKey" = _ClientKey and "StatusKey" <> 7) as a; 
  
  	select "RoleKey" into _rolekey from connecthub."Profileroles" where "ClientKey" = _ClientKey and "IsActive" = true
     and "ProfileKey" in 
     (
     	select "ProfileKey" from connecthub."UserProfiles" where "ClientKey" = _ClientKey and "IsActive" = true and "UserKey" = _UserKey
     );
	
  
    select json_agg(row_to_json(a)) into _AllPages
    from
  	(select 'Page' "PropertyType", p."PageKey" "Key", pl."Page" "Name", pl."Title" "Label" ,"TagConfig", "IsPublic", p."IsActive" "Active", 
	coalesce(rr."RoleKey",_rolekey) "RoleKey" , coalesce(rr."IsActive",false) "View" from
	(select * from connecthub."PageList" where "TagConfig" is not null and "PageKey"  <> 2) as p
	join connecthub."PageList_Lang" as pl on pl."ClientKey" = p."ClientKey" and pl."PageGroupKey" = p."PageGroupKey" and pl."PageKey" = p."PageKey"
	left join connecthub."PageRoleAccess"  as rr on p."ClientKey"= rr."ClientKey" and p."PageKey" = rr."PageKey" 
	and p."PageGroupKey" = rr."PageGroupKey" and rr."RoleKey" = _rolekey) as a;

	select json_agg(row_to_json(a)) into _AllActions
    from
  	(select 'Action' "PropertyType" ,a."ElementKey" "Key", "Label" "Name", "Label" , "TagConfig", "IsPublic", "IsActive" "Active",
	coalesce(rr."RoleKey",_rolekey) "RoleKey", coalesce(rr."View",false) "View" from 
	(select * from connecthub."UIElementsInfo" where "PropertyType" = 'Action') as a
	join connecthub."UIElementsInfo_Lang" as b on a."ClientKey"= b."ClientKey" and a."ElementKey" = b."ElementKey"
	left join connecthub."AuthenticationElements" as rr on a."ClientKey"= rr."ClientKey" 
	and a."ElementKey" = rr."ElementKey" and rr."RoleKey" = _rolekey) as a;

	select "Theme", "Palette" into _theme, _palette from hubviews.vw_clientinfo
	where "ClientKey" = _ClientKey and "Lang" = _Lang;

	select "ShowPartyCadreTab" into _spartycadretab from connecthub."ClientInfo"
	where "ClientKey" = _ClientKey;

   raise notice 'pages: %',_Pages;
	_return = json_build_object('Status','Success','Details',json_build_object('UserInfo',_UserInfo,'Pages',_Pages,'UIElements',_UiElements, 'Other', _CommonElements, 'Actions', _Actions->'Action', 'StatusList', _status_list, 'AllPages',_AllPages, 'AllActions', _AllActions,'Theme',_theme,'Palette',_palette, 'ShowPartyCadreTab', _spartycadretab));

	return _return;

	exception when others then 

    get stacked diagnostics
        _state   = returned_sqlstate,
        _msg     = message_text,
        _detail  = pg_exception_detail,
        _hint    = pg_exception_hint,
        _context = pg_exception_context;
       
     insert into connecthub."EXCEPTION_LOG" ("procedure", "state","msg", "detail", "hint", "context")
     values('ProcAfterLoggedIn', _state, _msg, _detail, _hint, _context);
    
	_return = json_build_object('Status','Failed','Details', json_build_object('State',_state, '_msg', _msg , '_detail', _detail, '_hint', _detail, '_context', _context));
--  
	return _return;

end; $function$
;



drop view hubviews.vw_pagelist;

CREATE OR REPLACE VIEW hubviews.vw_pagelist
AS SELECT a."ClientKey",
    a."PageGroupKey",
    c."PageGroup",
    a."PageKey",
    b."Lang",
    b."Page",
    a."PageTitle",
    b."Title",
    b."Description",
    a."Icon",
    a."Image",
    a."IsPublic",
    a."Class",
    a."CSS",
    a."TagConfig",
    a."ElementType",
    a."OrdinalPosition",
    c."OrdinalPosition" "PgGroupKeyOrder",
    b."TagContent",
    a."IsActive"
   FROM connecthub."PageList" a
     JOIN connecthub."PageList_Lang" b ON a."ClientKey" = b."ClientKey" AND a."PageGroupKey" = b."PageGroupKey" AND a."PageKey" = b."PageKey"
     LEFT JOIN connecthub."PageGroup" c ON a."ClientKey" = c."ClientKey" AND a."PageGroupKey" = c."PageGroupKey"
  ORDER BY c."OrdinalPosition", a."OrdinalPosition";


DROP FUNCTION connecthub.fn_task_overview;

CREATE OR REPLACE FUNCTION connecthub.fn_task_overview(_clientkey integer, _lang varchar(2),_assignedto text)
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
		    SUM(CASE WHEN "TaskStatus" = 'Pending' THEN 1 ELSE 0 END) AS "Pending",
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
		    where t."ClientKey" = _clientkey and t."Lang" = _lang --and t."AssignedTo" = 
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
		    SUM(CASE WHEN "TaskStatus" = 'Pending' THEN 1 ELSE 0 END) AS "Pending",
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
		    where t."ClientKey" = _clientkey and t."Lang" = _lang and t."AssignedTo" = _AssignedTo
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


			
drop table if exists masters."TaskCategory";	
			
CREATE TABLE masters."TaskCategory" (
    "ClientKey" int NOT NULL,
    "Lang" bpchar NOT NULL,  -- Language code (e.g., 'en', 'fr', etc.)
    "TaskCategoryID" INT  not null,  -- Unique ID for each task category
    "TaskCategory" VARCHAR(255) NOT NULL,  -- Name or description of the task category
    "CreatedBy" VARCHAR(255),  -- User who created the record
    "CreatedOn" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Timestamp when the record was created
    "ModifiedBy" VARCHAR(255),  -- User who last updated the record (optional)
    "ModifiedOn" TIMESTAMP,  -- Timestamp when the record was last updated (optional)
    "IsActive" BOOLEAN DEFAULT TRUE  -- Status of the record (active or inactive)
);

ALTER TABLE masters."TaskCategory" ADD CONSTRAINT taskcategory_unique UNIQUE ("ClientKey","TaskCategoryID");

INSERT INTO "masters"."TaskCategory" ("ClientKey", "Lang", "TaskCategoryID", "TaskCategory")
VALUES
    (1, 'en', 1, 'Personal'),
    (1, 'en', 2, 'Constituency'),
    (1, 'en', 3, 'Party'),
    (3, 'en', 1, 'Personal'),
    (3, 'en', 2, 'Constituency'),
    (3, 'en', 3, 'Party'),
    (4, 'en', 1, 'Personal'),
    (4, 'en', 2, 'Constituency'),
    (4, 'en', 3, 'Party');
   
 DROP FUNCTION if exists connecthub."ProcGetTaskCategory"(int4, bpchar, varchar);

CREATE OR REPLACE FUNCTION connecthub."ProcGetTaskCategory"(_clientkey integer, _lang character, _userid character varying)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
-- variable declaration
	_return json;
	_List json;
--exception variables
    _state   TEXT;
    _msg     TEXT;
    _detail  TEXT;
    _hint    TEXT;
    _context TEXT;
begin

/*select * from connecthub."ProcURLRouting"('{"params":{"Offset":0,"Limit":10,"Form":"TaskCategoryList"},"requestURI":"TaskCategoryList","global":{"ClientKey":1,"Lang":"en"}}');*/

	select 
	json_agg(json_build_object('ID',"TaskCategoryID", 'Value', "TaskCategory"))
	into _List  
	from masters."TaskCategory" where "ClientKey" = _ClientKey and "Lang" = _Lang;

	_return = json_build_object('Status','Success', 'Details', json_build_object('List', _List));
	
	return _return;
	
exception when others then 

    get stacked diagnostics
        _state   = returned_sqlstate,
        _msg     = message_text,
        _detail  = pg_exception_detail,
        _hint    = pg_exception_hint,
        _context = pg_exception_context;
       
            
     insert into connecthub."EXCEPTION_LOG" ("procedure", "state","msg", "detail", "hint", "context")
     values('ProcGetTaskCategory', _state, _msg, _detail, _hint, _context);
    
     _return = json_build_object('Status','Failed','Details', json_build_object('State',_state, '_msg', _msg , '_detail', _detail, '_hint', _hint, '_context', _context));

	return _return;
END;
$function$
;


ALTER TABLE "connecthub"."Task"
ADD COLUMN "TaskCategoryID" INT,
ADD CONSTRAINT "fk_taskcategory"
FOREIGN KEY ("ClientKey","TaskCategoryID") 
REFERENCES "masters"."TaskCategory"("ClientKey","TaskCategoryID");

DROP FUNCTION connecthub.fn_list_task;

CREATE OR REPLACE FUNCTION connecthub.fn_list_task(p_clientkey integer, p_assignedto uuid, p_limit integer, p_offset integer)
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
	
	select json_agg(taskobj) filter (where "SNO" >p_offset and "SNO" <=p_offset+p_limit ) into _return from 
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
    order by coalesce("LastUpdatedOn", t."CreatedOn") desc
	) as a ;

else
	select json_agg(taskobj) filter (where "SNO" >p_offset and "SNO" <=p_offset+p_limit ) into _return from 
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


DROP function if exists connecthub.fn_create_task;

CREATE OR REPLACE FUNCTION connecthub.fn_create_task(p_clientkey integer, p_lang character, p_title character varying, p_description text, p_createdby character varying, p_assignedto uuid, p_duedate date, p_attachments text, p_priority character varying, p_taskcategoryid int)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
declare 
	_List record;
	_return json;
	_taskid varchar;

--exception variables
    _state   TEXT;
    _msg     TEXT;
    _detail  TEXT;
    _hint    TEXT;
    _context TEXT;
BEGIN

select * from connecthub."GenerateUniqueTaskID"(_clientkey:=1, lang:='en', _userkey:=null) into _taskid;

    INSERT INTO connecthub."Task" (
        "TaskId", 
        "ClientKey", 
        "Lang", 
        "Title", 
        "Description", 
        "AssignedTo", 
        "DueDate", 
        "Attachments", 
        "Priority", 
        "CreatedOn", 
        "LastUpdatedOn",
        "CreatedBy",
        "TaskStatus",
		"TaskCategoryID"
    ) VALUES (
        _taskid, 
        p_ClientKey, 
        p_Lang, 
        p_Title, 
        p_Description, 
        p_AssignedTo, 
        p_DueDate, 
        string_to_array(p_Attachments, ','), 
        p_Priority, 
        current_date, 
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata')::TIMESTAMP(0),
        p_CreatedBy,
        'InProgress',
		p_taskcategoryid
    ) returning * into _List;
   
   _return = json_build_object('Status','Success', 'Details', row_to_json(_List));
	
	return _return;

exception when others then 

    get stacked diagnostics
        _state   = returned_sqlstate,
        _msg     = message_text,
        _detail  = pg_exception_detail,
        _hint    = pg_exception_hint,
        _context = pg_exception_context;
       
            
     insert into connecthub."EXCEPTION_LOG" ("procedure", "state","msg", "detail", "hint", "context")
     values('fn_create_task', _state, _msg, _detail, _hint, _context);
    
     _return = json_build_object('Status','Failed','Details', json_build_object('State',_state, '_msg', _msg , '_detail', _detail, '_hint', _hint, '_context', _context));

	return _return;
   
END;
$function$
;