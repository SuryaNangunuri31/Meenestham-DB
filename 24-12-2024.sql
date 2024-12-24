DROP FUNCTION connecthub."ProcCrudAssociationInfo";

CREATE OR REPLACE FUNCTION connecthub."ProcCrudAssociationInfo"(_clientkey integer, _lang character, _userid character varying, _operation text, _name character varying, _mobile character varying, _alternatemobile character varying, _registrationnumber text, _additionalinfo text, _locationgranularity character varying, _locationkey json, _address text, _tags text, _members json, _id integer)
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
	
	_tags = case when _tags = '' then '0' else coalesce(_tags, '0') end;
   	if _Operation = 'Insert' then 
   	
   		if not exists (select * from connecthub."AssociationInfo" where ("Mobile" = _Mobile or "AlternateMobile" = _Mobile) or ("Mobile" = _AlternateMobile or "AlternateMobile" = _AlternateMobile) ) then 
   	
	   		insert into connecthub."AssociationInfo"
	   		("ClientKey","Lang","Name","Mobile","AlternateMobile","RegistrationNumber","AdditonalInformation","LocationGranularity","LocationKey","Address","Tags","Members","CreatedBy")
	   		values (_ClientKey, _Lang, _Name, _Mobile, _AlternateMobile, _RegistrationNumber , _AdditionalInfo, _LocationGranularity, _LocationKey, _Address, string_to_array(_Tags,','), _Members, _UserID) returning "ID" into _ID; 
	   	
	   		select json_agg(row_to_json(a)) 
	   		into _List 
--	   		from hubviews."vw_unionInformation" as a where "ClientKey" = _ClientKey and "ID" = _ID and "IsActive" = true and "Lang" = _Lang;
		   	from hubviews."fn_unionInformation"() as a where "ClientKey" = _ClientKey and "ID" = _ID and "IsActive" = true and "Lang" = _Lang;
   		
	   		_return = json_build_object('Status','Success', 'Details', json_build_object('NewID',_ID, 'List', _List));
	   	
	   	else 
	   		_return = json_build_object('Status','Success', 'Details', 'Given Mobile Number Already Exists.');
	   	end if;
   	
   	elsif _Operation = 'Update' then  
   		
   		UPDATE connecthub."AssociationInfo"
		SET
		    "ClientKey" = _ClientKey,
		    "Lang" = _Lang,
		    "Name" = _Name,
		    "Mobile" = _Mobile,
		    "AlternateMobile" = _AlternateMobile,
		    "RegistrationNumber" = _RegistrationNumber,
		    "AdditonalInformation" = _AdditionalInfo,
		    "LocationGranularity" = _LocationGranularity,
		    "LocationKey" = _LocationKey,
		    "Address" = _Address,
		    "Tags" = string_to_array(_Tags, ','),
		    "Members" = _Members,
		    "CreatedBy" = _UserID
		WHERE "ID" = _ID and "ClientKey" = _ClientKey and "Lang" = _Lang;
	
		select json_agg(row_to_json(a)) 
   		into _List 
   		from 
   		(select 
   		"ID","ID" "AssociationID","ClientKey","Lang","Name","AssociationName","Mobile","AlternateMobile","RegistrationNumber","AdditonalInformation","LocationGranularity","LocationKey","Address","Tags","Members","IsActive","CreatedOn","CreatedBy","ModifiedOn","ModifiedBy"
--   		from hubviews."vw_unionInformation") as a where "ClientKey" = _ClientKey and "ID" = _ID and "IsActive" = true and "Lang" = _Lang;
		   	from hubviews."fn_unionInformation"()) as a where "ClientKey" = _ClientKey and "ID" = _ID and "IsActive" = true and "Lang" = _Lang;
   	
   		
   		_return = json_build_object('Status','Success', 'Details', json_build_object('NewID',_ID, 'List', _List));
   	   	
	elsif _Operation = 'Delete' then 
	
	elsif _Operation = 'Get' then
	
--   		select 
--		json_agg(jsonb_build_object('AssociationID', "ID",  'Value', "Name"||coalesce('-'||"LocationGranularity",'')||coalesce('-'||"State", '')||coalesce('-'||"District",'')||coalesce('-'||"Mandal",'')||coalesce('-'||"Village",''), 'Record', row_to_json(a)) order by "ID") 
   		select json_agg(row_to_json(a))   
   		into _List
   		from (
   		select a.*, b.grievance_details 
--   		from hubviews."vw_unionInformation"  as a
   		from hubviews."fn_unionInformation"()  as a
   		left join hubviews.vw_requestor_grievance_details as b on a."ClientKey" = b.client_key and a."Lang" = b.lang and b.requestor_type = 9 and a."AssociationID" = b.id 
   		) a
   		where "ClientKey" = _ClientKey 
   		and case when _ID is null then '1' else "ID" end = coalesce(_ID, '1')
   		and (lower("AssociationName") like '%'||lower(coalesce(_name,"AssociationName"))||'%')
   		and ("Mobile" = coalesce(_Mobile, "Mobile") or "AlternateMobile" = coalesce(_Mobile,"AlternateMobile"))
   		and "IsActive" = true and "Lang" = _Lang;
   	
   		_return = json_build_object('Status','Success', 'Details', json_build_object('List', _List));

	end if;

	return _return;
	
exception when others then 

    get stacked diagnostics
        _state   = returned_sqlstate,
        _msg     = message_text,
        _detail  = pg_exception_detail,
        _hint    = pg_exception_hint,
        _context = pg_exception_context;
       
            
     insert into connecthub."EXCEPTION_LOG" ("procedure", "state","msg", "detail", "hint", "context")
     values('ProcCrudAssociationInfo', _state, _msg, _detail, _hint, _context);
    
     if _state = '23505' then 
     	_return = json_build_object('Status','Failed','Details', 'Association'||' '||quote_Ident(_Name)||' is already exists, kindly select from the list.');
     else 
     	_return = json_build_object('Status','Failed','Details', json_build_object('State',_state, '_msg', _msg , '_detail', _detail, '_hint', _hint, '_context', _context));
     end if;

	return _return;
END;
$function$
;

DROP FUNCTION hubviews."fn_unionInformation"();

CREATE OR REPLACE FUNCTION hubviews."fn_unionInformation"()
 RETURNS TABLE("ID" integer, "AssociationID" integer, "ClientKey" integer, "Lang" character, "Name" character varying, "AssociationName" character varying, "Mobile" character varying, "AlternateMobile" character varying, "RegistrationNumber" character varying, "AdditonalInformation" text, "LocationGranularity" character varying, "LocationKey" json, "StateKey" integer, "State" text, "DistrictKey" integer, "District" text, "MandalKey" integer, "Mandal" text, "VillageKey" integer, "Village" text, "Address" text, "Tags" json, "Members" json, "IsActive" boolean, "CreatedOn" timestamp without time zone, "CreatedBy" character varying, "ModifiedOn" timestamp without time zone, "ModifiedBy" character varying)
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT 
        a."ID",
        a."AssociationID",
        a."ClientKey",
        a."Lang",
        a."Name",
        a."AssociationName",
        a."Mobile",
        a."AlternateMobile",
        a."RegistrationNumber",
        a."AdditonalInformation",
        a."LocationGranularity",
        COALESCE(a."LocationKey"::text::json, '[]'::json) AS "LocationKey",
        a."StateKey",
        a."State",
        a."DistrictKey",
        a."District",
        a."MandalKey",
        a."Mandal",
        a."VillageKey",
        a."Village",
        a."Address",
        CASE
            WHEN json_agg(
                CASE
                    WHEN b."ID" = 0 THEN '{}'::json
                    ELSE json_build_object('ID', b."ID", 'Value', b."Tags")
                END)::text = '[{}]'::text THEN '[]'::json
            ELSE json_agg(
                CASE
                    WHEN b."ID" = 0 THEN '{}'::json
                    ELSE json_build_object('ID', b."ID", 'Value', b."Tags")
                END)
        END AS "Tags",
        COALESCE(a."Members"::text::json, '[]'::json) AS "Members",
        a."IsActive",
        a."CreatedOn",
        a."CreatedBy",
        a."ModifiedOn",
        a."ModifiedBy"
    FROM (
        SELECT 
            "AssociationInfo"."ID",
            "AssociationInfo"."ID" AS "AssociationID",
            "AssociationInfo"."ClientKey",
            "AssociationInfo"."Lang",
            "AssociationInfo"."Name",
            "AssociationInfo"."Name" AS "AssociationName",
            "AssociationInfo"."Mobile",
            "AssociationInfo"."AlternateMobile",
            "AssociationInfo"."RegistrationNumber",
            "AssociationInfo"."AdditonalInformation",
            "AssociationInfo"."LocationGranularity",
            "AssociationInfo"."LocationKey",
            ((("AssociationInfo"."LocationKey" -> 0) -> 'StateKey'::text) ->> 'ID'::text)::integer AS "StateKey",
            (("AssociationInfo"."LocationKey" -> 0) -> 'StateKey'::text) ->> 'Value'::text AS "State",
            ((("AssociationInfo"."LocationKey" -> 0) -> 'DistrictKey'::text) ->> 'ID'::text)::integer AS "DistrictKey",
            (("AssociationInfo"."LocationKey" -> 0) -> 'DistrictKey'::text) ->> 'Value'::text AS "District",
            ((("AssociationInfo"."LocationKey" -> 0) -> 'MandalKey'::text) ->> 'ID'::text)::integer AS "MandalKey",
            (("AssociationInfo"."LocationKey" -> 0) -> 'MandalKey'::text) ->> 'Value'::text AS "Mandal",
            ((("AssociationInfo"."LocationKey" -> 0) -> 'VillageKey'::text) ->> 'ID'::text)::integer AS "VillageKey",
            (("AssociationInfo"."LocationKey" -> 0) -> 'VillageKey'::text) ->> 'Value'::text AS "Village",
            "AssociationInfo"."Address",
            unnest("AssociationInfo"."Tags") AS "TagID",
            "AssociationInfo"."Members",
            "AssociationInfo"."IsActive",
            "AssociationInfo"."CreatedOn",
            "AssociationInfo"."CreatedBy",
            "AssociationInfo"."ModifiedOn",
            "AssociationInfo"."ModifiedBy"
        FROM connecthub."AssociationInfo"
    ) a
    LEFT JOIN masters."TagList" b 
    ON a."TagID" = b."ID"::text 
    AND a."ClientKey" = b."ClientKey" 
    AND a."Lang" = b."Lang"
    GROUP BY 
        a."ID", 
        a."AssociationID", 
        a."ClientKey", 
        a."Lang", 
        a."Name", 
        a."AssociationName", 
        a."Mobile", 
        a."AlternateMobile", 
        a."RegistrationNumber", 
        a."AdditonalInformation", 
        a."LocationGranularity", 
        (a."LocationKey"::text), 
        a."StateKey", 
        a."State", 
        a."DistrictKey", 
        a."District", 
        a."MandalKey", 
        a."Mandal", 
        a."VillageKey", 
        a."Village", 
        a."Address", 
        (a."Members"::text), 
        a."IsActive", 
        a."CreatedOn", 
        a."CreatedBy", 
        a."ModifiedOn", 
        a."ModifiedBy";
END;
$function$
;

DROP FUNCTION connecthub."_4_dataset_get_sql"(jsonb);

CREATE OR REPLACE FUNCTION connecthub._4_dataset_get_sql(input jsonb)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
_location_keys text;
_area_uom text;
_area_uom_text text;
_location_granularity text;
_location_join text;
_number_of_decimals text;
_select_columns text;
_where_clause text := ' ';
_having_clause text := ' ';
_sortby_clause text := ' ';
_limit integer;
_offset integer;
_limit_string text;
_offset_string text;
_final_select text:='*';
_overallresultrows integer;
_subresultrows integer;
_final_groupby text:='';
sql_query text;
_final_numeric_variables text := '';
_search_clause text := ' ';
_finalwhereclause text='';
_UserID text;
_clientkey int;
_profilekey int;
_role_condition text;
begin

--_location_keys := input->'variables'->'location_keys'->>'value';
--_select_columns := input->'variables'->'location_keys'->>'value';
--_area_uom := input->'variables'->'area_uom'->>'value';
--_area_uom_text := input->'variables'->'area_uom_value'->>'value';
--_location_granularity := input->'variables'->'location_granularity'->>'value';
--_location_join := input->'variables'->'location_join'->>'value';
--_number_of_decimals := (input->'variables'->'number_of_decimals'->>'value')::text;
_select_columns := replace(replace((input->>'_select_array_jsonb'),'[',''),']','');
_where_clause := (input->>'_where_clause');
--_search_clause := (input->>'_search_clause');
--_having_clause := (input->>'_having_clause');
_sortby_clause := (input->>'_sortby_clause');
_UserID := (input->>'UserID');
_clientkey := (input->>'ClientKey')::int;
_limit := (input->>'limit')::integer;
_offset := (input->>'offset')::integer;
--_overallresultrows:= (input->'gridoptions'->>'overallresultrows')::integer;
--_subresultrows:= (input->'gridoptions'->>'subresultrows')::integer;

--if _overallresultrows = 1 or _subresultrows = 1 then 
--	select fn_data_explorer_grouping_rollup 
--	into _final_select
--	from reporting.fn_data_explorer_grouping_rollup((input->>'_select_array_jsonb')::jsonb);
--	
--	select fn_data_explorer_groupby_rollup
--	into _final_groupby
--	from reporting.fn_data_explorer_groupby_rollup((input->>'_select_array_jsonb')::jsonb);
--
--end if;

_finalwhereclause =' ';

if _offset >= 0 then

	_limit_string := ' limit '|| _limit::text;
	_offset_string := ' offset '|| _offset::text;

else

	_limit_string := '';
	_offset_string := '';

end if;

if trim(_where_clause) <> '' then
	_where_clause := ' where ' || _where_clause;
end if;

--if trim(_search_clause) <> '' and trim(_where_clause) <> '' then
--	_where_clause := _where_clause || ' and ' || _search_clause;
--elsif trim(_search_clause) <> '' and trim(_where_clause) = '' then
--	_where_clause := ' where ' || _search_clause;
--elsif trim(_search_clause) = '' and trim(_where_clause) <> '' then
--	_where_clause := _where_clause;
--else
--      _where_clause := _where_clause;
--end if;

--if trim(_having_clause) <> '' then
--	_having_clause := ' where ' || _having_clause;
--end if;


if trim(_sortby_clause) <> '' then
	_sortby_clause := ' order by ' || _sortby_clause;
end if;


select "ProfileKey" into _profilekey from connecthub."UserProfiles" where  "UserKey" = _UserID;


if _profilekey = 1 then 
	_role_condition = '"ClientKey" = '||_clientkey||' and grievance_status_key <> 7 ';
elsif _profilekey = 2 then 
	_role_condition = '"ClientKey" = '||_clientkey||' and grievance_status_key <> 7 ';
elsif _profilekey = 3 then
--	_role_condition = '"ClientKey" = '||_clientkey||' and grievance_status_key <> 7 and (created_by_key = '|| quote_literal(_UserID)||' or ( grievance_status_key in (6,3,4) and assignee_to_key = '||quote_literal(_UserID) ||') ) ' ;
	_role_condition = '"ClientKey" = '||_clientkey||' and grievance_status_key <> 7 and (created_by_key = '|| quote_literal(_UserID)||' or ( grievance_status_key in (1,6,3,4)) ) ' ;
elsif _profilekey = 4 then
	_role_condition = '"ClientKey" = '||_clientkey||' and grievance_status_key <> 7 and ( grievance_status_key = 1 or "assignee_to_key" = '|| quote_literal(_UserID)||' or created_by_key = '|| quote_literal(_UserID)||')';
elsif _profilekey = 5 then
	_role_condition = '"ClientKey" = '||_clientkey||'  and grievance_status_key <> 7 and created_by_key = '|| quote_literal(_UserID)  ;
else 
	_role_condition = '"ClientKey" = '||_clientkey||' and grievance_status_key <> 7 ' ;
end if;


raise notice 'select %, where %, having %', _select_columns, _where_clause, _having_clause;

sql_query = 
'SELECT json_agg(row_to_json(t)) FROM ( select '||_select_columns||' from (
with base_data as (
select '||_select_columns||', grievance_status_key
--from hubviews."vw_grievancedataset"
from hubviews."fn_grievancedataset"()
'||_where_clause||
case when _where_clause is null or _where_clause= '' then ' where '||_role_condition  
	when _where_clause is not null or _where_clause <> '' then ' and '||_role_condition else '' end 
	||' )
select '||_select_columns||' from base_data 
order by   
case when grievance_status_key = 5 then created_on end desc,
case when grievance_status_key = 4 then created_on end desc,
case when grievance_status_key = 3 then created_on end desc,
case when grievance_status_key = 0 then created_on end desc,
case when grievance_status_key = 6 then created_on end desc,
case when grievance_status_key = 1 then created_on end desc,
--case when grievance_status_key = 1 and priority = ''Low'' then created_on end desc,
--case when grievance_status_key = 1 and priority = ''Medium'' then created_on end desc,
--case when grievance_status_key = 1 and priority = ''High'' then created_on end desc,


--case when grievance_status_key = 6 and priority = ''Low'' then created_on end desc,
--case when grievance_status_key = 6 and priority = ''Medium'' then created_on end desc,
--case when grievance_status_key = 6 and priority = ''High'' then created_on end desc,


--case when grievance_status_key = 0 and priority = ''Low'' then created_on end desc,
--case when grievance_status_key = 0 and priority = ''Medium'' then created_on end desc,
--case when grievance_status_key = 0 and priority = ''High'' then created_on end desc,


--case when grievance_status_key = 3 and priority = ''Low'' then created_on end desc,
--case when grievance_status_key = 3 and priority = ''Medium'' then created_on end desc,
--case when grievance_status_key = 3 and priority = ''High'' then created_on end desc,


--case when grievance_status_key = 4 and priority = ''Low'' then created_on end desc,
--case when grievance_status_key = 4 and priority = ''Medium'' then created_on end desc,
--case when grievance_status_key = 4 and priority = ''High'' then created_on end desc,


--case when grievance_status_key = 5 and priority = ''Low''  then created_on end desc,
--case when grievance_status_key = 5 and priority = ''Medium''  then created_on end desc,
--case when grievance_status_key = 5 and priority = ''High''  then created_on end desc,
created_on desc
) subquery ' ||_sortby_clause || _limit_string::text ||  _offset_string::text ||') t';


raise notice '%', 'Begin of Final Sql Query';
raise notice '%', '------------------------';
raise notice '%', sql_query;
raise notice '%', '------------------------';
raise notice '%', 'End of Final Sql Query';

return sql_query;

END;
$function$
;


DROP FUNCTION connecthub."_4_dataset_get_sql_count";

CREATE OR REPLACE FUNCTION connecthub._4_dataset_get_sql_count(input jsonb)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
_location_keys text;
_area_uom text;
_area_uom_text text;
_location_granularity text;
_location_join text;
_number_of_decimals text;
_select_columns text;
_where_clause text := ' ';
_having_clause text := ' ';
_limit text;
_offset text;
_search_clause text := ' ';
_UserID text;
_clientkey int;
_profilekey int;
_role_condition text;
sql_query text;

begin

--_location_keys := input->'variables'->'location_keys'->>'value';
--_select_columns := input->'variables'->'location_keys'->>'value';
--_area_uom := input->'variables'->'area_uom'->>'value';
--_area_uom_text := input->'variables'->'area_uom_value'->>'value';
--_location_granularity := input->'variables'->'location_granularity'->>'value';
--_location_join := input->'variables'->'location_join'->>'value';
--_number_of_decimals := (input->'variables'->'number_of_decimals'->>'value')::text;
_select_columns := replace(replace((input->>'_select_array_jsonb'),'[',''),']','');
_where_clause := (input->>'_where_clause');
_UserID := (input->>'UserID');
_clientkey := (input->>'ClientKey')::int;
--_search_clause := (input->>'_search_clause');
--_having_clause := (input->>'_having_clause');
_limit := (input->>'limit');
_offset := (input->>'offset');


if trim(_where_clause) <> '' then
	_where_clause := ' where ' || _where_clause;
end if;

--if trim(_search_clause) <> '' and trim(_where_clause) <> '' then
--	_where_clause := _where_clause || ' and ' || _search_clause;
--elsif trim(_search_clause) <> '' and trim(_where_clause) = '' then
--	_where_clause := ' where ' || _search_clause;
--elsif trim(_search_clause) = '' and trim(_where_clause) <> '' then
--	_where_clause := _where_clause;
--else
--      _where_clause := _where_clause;
--end if;

--if trim(_having_clause) <> '' then
--	_having_clause := ' where ' || _having_clause;
--end if;



select "ProfileKey" into _profilekey from connecthub."UserProfiles" where  "UserKey" = _UserID;


if _profilekey = 1 then 
	_role_condition = '"ClientKey" = '||_clientkey||' and grievance_status_key <> 7 ';
elsif _profilekey = 2 then 
	_role_condition = '"ClientKey" = '||_clientkey||' and grievance_status_key <> 7 ';
elsif _profilekey = 3 then
--	_role_condition = '"ClientKey" = '||_clientkey||' and grievance_status_key <> 7 and (created_by_key = '|| quote_literal(_UserID)||' or ( grievance_status_key in (6,3,4) and assignee_to_key = '||quote_literal(_UserID) ||') ) ' ;
	_role_condition = '"ClientKey" = '||_clientkey||' and grievance_status_key <> 7 and (created_by_key = '|| quote_literal(_UserID)||' or ( grievance_status_key in (1,6,3,4)) ) ' ;
elsif _profilekey = 4 then
	_role_condition = '"ClientKey" = '||_clientkey||' and grievance_status_key <> 7 and ( grievance_status_key = 1 or "assignee_to_key" = '|| quote_literal(_UserID)||' or created_by_key = '|| quote_literal(_UserID)||')';
elsif _profilekey = 5 then
	_role_condition = '"ClientKey" = '||_clientkey||'  and grievance_status_key <> 7 and created_by_key = '|| quote_literal(_UserID)  ;
else 
	_role_condition = '"ClientKey" = '||_clientkey||' and grievance_status_key <> 7 ' ;
end if;



raise notice 'select %, where %', _select_columns, _where_clause;

sql_query = 
'select count(*) from (
with base_data as (
select '||_select_columns||'
--from hubviews."vw_grievancedataset" 
from hubviews."fn_grievancedataset"() 
'||_where_clause||
case when _where_clause is null or _where_clause= '' then ' where '||_role_condition  
	when _where_clause is not null or _where_clause <> '' then ' and '||_role_condition else '' end 
	||' )
select * from base_data
) subquery ';

return sql_query;
END;
$function$
;

DROP FUNCTION hubviews.fn_grievancedataset();

CREATE OR REPLACE FUNCTION hubviews.fn_grievancedataset()
 RETURNS TABLE("ClientKey" integer, "Lang" character, grievance_key character varying, grievance_type_key integer, grievance_type text, grievance_text text, no_of_attachments_in_grievance integer, association_id integer[], requested_for json, requested_by json, refered_by json, requestor_ac_key integer[], requestor_state_key integer[], requestor_district_key integer[], requestor_mandal_key integer[], requestor_village_key integer[], requestor_type integer[], gender text[], mobile text[], email text[], requestor_tags text[], age_group text[], tags text[], additional_info json, priority character varying, sla text, due_date date, grievance_status_key integer, grievance_status character varying, is_active boolean, remarks text, age_in_days numeric, age_category text, created_on timestamp without time zone, modified_on timestamp without time zone, created_by_key character varying, created_by character varying, modified_by character varying, note text, location_granularity character varying, request_state_key integer, request_state text, request_district_key integer, request_district text, request_pc_key integer, request_pc_name character varying, request_ac_key integer, request_ac_name character varying, request_mandal_key integer, request_mandal text, request_village_key integer, request_village text, assignee_to_key text, assigned_to character varying, source character varying, department text, departmentkey text, hod text, hodkey text, subject text, subjectkey text, subsubject text, subsubjectkey integer, party_cadre_status character varying, booth_incharge text, booth_incharge_mobile text, unit_incharge text, unit_incharge_mobile text, cluster_incharge text, cluster_incharge_mobile text)
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    WITH grievance_data AS (
         SELECT a_1."ClientKey",
            a_1."Lang",
            a_1."GrievanceKey",
            a_1."GrievanceType",
            a_1."GrievanceTypeName",
            a_1."GrievanceText",
            a_1."Attachments",
            b."ReferenceRelation",
                CASE
                    WHEN b."ReferenceType"::text = 'Association'::text THEN b."ReferenceID"::text
                    ELSE NULL::text
                END AS "AssociationID",
            json_build_object('Sno', row_number() OVER (PARTITION BY a_1."GrievanceKey", b."ReferenceRelation"), 'ID', b."ReferenceID", 'Label',
                CASE
                    WHEN b."ReferenceType"::text = 'Association'::text THEN ((b."Name"::text || ' ('::text) || b."ReferenceType"::text) || ')'::text
                    WHEN b."ReferenceType"::text = 'Individual'::text THEN ((((COALESCE(b."RequestorFirstName"::text, ''::text) || ' '::text) || COALESCE(b."RequestorLastName"::text, ''::text)) || ' ('::text) || b."ReferenceType"::text) || ')'::text
                    ELSE NULL::text
                END, 'ReferenceType', b."ReferenceType") AS obj,
            b."ConstituencyKey"::text AS "RequestorACKey",
            b."StateKey"::text AS "RequestorStateKey",
            b."DistrictKey"::text AS "RequestorDistrictKey",
            b."MandalKey"::text AS "RequestorMandalKey",
            b."VillageKey"::text AS "RequestorVillageKey",
            a_1."Tags",
            a_1."AdditionalInfo",
            a_1."Priority",
                CASE
                    WHEN a_1."DueDate" > CURRENT_DATE THEN 'Beyond SLA'::text
                    ELSE 'Within SLA'::text
                END AS sla,
            a_1."DueDate",
            a_1."GrievanceStatus",
            s_1_1."Status",
            a_1."IsActive",
            a_1."Remarks",
            EXTRACT(day FROM CURRENT_DATE::timestamp without time zone - a_1."CreatedOn") AS age_in_days,
            a_1."CreatedOn",
            a_1."ModifiedOn",
            a_1."CreatedBy",
            a_1."CreatedUserName",
            a_1."ModifiedUserName",
            a_1."Note",
            a_1."IsLocationSpecific",
            a_1."LocationGranularity",
            a_1."LocationKey",
            b."RequestorType"::text AS "RequestorType",
                CASE
                    WHEN b."Age"::text::integer < 18 THEN 'Minor'::text
                    WHEN b."Age"::text::integer >= 18 AND b."Age"::text::integer <= 25 THEN 'AgeGroup(18-25)'::text
                    WHEN b."Age"::text::integer >= 26 AND b."Age"::text::integer <= 30 THEN 'AgeGroup(26-30)'::text
                    WHEN b."Age"::text::integer >= 31 AND b."Age"::text::integer <= 35 THEN 'AgeGroup(31-35)'::text
                    WHEN b."Age"::text::integer >= 36 AND b."Age"::text::integer <= 40 THEN 'AgeGroup(36-40)'::text
                    WHEN b."Age"::text::integer >= 41 AND b."Age"::text::integer <= 45 THEN 'AgeGroup(41-45)'::text
                    WHEN b."Age"::text::integer >= 46 AND b."Age"::text::integer <= 50 THEN 'AgeGroup(46-50)'::text
                    WHEN b."Age"::text::integer >= 51 AND b."Age"::text::integer <= 55 THEN 'AgeGroup(51-55)'::text
                    WHEN b."Age"::text::integer >= 56 AND b."Age"::text::integer <= 60 THEN 'AgeGroup(56-60)'::text
                    WHEN b."Age"::text::integer >= 61 AND b."Age"::text::integer <= 65 THEN 'AgeGroup(61-65)'::text
                    WHEN b."Age"::text::integer >= 66 AND b."Age"::text::integer <= 70 THEN 'AgeGroup(66-70)'::text
                    WHEN b."Age"::text::integer >= 71 AND b."Age"::text::integer <= 75 THEN 'AgeGroup(71-75)'::text
                    WHEN b."Age"::text::integer > 75 THEN 'AgeGroup(>75)'::text
                    ELSE NULL::text
                END AS "AgeGroup",
            b."Gender"::text AS "Gender",
            ((COALESCE(b."Mobile"::text, ''::text) || COALESCE(','::text || b."AlternateMobile"::text, ''::text)) || COALESCE(','::text || b."RelationContact"::text, ''::text)) || COALESCE(','::text || b."RelationAlternateContact"::text, ''::text) AS "Mobile",
            b."Email"::text AS "Email",
            b."Occupation"::text AS "Occupation",
            b."Tags"::text AS "RequestorTags",
            a_1."AssignedTo",
            u."UserName" AS "AssignedName",
            a_1."Source",
            s_1."Department" AS "HOD",
            s_1."DepartmentKey" AS "HODKey",
            a_1."Department" AS "Department",
            a_1."DepartmentCode"::text AS "DepartmentKey",
            s_1."Subject",
            s_1."SubjectKey",
            s_1."SubSubject",
            a_1."SubSubjectCode",
            a_1."PartyCadreStatus",
            v_1.booth_incharge,
            v_1.booth_incharge_mobile,
            v_1.unit_incharge,
            v_1.unit_incharge_mobile,
            v_1.cluster_incharge,
            v_1.cluster_incharge_mobile
           FROM connecthub."GrievanceInfo" a_1
             LEFT JOIN ( SELECT COALESCE(a_2."HODKey"::text, b_1."HODKey") AS "HODKey",
                    COALESCE(a_2."HOD", b_1."HOD") AS "HOD",
                    COALESCE(a_2."DepartmentKey"::text, b_1."DepartmentKey") AS "DepartmentKey",
                    COALESCE(a_2."Department", b_1."Department") AS "Department",
                    b_1."SubjectKey",
                    b_1."Subject",
                    COALESCE(a_2."ID"::text, b_1."SubSubjectKey") AS "SubSubjectKey",
                    COALESCE(a_2."GrievanceType", b_1."SubSubject"::character varying)::text AS "SubSubject",
                    b_1."ID",
                    COALESCE(a_2."ClientKey", b_1."ClientKey") AS "ClientKey",
                    b_1."Lang",
                    b_1."Officer",
                    b_1."Title",
                    b_1."Description"
                   FROM masters."Subject" b_1
                     RIGHT JOIN connecthub."GrievanceType" a_2 ON a_2."ID" = b_1."SubSubjectKey"::integer AND a_2."ClientKey" = b_1."ClientKey" AND a_2."Lang" = b_1."Lang") s_1 ON s_1."SubSubjectKey"::integer = COALESCE(a_1."SubSubjectCode", a_1."GrievanceType") AND s_1."ClientKey" = a_1."ClientKey"
             LEFT JOIN ( SELECT p_2."ClientKey",
                    p_2."PriorityKey",
                    p_2."CreatedBy",
                    p_2."CreatedOn",
                    p_2."ModifiedBy",
                    p_2."ModifiedOn",
                    p_2."SLA",
                    pl.text
                   FROM masters."Priority" p_2
                     JOIN masters."Priority_Lang" pl ON p_2."ClientKey" = pl."ClientKey" AND p_2."PriorityKey" = pl."PriorityKey") p_1 ON p_1."ClientKey" = a_1."ClientKey" AND p_1.text::text = a_1."Priority"::text
             LEFT JOIN masters."Status" s_1_1 ON s_1_1."ClientKey" = a_1."ClientKey" AND s_1_1."StatusKey" = a_1."GrievanceStatus"
             LEFT JOIN ( SELECT a_2."ReferenceID",
                    a_2."ClientKey",
                    a_2."Lang",
                    a_2."ReferenceRelation",
                    a_2."ReferenceType",
                    a_2."ID",
                    a_2."ConstituencyKey",
                    a_2."StateKey",
                    a_2."DistrictKey",
                    a_2."MandalKey",
                    a_2."VillageKey",
                    a_2."Mobile",
                    a_2."AlternateMobile",
                    a_2."RelationContact",
                    a_2."RelationAlternateContact",
                    a_2."Name",
                    a_2."RequestorFirstName",
                    a_2."RequestorLastName",
                    a_2."GrievanceKey",
                    a_2."RequestorType",
                    a_2."RequestorTypeName",
                    a_2."Gender",
                    a_2."Age",
                    a_2."Email",
                    a_2."Occupation",
                    array_agg(COALESCE(tl."ReportingCategory", a_2."Tag")) AS "Tags",
                    a_2."VoterID",
                    a_2."BoothID"
                   FROM ( SELECT a_3."ReferenceID",
                            a_3."ClientKey",
                            a_3."Lang",
                            a_3."ReferenceRelation",
                            a_3."ReferenceType",
                            a_3."ID",
                            a_3."ConstituencyKey",
                            a_3."StateKey",
                            a_3."DistrictKey",
                            a_3."MandalKey",
                            a_3."VillageKey",
                            a_3."Mobile",
                            a_3."AlternateMobile",
                            a_3."RelationContact",
                            a_3."RelationAlternateContact",
                            a_3."Name",
                            a_3."RequestorFirstName",
                            a_3."RequestorLastName",
                            a_3."GrievanceKey",
                            a_3."RequestorType",
                            a_3."RequestorTypeName",
                            a_3."Gender",
                            a_3."Age",
                            a_3."Email",
                            a_3."Occupation",
                            a_3."Tags",
                            unnest(a_3."Tags") AS "Tag",
                            a_3."VoterID",
                            a_3."BoothID"
                           FROM ( SELECT b_1."ReferenceID",
                                    b_1."ClientKey",
                                    b_1."Lang",
                                    b_1."ReferenceRelation",
                                    b_1."ReferenceType",
                                    COALESCE(c."AssociationID", a_2_1."RequestorID") AS "ID",
                                    a_2_1."ConstituencyKey",
                                    COALESCE(c."StateKey", a_2_1."StateKey") AS "StateKey",
                                    COALESCE(c."DistrictKey", a_2_1."DistrictKey") AS "DistrictKey",
                                    COALESCE(c."MandalKey", a_2_1."MandalKey") AS "MandalKey",
                                    COALESCE(c."VillageKey", a_2_1."VillageKey") AS "VillageKey",
                                    COALESCE(c."Mobile", a_2_1."Mobile") AS "Mobile",
                                    COALESCE(c."AlternateMobile", a_2_1."AlternateMobile") AS "AlternateMobile",
                                    a_2_1."RelationContact",
                                    a_2_1."RelationAlternateContact",
                                    c."Name",
                                    a_2_1."RequestorFirstName",
                                    a_2_1."RequestorLastName",
                                    b_1."GrievanceKey",
                                    COALESCE(a_2_1."RequestorType", 9) AS "RequestorType",
                                    COALESCE(a_2_1."RequestorTypeName", 'Association'::character varying) AS "RequestorTypeName",
                                    a_2_1."Gender",
                                    a_2_1."Age",
                                    a_2_1."Email",
                                    a_2_1."Occupation",
                                    COALESCE(c."Tags", a_2_1."Tags") AS "Tags",
                                    a_2_1."VoterID",
                                    a_2_1."BoothID"
                                   FROM connecthub."RequestRequestorList" b_1
                                     LEFT JOIN hubviews.requestor_details_v1 a_2_1 ON a_2_1."ClientKey" = b_1."ClientKey" AND a_2_1."Lang" = b_1."Lang" AND a_2_1."RequestorID" = b_1."ReferenceID" AND b_1."ReferenceType"::text = 'Individual'::text
                                     LEFT JOIN hubviews."vw_unionInfo" c ON c."ClientKey" = b_1."ClientKey" AND c."Lang" = b_1."Lang" AND b_1."ReferenceType"::text = 'Association'::text AND c."AssociationID" = b_1."ReferenceID") a_3) a_2
                     LEFT JOIN ( SELECT "TagList"."ID",
                            "TagList"."TagType",
                            "TagList"."Tags",
                            "TagList"."IsActive",
                            "TagList"."ClientKey",
                            "TagList"."Lang",
                            "TagList"."ReportingCategory"
                           FROM masters."TagList"
                          WHERE ("TagList"."TagType"::text = ANY (ARRAY['AssociationType'::character varying::text, 'RequestorType'::character varying::text])) AND "TagList"."Lang" = 'en'::bpchar) tl ON tl."Tags"::text = a_2."Tag" AND tl."ClientKey" = a_2."ClientKey"
                  GROUP BY a_2."ReferenceID", a_2."ClientKey", a_2."Lang", a_2."ReferenceRelation", a_2."ReferenceType", a_2."ID", a_2."ConstituencyKey", a_2."StateKey", a_2."DistrictKey", a_2."MandalKey", a_2."VillageKey", a_2."Mobile", a_2."AlternateMobile", a_2."RelationContact", a_2."RelationAlternateContact", a_2."Name", a_2."RequestorFirstName", a_2."RequestorLastName", a_2."GrievanceKey", a_2."RequestorType", a_2."RequestorTypeName", a_2."Gender", a_2."Age", a_2."Email", a_2."Occupation", a_2."VoterID", a_2."BoothID") b ON a_1."ClientKey" = b."ClientKey" AND a_1."Lang" = b."Lang" AND a_1."GrievanceKey"::text = b."GrievanceKey"::text
             LEFT JOIN connecthub."UserInfo" u ON u."ClientKey" = a_1."ClientKey" AND a_1."AssignedTo" = u."UserKey"::text
             LEFT JOIN hubviews.vw_party_incharges_publicdata v_1 ON v_1.booth = b."BoothID"
          WHERE a_1."IsActive" = true AND a_1."GrievanceStatus" <> 7
        ), grievance_aggregated AS (
         SELECT grievance_data."ClientKey",
            grievance_data."Lang",
            grievance_data."GrievanceKey",
            grievance_data."GrievanceType",
            grievance_data."GrievanceTypeName",
            grievance_data."GrievanceText",
            grievance_data."Attachments",
            string_to_array(string_agg(grievance_data."AssociationID", ','::text), ','::text)::integer[] AS "AssociationID",
                CASE
                    WHEN grievance_data."ReferenceRelation"::text = 'RequestedFor'::text THEN json_agg(grievance_data.obj)::text
                    ELSE NULL::text
                END AS "RequestedFor",
                CASE
                    WHEN grievance_data."ReferenceRelation"::text = 'RequestedBy'::text THEN json_agg(grievance_data.obj)::text
                    ELSE NULL::text
                END AS "RequestedBy",
                CASE
                    WHEN grievance_data."ReferenceRelation"::text = 'ReferedBy'::text THEN json_agg(grievance_data.obj)::text
                    ELSE NULL::text
                END AS "ReferedBy",
            connecthub.array_distinct(string_to_array(string_agg(grievance_data."RequestorACKey", ','::text), ','::text)::integer[]) AS "RequestorACKey",
            connecthub.array_distinct(string_to_array(string_agg(grievance_data."RequestorStateKey", ','::text), ','::text)::integer[]) AS "RequestorStateKey",
            connecthub.array_distinct(string_to_array(string_agg(grievance_data."RequestorDistrictKey", ','::text), ','::text)::integer[]) AS "RequestorDistrictKey",
            connecthub.array_distinct(string_to_array(string_agg(grievance_data."RequestorMandalKey", ','::text), ','::text)::integer[]) AS "RequestorMandalKey",
            connecthub.array_distinct(string_to_array(string_agg(grievance_data."RequestorVillageKey", ','::text), ','::text)::integer[]) AS "RequestorVillageKey",
            grievance_data."Tags",
            grievance_data."AdditionalInfo"::text AS "AdditionalInfo",
            grievance_data."Priority",
            grievance_data.sla,
            grievance_data."DueDate",
            grievance_data."GrievanceStatus",
            grievance_data."Status",
            grievance_data."IsActive",
            grievance_data."Remarks",
            grievance_data.age_in_days,
                CASE
                    WHEN grievance_data.age_in_days >= 0::numeric AND grievance_data.age_in_days <= 10::numeric THEN '0-10 days'::text
                    WHEN grievance_data.age_in_days >= 11::numeric AND grievance_data.age_in_days <= 20::numeric THEN '11-20 days'::text
                    WHEN grievance_data.age_in_days >= 21::numeric AND grievance_data.age_in_days <= 30::numeric THEN '21-30 days'::text
                    WHEN grievance_data.age_in_days >= 31::numeric AND grievance_data.age_in_days <= 40::numeric THEN '31-40 days'::text
                    ELSE '> 40 days'::text
                END AS age_category,
            grievance_data."CreatedOn",
            grievance_data."ModifiedOn",
            grievance_data."CreatedBy",
            grievance_data."CreatedUserName",
            grievance_data."ModifiedUserName",
            grievance_data."Note",
            grievance_data."IsLocationSpecific",
            grievance_data."LocationGranularity",
            grievance_data."LocationKey"::text AS "LocationKey",
            connecthub.array_distinct(string_to_array(string_agg(grievance_data."RequestorType", ','::text), ','::text)::integer[]) AS "RequestorType",
            array_textdistinct(string_to_array(string_agg(grievance_data."Gender", ','::text), ','::text)) AS "Gender",
            array_textdistinct(string_to_array(string_agg(grievance_data."Mobile", ','::text), ','::text)) AS "Mobile",
            array_textdistinct(string_to_array(string_agg(grievance_data."Email", ','::text), ','::text)) AS "Email",
            array_textdistinct(string_to_array(string_agg(btrim(grievance_data."RequestorTags", '{""}'::text), ','::text), ','::text)) AS "RequestorTags",
            array_textdistinct(string_to_array(string_agg(grievance_data."AgeGroup", ','::text), ','::text)) AS "AgeGroup",
            grievance_data."AssignedTo",
            grievance_data."AssignedName",
            grievance_data."Source",
            grievance_data."Department",
            grievance_data."DepartmentKey",
            grievance_data."HOD",
            grievance_data."HODKey",
            grievance_data."Subject",
            grievance_data."SubjectKey",
            grievance_data."SubSubject",
            grievance_data."SubSubjectCode",
            grievance_data."PartyCadreStatus",
            grievance_data.booth_incharge,
            grievance_data.booth_incharge_mobile,
            grievance_data.unit_incharge,
            grievance_data.unit_incharge_mobile,
            grievance_data.cluster_incharge,
            grievance_data.cluster_incharge_mobile
           FROM grievance_data
          GROUP BY grievance_data."Source", grievance_data."ClientKey", grievance_data.age_in_days, (
                CASE
                    WHEN grievance_data.age_in_days >= 0::numeric AND grievance_data.age_in_days <= 10::numeric THEN '0-10 days'::text
                    WHEN grievance_data.age_in_days >= 11::numeric AND grievance_data.age_in_days <= 20::numeric THEN '11-20 days'::text
                    WHEN grievance_data.age_in_days >= 21::numeric AND grievance_data.age_in_days <= 30::numeric THEN '21-30 days'::text
                    WHEN grievance_data.age_in_days >= 31::numeric AND grievance_data.age_in_days <= 40::numeric THEN '31-40 days'::text
                    ELSE '> 40 days'::text
                END), grievance_data."Lang", grievance_data."GrievanceKey", grievance_data."GrievanceType", grievance_data."GrievanceTypeName", grievance_data."GrievanceText", grievance_data."Attachments", grievance_data."ReferenceRelation", grievance_data."Tags", (grievance_data."AdditionalInfo"::text), grievance_data."Priority", grievance_data.sla, grievance_data."DueDate", grievance_data."GrievanceStatus", grievance_data."Status", grievance_data."IsActive", grievance_data."Remarks", grievance_data."CreatedOn", grievance_data."ModifiedOn", grievance_data."CreatedBy", grievance_data."CreatedUserName", grievance_data."ModifiedUserName", grievance_data."Note", grievance_data."IsLocationSpecific", grievance_data."LocationGranularity", (grievance_data."LocationKey"::text), grievance_data."AssignedTo", grievance_data."AssignedName", grievance_data."Department", grievance_data."DepartmentKey", grievance_data."HOD", grievance_data."HODKey", grievance_data."Subject", grievance_data."SubjectKey", grievance_data."SubSubject", grievance_data."SubSubjectCode", grievance_data."PartyCadreStatus", grievance_data.booth_incharge, grievance_data.booth_incharge_mobile, grievance_data.unit_incharge, grievance_data.unit_incharge_mobile, grievance_data.cluster_incharge, grievance_data.cluster_incharge_mobile
        ), grievance_final AS (
         SELECT grievance_aggregated."ClientKey",
            grievance_aggregated."Lang",
            grievance_aggregated."GrievanceKey",
            grievance_aggregated."GrievanceType",
            grievance_aggregated."GrievanceTypeName",
            grievance_aggregated."GrievanceText",
            array_length(string_to_array(grievance_aggregated."Attachments", ','::text), 1) AS "Attachments",
            COALESCE(connecthub.array_distinct(string_to_array(string_agg(btrim(grievance_aggregated."AssociationID"::text, '{}'::text), ','::text), ','::text)::integer[]), '{}'::integer[]) AS "AssociationID",
            json_build_object('Count', json_array_length(max(grievance_aggregated."RequestedFor")::json), 'List', max(grievance_aggregated."RequestedFor")::json) AS "RequestedFor",
            json_build_object('Count', json_array_length(max(grievance_aggregated."RequestedBy")::json), 'List', max(grievance_aggregated."RequestedBy")::json) AS "RequestedBy",
            json_build_object('Count', json_array_length(max(grievance_aggregated."ReferedBy")::json), 'List', max(grievance_aggregated."ReferedBy")::json) AS "ReferedBy",
            connecthub.array_distinct(string_to_array(string_agg(btrim(grievance_aggregated."RequestorACKey"::text, '{}'::text), ','::text), ','::text)::integer[]) AS "RequestorACKey",
            connecthub.array_distinct(string_to_array(string_agg(btrim(grievance_aggregated."RequestorStateKey"::text, '{}'::text), ','::text), ','::text)::integer[]) AS "RequestorStateKey",
            connecthub.array_distinct(string_to_array(string_agg(btrim(grievance_aggregated."RequestorDistrictKey"::text, '{}'::text), ','::text), ','::text)::integer[]) AS "RequestorDistrictKey",
            connecthub.array_distinct(string_to_array(string_agg(btrim(grievance_aggregated."RequestorMandalKey"::text, '{}'::text), ','::text), ','::text)::integer[]) AS "RequestorMandalKey",
            connecthub.array_distinct(string_to_array(string_agg(btrim(grievance_aggregated."RequestorVillageKey"::text, '{}'::text), ','::text), ','::text)::integer[]) AS "RequestorVillageKey",
            connecthub.array_distinct(string_to_array(string_agg(btrim(grievance_aggregated."RequestorType"::text, '{}'::text), ','::text), ','::text)::integer[]) AS "RequestorType",
            array_textdistinct(string_to_array(string_agg(btrim(grievance_aggregated."Gender"::text, '{}'::text), ','::text), ','::text)) AS "Gender",
            array_textdistinct(string_to_array(string_agg(btrim(grievance_aggregated."Mobile"::text, '{}'::text), ','::text), ','::text)) AS "Mobile",
            array_textdistinct(string_to_array(string_agg(btrim(grievance_aggregated."Email"::text, '{}'::text), ','::text), ','::text)) AS "Email",
            COALESCE(array_textdistinct(string_to_array(string_agg(replace(btrim(grievance_aggregated."RequestorTags"::text, '{"\"}'::text), '"'::text, ''::text), ','::text), ','::text)), '{}'::text[]) AS "RequestorTags",
            array_textdistinct(string_to_array(string_agg(btrim(grievance_aggregated."AgeGroup"::text, '{}'::text), ','::text), ','::text)) AS "AgeGroup",
            COALESCE(grievance_aggregated."Tags", '{}'::text[]) AS "Tags",
            grievance_aggregated."AdditionalInfo"::json AS "AdditionalInfo",
            COALESCE(grievance_aggregated."Priority", 'NA'::character varying) AS "Priority",
            grievance_aggregated.sla,
            COALESCE(grievance_aggregated."DueDate", '9999-12-31'::date) AS "DueDate",
            grievance_aggregated."GrievanceStatus",
            grievance_aggregated."Status",
            grievance_aggregated."IsActive",
            grievance_aggregated."Remarks",
            grievance_aggregated."CreatedOn",
            grievance_aggregated.age_in_days,
            grievance_aggregated.age_category,
            grievance_aggregated."ModifiedOn",
            grievance_aggregated."CreatedBy",
            grievance_aggregated."CreatedUserName",
            grievance_aggregated."ModifiedUserName",
            grievance_aggregated."Note",
                CASE
                    WHEN grievance_aggregated."IsLocationSpecific" = false OR grievance_aggregated."IsLocationSpecific" IS NULL THEN 'No'::text
                    ELSE 'Yes'::text
                END AS "IsLocationSpecific",
            COALESCE(grievance_aggregated."LocationGranularity", 'Not Specified'::character varying) AS "LocationGranularity",
            COALESCE(((grievance_aggregated."LocationKey"::json -> 0) -> 'StateKey'::text) ->> 'ID'::text, '0'::text)::integer AS "StateKey",
            COALESCE(((grievance_aggregated."LocationKey"::json -> 0) -> 'DistrictKey'::text) ->> 'ID'::text, '0'::text)::integer AS "DistrictKey",
            COALESCE(((grievance_aggregated."LocationKey"::json -> 0) -> 'PCKey'::text) ->> 'ID'::text, '0'::text)::integer AS "PCKey",
            COALESCE(((grievance_aggregated."LocationKey"::json -> 0) -> 'ACKey'::text) ->> 'ID'::text, '0'::text)::integer AS "ACKey",
            COALESCE(((grievance_aggregated."LocationKey"::json -> 0) -> 'MandalKey'::text) ->> 'ID'::text, '0'::text)::integer AS "MandalKey",
            COALESCE(((grievance_aggregated."LocationKey"::json -> 0) -> 'VillageKey'::text) ->> 'ID'::text, '0'::text)::integer AS "VillageKey",
            grievance_aggregated."AssignedTo",
            grievance_aggregated."AssignedName",
            grievance_aggregated."Source",
            grievance_aggregated."Department",
            grievance_aggregated."DepartmentKey",
            grievance_aggregated."HOD",
            grievance_aggregated."HODKey",
            grievance_aggregated."Subject",
            grievance_aggregated."SubjectKey",
            grievance_aggregated."SubSubject",
            grievance_aggregated."SubSubjectCode",
            grievance_aggregated."PartyCadreStatus",
            grievance_aggregated.booth_incharge,
            grievance_aggregated.booth_incharge_mobile,
            grievance_aggregated.unit_incharge,
            grievance_aggregated.unit_incharge_mobile,
            grievance_aggregated.cluster_incharge,
            grievance_aggregated.cluster_incharge_mobile
           FROM grievance_aggregated
          GROUP BY grievance_aggregated."Source", grievance_aggregated."ClientKey", grievance_aggregated.age_in_days, grievance_aggregated.age_category, grievance_aggregated."Lang", grievance_aggregated."GrievanceKey", grievance_aggregated."GrievanceType", grievance_aggregated."GrievanceTypeName", grievance_aggregated."GrievanceText", grievance_aggregated."Attachments", grievance_aggregated."Tags", grievance_aggregated."AdditionalInfo", grievance_aggregated."Priority", grievance_aggregated.sla, grievance_aggregated."DueDate", grievance_aggregated."GrievanceStatus", grievance_aggregated."Status", grievance_aggregated."IsActive", grievance_aggregated."Remarks", grievance_aggregated."CreatedOn", grievance_aggregated."ModifiedOn", grievance_aggregated."CreatedBy", grievance_aggregated."CreatedUserName", grievance_aggregated."ModifiedUserName", grievance_aggregated."Note", grievance_aggregated."IsLocationSpecific", grievance_aggregated."LocationGranularity", grievance_aggregated."LocationKey", grievance_aggregated."AssignedTo", grievance_aggregated."AssignedName", grievance_aggregated."Department", grievance_aggregated."DepartmentKey", grievance_aggregated."HOD", grievance_aggregated."HODKey", grievance_aggregated."Subject", grievance_aggregated."SubjectKey", grievance_aggregated."SubSubject", grievance_aggregated."SubSubjectCode", grievance_aggregated."PartyCadreStatus", grievance_aggregated.booth_incharge, grievance_aggregated.booth_incharge_mobile, grievance_aggregated.unit_incharge, grievance_aggregated.unit_incharge_mobile, grievance_aggregated.cluster_incharge, grievance_aggregated.cluster_incharge_mobile
        )
    SELECT 
        g."ClientKey",
        g."Lang",
        g."GrievanceKey" AS grievance_key,
        g."GrievanceType" AS grievance_type_key,
        g."GrievanceTypeName" AS grievance_type,
        g."GrievanceText" AS grievance_text,
        array_length(string_to_array(g."Attachments"::text, ','), 1) AS no_of_attachments_in_grievance,
        g."AssociationID" AS association_id,
        g."RequestedFor" AS requested_for,
        g."RequestedBy" AS requested_by,
        g."ReferedBy" AS refered_by,
        COALESCE(g."RequestorACKey", '{}'::integer[]) AS requestor_ac_key,
        COALESCE(g."RequestorStateKey", '{}'::integer[]) AS requestor_state_key,
        COALESCE(g."RequestorDistrictKey", '{}'::integer[]) AS requestor_district_key,
        COALESCE(g."RequestorMandalKey", '{}'::integer[]) AS requestor_mandal_key,
        COALESCE(g."RequestorVillageKey", '{}'::integer[]) AS requestor_village_key,
        g."RequestorType" AS requestor_type,
        COALESCE(g."Gender", '{None}'::text[]) AS gender,
        g."Mobile" AS mobile,
        COALESCE(g."Email", '{}'::text[]) AS email,
        g."RequestorTags" AS requestor_tags,
        COALESCE(g."AgeGroup", '{}'::text[]) AS age_group,
        g."Tags" AS tags,
        COALESCE(g."AdditionalInfo", '[{}]'::json) AS additional_info,
        g."Priority" AS priority,
        g.sla,
        g."DueDate" AS due_date,
        g."GrievanceStatus" AS grievance_status_key,
        g."Status" AS grievance_status,
        g."IsActive" AS is_active,
        g."Remarks" AS remarks,
        g.age_in_days,
        g.age_category,
        g."CreatedOn" AS created_on,
        g."ModifiedOn" AS modified_on,
        g."CreatedBy" AS created_by_key,
        g."CreatedUserName" AS created_by,
        g."ModifiedUserName" AS modified_by,
        g."Note" AS note,
        g."LocationGranularity" AS location_granularity,
        s."StateKey" AS request_state_key,
        s."State" AS request_state,
        dis."DistrictKey" AS request_district_key,
        dis."District" AS request_district,
        g."PCKey" AS request_pc_key,
        p."PCName" AS request_pc_name,
        g."ACKey" AS request_ac_key,
        a."ACName" AS request_ac_name,
        g."MandalKey" AS request_mandal_key,
        m."Mandal" AS request_mandal,
        g."VillageKey" AS request_village_key,
        COALESCE(v."Village", 'NA') AS request_village,
        g."AssignedTo" AS assignee_to_key,
        g."AssignedName" AS assigned_to,
        g."Source" AS source,
        g."Department" AS department,
        g."DepartmentKey" AS departmentkey,
        g."HOD" AS hod,
        g."HODKey" AS hodkey,
        g."Subject" AS subject,
        g."SubjectKey" AS subjectkey,
        g."SubSubject" AS subsubject,
        g."SubSubjectCode" AS subsubjectkey,
        g."PartyCadreStatus" AS party_cadre_status,
        g.booth_incharge,
        g.booth_incharge_mobile,
        g.unit_incharge,
        g.unit_incharge_mobile,
        g.cluster_incharge,
        g.cluster_incharge_mobile
    FROM grievance_final g
     LEFT JOIN ( SELECT DISTINCT "PCACVillages"."StateKey",
            "PCACVillages"."State",
            "PCACVillages"."ClientKey"
           FROM masters."PCACVillages"
          WHERE "PCACVillages"."StateKey" = 28) s ON s."StateKey" = 28 AND s."ClientKey" = g."ClientKey"
     LEFT JOIN ( SELECT DISTINCT "PCACVillages"."StateKey",
            "PCACVillages"."DistrictKey",
            "PCACVillages"."District",
            "PCACVillages"."ClientKey"
           FROM masters."PCACVillages") dis ON g."DistrictKey" = dis."DistrictKey" AND g."ClientKey" = dis."ClientKey"
     LEFT JOIN ( SELECT DISTINCT "PCACVillages"."PCKey",
            "PCACVillages"."PCName",
            "PCACVillages"."ClientKey"
           FROM masters."PCACVillages") p ON g."PCKey" = p."PCKey" AND g."ClientKey" = p."ClientKey"
     LEFT JOIN ( SELECT DISTINCT "PCACVillages"."ACKey",
            "PCACVillages"."ACName",
            "PCACVillages"."ClientKey"
           FROM masters."PCACVillages") a ON g."ACKey" = a."ACKey" AND g."ClientKey" = a."ClientKey"
     LEFT JOIN ( SELECT DISTINCT "PCACVillages"."ACKey",
            "PCACVillages"."MandalKey",
            "PCACVillages"."Mandal",
            "PCACVillages"."ClientKey"
           FROM masters."PCACVillages") m ON g."ACKey" = m."ACKey" AND g."MandalKey" = m."MandalKey" AND g."ClientKey" = m."ClientKey"
     LEFT JOIN ( SELECT "PCACVillages"."ACKey",
            "PCACVillages"."MandalKey",
            "PCACVillages"."VillageKey",
            "PCACVillages"."Village",
            "PCACVillages"."ClientKey"
           FROM masters."PCACVillages") v ON g."ACKey" = v."ACKey" AND g."MandalKey" = v."MandalKey" AND g."VillageKey" = v."VillageKey" AND g."ClientKey" = v."ClientKey";
END;
$function$
;



-- INSERT INTO connecthub."UserInfo"
-- ("ClientKey", "UserKey", "Email", "IsActive", "UserName", "Lang")
-- VALUES(1, '0 - 0 - 0 - 0 - 0', '', true, 'UnAssigned','en'),
-- (3, '0 - 0 - 0 - 0 - 0', '', true, 'UnAssigned','en'),
-- (4, '0 - 0 - 0 - 0 - 0', '', true, 'UnAssigned','en')

UPDATE connecthub."ClientInfo"
SET "Palette"='{
   "secondary":"#f9f2e8",
   "secondarylight":"#EFDC83",
   "secondarycontrasttext":"#202020",
   "warningmain":"#FFA227",
   "warningcontrasttext":"#FFFFFF",
   "errormain":"#EE3924"
}'::json
WHERE "Theme" = 'TDP';

DROP FUNCTION connecthub.fn_task_overview;

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
		    left join connecthub."UserInfo" ass on ass."UserKey" = t."AssignedTo"::text and ass."ClientKey" = t."ClientKey" 
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
		    left join connecthub."UserInfo" ass on ass."UserKey" = t."AssignedTo"::text and ass."ClientKey" = t."ClientKey" 
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

DROP FUNCTION connecthub.fn_homepage;

CREATE OR REPLACE FUNCTION connecthub.fn_homepage(_clientkey integer, _lang character varying, _userid character varying, _parameterizedjson json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE
    total_grievances json;
    total_draft_grievances json;
    grievances_for_correction json;
    grievances_created_by_me json;
    drafts json;
	grievances_for_inreview json;
	grievances_in_inreview json;
	_return json;
	grievance_templates json;
	grievances_for_correction_count int;
	grievances_created_by_me_count int;
	drafts_count int;
	grievances_in_inreview_count int;
	events_count_for_nextweek int;
	appointments_count_for_nextweek int;
	grievances_assigned_to_me json;
	grievances_assigned_to_me_count int;

    -- Parameters for filters
    sendforcorrections_limit integer;
    sendforcorrections_offset integer;
    drafts_limit integer;
    drafts_offset integer;
    inreview_limit int;
    inreview_offset int ;
    createdbyme_limit int;
    createdbyme_offset int;
	assignedtome_limit int;
	assignedtome_offset int;
  
BEGIN
    -- Extract parameters for filtering
    SELECT COALESCE((_parameterizedjson->'GrievancesForCorrection'->>'Limit')::integer, 10),
           COALESCE((_parameterizedjson->'GrievancesForCorrection'->>'Offset')::integer, 0)
    INTO sendforcorrections_limit, sendforcorrections_offset;

    SELECT COALESCE((_parameterizedjson->'Drafts'->>'Limit')::integer, 10),
           COALESCE((_parameterizedjson->'Drafts'->>'Offset')::integer, 0)
    INTO drafts_limit, drafts_offset;
   
    SELECT COALESCE((_parameterizedjson->'GrievancesInInReview'->>'Limit')::integer, 10),
           COALESCE((_parameterizedjson->'GrievancesInInReview'->>'Offset')::integer, 0)
    INTO inreview_limit, inreview_offset;
   
   SELECT COALESCE((_parameterizedjson->'GrievancesCreatedByMe'->>'Limit')::integer, 10),
           COALESCE((_parameterizedjson->'GrievancesCreatedByMe'->>'Offset')::integer, 0)
    INTO createdbyme_limit, createdbyme_offset;

	SELECT COALESCE((_parameterizedjson->'GrievancesAssignedToMe'->>'Limit')::integer, 10),
           COALESCE((_parameterizedjson->'GrievancesAssignedToMe'->>'Offset')::integer, 0)
    INTO assignedtome_limit, assignedtome_offset;
   
--   SELECT coalesce((_parameterizedjson->>'SearchTerm'),'')into _searchvalue;
   
--   raise notice '%,%',sendforcorrections_limit,sendforcorrections_offset;
   

    -- Query for Total Grievances
    SELECT json_build_object('TotalGrievances', count(*))
    INTO total_grievances
    FROM connecthub."GrievanceInfo" gi
    WHERE gi."CreatedBy" = _userid
    AND gi."ClientKey" = _clientkey
    AND gi."GrievanceStatus" <> 7
    AND gi."Source" = 'Meenestham';

    -- Query for Total Draft Grievances
    SELECT json_build_object('TotalDraftGrievances', count(*))
    INTO total_draft_grievances
    FROM connecthub."GrievanceInfo" gi
    WHERE gi."CreatedBy" = _userid
    AND gi."ClientKey" = _clientkey
    AND gi."GrievanceStatus" = 7
    AND gi."Source" = 'Meenestham';

    -- Query for Grievances Sent for Correction with Limit and Offset
	select 
		json_build_object('obj', json_agg(b)) 
	INTO grievances_for_correction
	from 
	(
    SELECT 
--	json_agg(
               json_build_object(
                   'GrievanceKey', "GrievanceKey",
				   'Department',"Department",
                   'Reason for Correction', "Note",
                   'Date: Received for Correction', TO_CHAR("ModifiedOn"::timestamp, 'YYYY-MM-DD HH:MI:SS AM')
               ) b
--           )
--    INTO grievances_for_correction
    FROM connecthub."GrievanceInfo" gi
    WHERE gi."CreatedBy" = _userid
    AND gi."ClientKey" = _clientkey
    AND gi."GrievanceStatus" = 0
    and ("GrievanceKey" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or "Note" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or "Department" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%')
    ORDER BY 
	  CASE 
	    WHEN (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Field') = 'Date: Received for Correction' and (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Order') = 'desc' THEN "ModifiedOn"::text
	    WHEN (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Field') = 'GrievanceKey' and (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Order') = 'desc' THEN "GrievanceKey" 
	    WHEN (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Field') = 'Reason for Correction' and (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Order') = 'desc' THEN "Note" 
		WHEN (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Field') = 'Department' and (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Order') = 'desc' THEN "Department"
	  end desc,
	   case
		WHEN (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Field') = 'Date: Received for Correction' and (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Order') = 'asc' THEN "ModifiedOn"::text
	    WHEN (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Field') = 'GrievanceKey' and (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Order') = 'asc' THEN "GrievanceKey" 
	    WHEN (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Field') = 'Reason for Correction' and (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Order') = 'asc' THEN "Note" 
		WHEN (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Field') = 'Department' and (_parameterizedjson->'GrievancesForCorrection'->'Sort'->>'Order') = 'asc' THEN "Department"
	  	end asc, "ModifiedOn"::timestamp desc
    limit sendforcorrections_limit offset sendforcorrections_offset
	) a;

	select count(*) from (
    select * into grievances_for_correction_count
    FROM connecthub."GrievanceInfo" gi
    WHERE gi."CreatedBy" = _userid
    AND gi."ClientKey" = _clientkey
    AND gi."GrievanceStatus" = 0
    and ("GrievanceKey" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or "Note" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or "Department" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%')) a;
--    ORDER BY "ModifiedOn" desc
--    limit sendforcorrections_limit offset sendforcorrections_offset

-- grievances created by me

WITH requestor_data AS (
        (SELECT "RequestorID",
            CONCAT("RequestorFirstName", ' ', "RequestorLastName") AS "RequestorName",
            "Mobile" AS "RequestorMobileNumber", 
            'Individual' AS "RequestorType"
        FROM connecthub."RequestorInformation" ri)
        UNION
        (SELECT "ID" AS "RequestorID",
            "Name" AS "RequestorName",
            "Mobile" AS "RequestorMobileNumber", 
            'Association' AS "RequestorType"
        FROM connecthub."AssociationInfo" ai)
    ), grivenace_data AS (
        SELECT "GrievanceKey", "Status","Department",
            TO_CHAR("CreatedOn"::timestamp, 'YYYY-MM-DD') AS "Created On",
            trim(replace((json_array_elements("RequestedBy"::json)->'ID')::text,'"',''))::integer AS "RequestorID", 
            trim(replace((json_array_elements("RequestedBy"::json)->'Type')::text,'"','')) AS "RequestorType"
        FROM connecthub."GrievanceInfo" gi
        WHERE gi."CreatedBy" = _userid
        AND gi."ClientKey" = _clientkey
        AND gi."GrievanceStatus" <> 7
        AND gi."Source" = 'Meenestham'
    ), formulate_griveance_with_requestor_information AS (
        SELECT t1.*, 
            CONCAT(t2."RequestorName", ' ( ', 
                CASE 
                    WHEN t2."RequestorMobileNumber" IS NULL THEN 'No Mobile Number' 
                    ELSE t2."RequestorMobileNumber" 
                END, ' )') AS "RequestorDeatails" 
        FROM grivenace_data t1
        LEFT OUTER JOIN requestor_data t2
            ON t1."RequestorID" = t2."RequestorID"
            AND t1."RequestorType" = t2."RequestorType"
    )
    SELECT 
	json_build_object('obj', json_agg(b)) 
	INTO grievances_created_by_me
	from 
	(
	select 
                json_build_object(
                    'GrievanceKey', "GrievanceKey",
					'Department',"Department",
                    'Created On', "Created On",
                    'Status', "Status",
--                    'RequstorDetails', details
					'RequestorDetails', details
                ) b
    FROM (

	select * from (
        SELECT "GrievanceKey","Department", "Created On", "Status", string_agg("RequestorDeatails", '<br/>') AS details
        FROM formulate_griveance_with_requestor_information
        GROUP BY "GrievanceKey","Department", "Created On", "Status"
	) a
	where ("GrievanceKey" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or details ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or "Department" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%')
--	ORDER BY 
--	  CASE 
--	    WHEN (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Field') IS NOT NULL THEN (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Field')||' '||(_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Order')
--	    ELSE '"Created On"::text' ||' desc'
--	  END
--        LIMIT createdbyme_limit OFFSET createdbyme_offset
	ORDER BY 
	  CASE 
	    WHEN (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Field') = 'Created On' and (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Order') = 'desc' THEN "Created On"::text
	    WHEN (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Field') = 'GrievanceKey' and (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Order') = 'desc' THEN "GrievanceKey" 
	    WHEN (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Field') = 'RequstorDetails' and (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Order') = 'desc' THEN "details" 
		WHEN (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Field') = 'Department' and (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Order') = 'desc' THEN "Department" 
	  end desc,
	   case
		WHEN (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Field') = 'Created On' and (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Order') = 'asc' THEN "Created On"::text
	    WHEN (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Field') = 'GrievanceKey' and (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Order') = 'asc' THEN "GrievanceKey" 
	    WHEN (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Field') = 'RequstorDetails' and (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Order') = 'asc' THEN "details" 
		WHEN (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Field') = 'Department' and (_parameterizedjson->'GrievancesCreatedByMe'->'Sort'->>'Order') = 'asc' THEN "Department"
	  	end asc, "Created On"::timestamp desc
        LIMIT createdbyme_limit OFFSET createdbyme_offset
) aa
    ) b;

   
   WITH requestor_data AS (
        (SELECT "RequestorID",
            CONCAT("RequestorFirstName", ' ', "RequestorLastName") AS "RequestorName",
            "Mobile" AS "RequestorMobileNumber", 
            'Individual' AS "RequestorType"
        FROM connecthub."RequestorInformation" ri)
        UNION
        (SELECT "ID" AS "RequestorID",
            "Name" AS "RequestorName",
            "Mobile" AS "RequestorMobileNumber", 
            'Association' AS "RequestorType"
        FROM connecthub."AssociationInfo" ai)
    ), grivenace_data AS (
        SELECT "GrievanceKey", "Status","Department",
            TO_CHAR("CreatedOn"::timestamp, 'YYYY-MM-DD') AS "Created On",
            trim(replace((json_array_elements("RequestedBy"::json)->'ID')::text,'"',''))::integer AS "RequestorID", 
            trim(replace((json_array_elements("RequestedBy"::json)->'Type')::text,'"','')) AS "RequestorType"
        FROM connecthub."GrievanceInfo" gi
        WHERE gi."CreatedBy" = _userid
        AND gi."ClientKey" = _clientkey
        AND gi."GrievanceStatus" <> 7
        AND gi."Source" = 'Meenestham'
    ), formulate_griveance_with_requestor_information AS (
        SELECT t1.*, 
            CONCAT(t2."RequestorName", ' ( ', 
                CASE 
                    WHEN t2."RequestorMobileNumber" IS NULL THEN 'No Mobile Number' 
                    ELSE t2."RequestorMobileNumber" 
                END, ' )') AS "RequestorDeatails" 
        FROM grivenace_data t1
        LEFT OUTER JOIN requestor_data t2
            ON t1."RequestorID" = t2."RequestorID"
            AND t1."RequestorType" = t2."RequestorType"
    )
    SELECT 
	count(*)
	INTO grievances_created_by_me_count
	from 
	(
	select * from (
	        SELECT "GrievanceKey","Department", "Created On", "Status", string_agg("RequestorDeatails", ',') AS details
        FROM formulate_griveance_with_requestor_information
        GROUP BY "GrievanceKey","Department", "Created On", "Status"
        ) aa
        where ("GrievanceKey" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or details ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or "Department" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%')
--        order by "Created On" desc
--        LIMIT createdbyme_limit OFFSET createdbyme_offset
	) a;
	



--    LIMIT sendforcorrections_limit OFFSET sendforcorrections_offset;

    -- Query for Drafts with Limit and Offset
    WITH requestor_data AS (
        (SELECT "RequestorID",
            CONCAT("RequestorFirstName", ' ', "RequestorLastName") AS "RequestorName",
            "Mobile" AS "RequestorMobileNumber", 
            'Individual' AS "RequestorType"
        FROM connecthub."RequestorInformation" ri)
        UNION
        (SELECT "ID" AS "RequestorID",
            "Name" AS "RequestorName",
            "Mobile" AS "RequestorMobileNumber", 
            'Association' AS "RequestorType"
        FROM connecthub."AssociationInfo" ai)
    ), grivenace_data AS (
        SELECT "GrievanceKey", "GrievanceText",
            TO_CHAR("CreatedOn"::timestamp, 'YYYY-MM-DD') AS "Created On",
            trim(replace((json_array_elements("RequestedBy"::json)->'ID')::text,'"',''))::integer AS "RequestorID", 
            trim(replace((json_array_elements("RequestedBy"::json)->'Type')::text,'"','')) AS "RequestorType"
        FROM connecthub."GrievanceInfo" gi
        WHERE gi."CreatedBy" = _userid
        AND gi."ClientKey" = _clientkey
        AND gi."GrievanceStatus" = 7
        AND gi."Source" = 'Meenestham'
    ), formulate_griveance_with_requestor_information AS (
        SELECT t1.*, 
            CONCAT(t2."RequestorName", ' ( ', 
                CASE 
                    WHEN t2."RequestorMobileNumber" IS NULL THEN 'No Mobile Number' 
                    ELSE t2."RequestorMobileNumber" 
                END, ' )') AS "RequestorDeatails" 
        FROM grivenace_data t1
        LEFT OUTER JOIN requestor_data t2
            ON t1."RequestorID" = t2."RequestorID"
            AND t1."RequestorType" = t2."RequestorType"
    )
    SELECT 
	json_build_object('obj', json_agg(b)) 
	INTO drafts
	from 
	(
	select 
--	json_agg(   
                json_build_object(
                    'GrievanceKey', "GrievanceKey",
                    'Created On', "Created On",
                    'GrievanceText', "GrievanceText",
--                    'RequstorDetails', details
					'RequestorDetails', details
                ) b
--           ) 
--    INTO drafts
    FROM (
        SELECT "GrievanceKey", "Created On", "GrievanceText", string_agg("RequestorDeatails", ',') AS details
        FROM formulate_griveance_with_requestor_information
        GROUP BY "GrievanceKey", "Created On", "GrievanceText"
	) a
	where ("GrievanceKey" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or "GrievanceText" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or details ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' )
	ORDER BY 
	  CASE 
	    WHEN (_parameterizedjson->'Drafts'->'Sort'->>'Field') = 'Created On' and (_parameterizedjson->'Drafts'->'Sort'->>'Order') = 'desc' THEN "Created On"::text
	    WHEN (_parameterizedjson->'Drafts'->'Sort'->>'Field') = 'GrievanceKey' and (_parameterizedjson->'Drafts'->'Sort'->>'Order') = 'desc' THEN "GrievanceKey" 
	    WHEN (_parameterizedjson->'Drafts'->'Sort'->>'Field') = 'RequstorDetails' and (_parameterizedjson->'Drafts'->'Sort'->>'Order') = 'desc' THEN "details" 
		WHEN (_parameterizedjson->'Drafts'->'Sort'->>'Field') = 'GrievanceText' and (_parameterizedjson->'Drafts'->'Sort'->>'Order') = 'desc' THEN "GrievanceText"
	  end desc,
	   case
		WHEN (_parameterizedjson->'Drafts'->'Sort'->>'Field') = 'Created On' and (_parameterizedjson->'Drafts'->'Sort'->>'Order') = 'asc' THEN "Created On"::text
	    WHEN (_parameterizedjson->'Drafts'->'Sort'->>'Field') = 'GrievanceKey' and (_parameterizedjson->'Drafts'->'Sort'->>'Order') = 'asc' THEN "GrievanceKey" 
	    WHEN (_parameterizedjson->'Drafts'->'Sort'->>'Field') = 'RequstorDetails' and (_parameterizedjson->'Drafts'->'Sort'->>'Order') = 'asc' THEN "details" 
		WHEN (_parameterizedjson->'Drafts'->'Sort'->>'Field') = 'GrievanceText' and (_parameterizedjson->'Drafts'->'Sort'->>'Order') = 'asc' THEN "GrievanceText"
	  	end asc, "Created On"::timestamp desc
        LIMIT drafts_limit OFFSET drafts_offset
    ) b;
   
    WITH requestor_data AS (
        (SELECT "RequestorID",
            CONCAT("RequestorFirstName", ' ', "RequestorLastName") AS "RequestorName",
            "Mobile" AS "RequestorMobileNumber", 
            'Individual' AS "RequestorType"
        FROM connecthub."RequestorInformation" ri)
        UNION
        (SELECT "ID" AS "RequestorID",
            "Name" AS "RequestorName",
            "Mobile" AS "RequestorMobileNumber", 
            'Association' AS "RequestorType"
        FROM connecthub."AssociationInfo" ai)
    ), grivenace_data AS (
        SELECT "GrievanceKey", "GrievanceText", 
            TO_CHAR("CreatedOn"::timestamp, 'YYYY-MM-DD') AS "Created On",
            trim(replace((json_array_elements("RequestedBy"::json)->'ID')::text,'"',''))::integer AS "RequestorID", 
            trim(replace((json_array_elements("RequestedBy"::json)->'Type')::text,'"','')) AS "RequestorType"
        FROM connecthub."GrievanceInfo" gi
        WHERE gi."CreatedBy" = _userid
        AND gi."ClientKey" = _clientkey
        AND gi."GrievanceStatus" = 7
        AND gi."Source" = 'Meenestham'
    ), formulate_griveance_with_requestor_information AS (
        SELECT t1.*, 
            CONCAT(t2."RequestorName", ' ( ', 
                CASE 
                    WHEN t2."RequestorMobileNumber" IS NULL THEN 'No Mobile Number' 
                    ELSE t2."RequestorMobileNumber" 
                END, ' )') AS "RequestorDeatails" 
        FROM grivenace_data t1
        LEFT OUTER JOIN requestor_data t2
            ON t1."RequestorID" = t2."RequestorID"
            AND t1."RequestorType" = t2."RequestorType"
    )
    SELECT 
    count(*)
	INTO drafts_count
	from 
	(
	select * from (
        SELECT "GrievanceKey", "Created On", "GrievanceText", string_agg("RequestorDeatails", ',') AS details
        FROM formulate_griveance_with_requestor_information
        GROUP BY "GrievanceKey", "Created On", "GrievanceText"
       ) aa
       where ("GrievanceKey" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or "GrievanceText" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or details ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' )
--	order by "Created On" desc
--        LIMIT drafts_limit OFFSET drafts_offset
	) a;



if (select distinct "ProfileKey" from connecthub."UserProfiles" where "UserKey" = _userid) <> 5 then

-- Query for InReview for reviewer with Limit and Offset

	WITH requestor_data AS (
	        (SELECT "RequestorID",
	            CONCAT("RequestorFirstName", ' ', "RequestorLastName") AS "RequestorName",
	            "Mobile" AS "RequestorMobileNumber", 
	            'Individual' AS "RequestorType"
	        FROM connecthub."RequestorInformation" ri)
	        UNION
	        (SELECT "ID" AS "RequestorID",
	            "Name" AS "RequestorName",
	            "Mobile" AS "RequestorMobileNumber", 
	            'Association' AS "RequestorType"
	        FROM connecthub."AssociationInfo" ai)
	    ), grivenace_data AS (
	        SELECT "GrievanceKey", "Status", "Department",
	            TO_CHAR("CreatedOn"::timestamp, 'YYYY-MM-DD') AS "Created On",
	            trim(replace((json_array_elements("RequestedBy"::json)->'ID')::text,'"',''))::integer AS "RequestorID", 
	            trim(replace((json_array_elements("RequestedBy"::json)->'Type')::text,'"','')) AS "RequestorType"
	        FROM connecthub."GrievanceInfo" gi
	        WHERE 
	--		gi."CreatedBy" = _userid and 
	        gi."ClientKey" = _clientkey
	        AND gi."GrievanceStatus" = 1
	        AND gi."Source" = 'Meenestham'
	    ), formulate_griveance_with_requestor_information AS (
	        SELECT t1.*, 
	            CONCAT(t2."RequestorName", ' ( ', 
	                CASE 
	                    WHEN t2."RequestorMobileNumber" IS NULL THEN 'No Mobile Number' 
	                    ELSE t2."RequestorMobileNumber" 
	                END, ' )') AS "RequestorDeatails" 
	        FROM grivenace_data t1
	        LEFT OUTER JOIN requestor_data t2
	            ON t1."RequestorID" = t2."RequestorID"
	            AND t1."RequestorType" = t2."RequestorType"
	    )
	    SELECT 
		json_build_object('obj', json_agg(b)) 
		INTO grievances_in_inreview
		from 
		(
		select 
	--	json_agg(   
	                json_build_object(
	                    'GrievanceKey', "GrievanceKey",
						'Department',"Department",
	                    'Created On', "Created On",
--	                    'Status', "Status",
--	                    'RequstorDetails', details
						'RequestorDetails', details
	                ) b
	--           ) 
	--    INTO drafts
	    FROM (
	        SELECT "GrievanceKey", "Department", "Created On", "Status", string_agg("RequestorDeatails", ',') AS details
	        FROM formulate_griveance_with_requestor_information
	        GROUP BY "GrievanceKey", "Department", "Created On", "Status"
		) a
		where ("GrievanceKey"  ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or details  ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or "Department" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%')
		ORDER BY 
	  CASE 
	    WHEN (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Field') = 'Created On' and (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Order') = 'desc' THEN "Created On"::text
	    WHEN (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Field') = 'GrievanceKey' and (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Order') = 'desc' THEN "GrievanceKey" 
	    WHEN (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Field') = 'RequstorDetails' and (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Order') = 'desc' THEN "details" 
		WHEN (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Field') = 'Department' and (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Order') = 'desc' THEN "Department"
	  end desc,
	   case
		WHEN (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Field') = 'Created On' and (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Order') = 'asc' THEN "Created On"::text
	    WHEN (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Field') = 'GrievanceKey' and (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Order') = 'asc' THEN "GrievanceKey" 
	    WHEN (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Field') = 'RequstorDetails' and (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Order') = 'asc' THEN "details" 
		WHEN (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Field') = 'Department' and (_parameterizedjson->'GrievancesInInReview'->'Sort'->>'Order') = 'asc' THEN "Department"
	  	end asc, "Created On"::timestamp desc
	      LIMIT inreview_limit OFFSET inreview_offset
	    ) b;
	   
	WITH requestor_data AS (
	        (SELECT "RequestorID",
	            CONCAT("RequestorFirstName", ' ', "RequestorLastName") AS "RequestorName",
	            "Mobile" AS "RequestorMobileNumber", 
	            'Individual' AS "RequestorType"
	        FROM connecthub."RequestorInformation" ri)
	        UNION
	        (SELECT "ID" AS "RequestorID",
	            "Name" AS "RequestorName",
	            "Mobile" AS "RequestorMobileNumber", 
	            'Association' AS "RequestorType"
	        FROM connecthub."AssociationInfo" ai)
	    ), grivenace_data AS (
	        SELECT "GrievanceKey", "Status", "Department",
	            TO_CHAR("CreatedOn"::timestamp, 'YYYY-MM-DD') AS "Created On",
	            trim(replace((json_array_elements("RequestedBy"::json)->'ID')::text,'"',''))::integer AS "RequestorID", 
	            trim(replace((json_array_elements("RequestedBy"::json)->'Type')::text,'"','')) AS "RequestorType"
	        FROM connecthub."GrievanceInfo" gi
	        WHERE 
	--		gi."CreatedBy" = _userid and 
	        gi."ClientKey" = _clientkey
	        AND gi."GrievanceStatus" = 1
	        AND gi."Source" = 'Meenestham'
	    ), formulate_griveance_with_requestor_information AS (
	        SELECT t1.*, 
	            CONCAT(t2."RequestorName", ' ( ', 
	                CASE 
	                    WHEN t2."RequestorMobileNumber" IS NULL THEN 'No Mobile Number' 
	                    ELSE t2."RequestorMobileNumber" 
	                END, ' )') AS "RequestorDeatails" 
	        FROM grivenace_data t1
	        LEFT OUTER JOIN requestor_data t2
	            ON t1."RequestorID" = t2."RequestorID"
	            AND t1."RequestorType" = t2."RequestorType"
	    )
	    SELECT 
	    count(*)
		INTO grievances_in_inreview_count
		from (
			select * from (
	        SELECT "GrievanceKey","Department", "Created On", "Status", string_agg("RequestorDeatails", ',') AS details
	        FROM formulate_griveance_with_requestor_information
			GROUP BY "GrievanceKey","Department", "Created On", "Status"
			) aa
	        where ("GrievanceKey"  ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or details  ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or "Department" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%')
--		order by "Created On" desc
--	      LIMIT inreview_limit OFFSET inreview_offset
		) a;

end if;

select 
json_agg(json_build_object('ID',"ID",'Name',"Name"))
into grievance_templates
from connecthub."GrievanceTemplates" gt where "DepartmentID" is not null;


    -- Combine all JSON results into one JSON object
--	SELECT jsonb_strip_nulls(
--    jsonb_build_object(
--        'TotalGrievances', jsonb_pretty(total_grievances::jsonb),
--        'TotalDraftGrievances', total_draft_grievances::jsonb,
--        'GrievancesForCorrection', grievances_for_correction::jsonb,
--        'GrievancesCreatedByMe', grievances_created_by_me::jsonb,
--        'Drafts', drafts::jsonb,
--		'GrievanceTemplates', grievance_templates::jsonb
--    ) || 
--    CASE 
--        WHEN grievances_in_inreview IS NOT NULL THEN jsonb_build_object('GrievancesInInReview', grievances_in_inreview::jsonb)
--        ELSE '{}'::jsonb 
--    END
--)::json into _return;

--SELECT jsonb_strip_nulls(
--    json_build_object(
--        'TotalGrievances', total_grievances,
--        'TotalDraftGrievances', total_draft_grievances,
--        'GrievancesForCorrection', grievances_for_correction,
--        'GrievancesCreatedByMe', grievances_created_by_me,
--        'Drafts', drafts,
--		'GrievanceTemplates', grievance_templates
--    )::jsonb || 
--    CASE 
--        WHEN grievances_in_inreview IS NOT NULL THEN jsonb_build_object('GrievancesInInReview', grievances_in_inreview::jsonb)
--        ELSE '{}'::jsonb 
--    END
--)::json into _return;

	
--select json_build_object('Status','Success','Details',_return) into _return;

SELECT count(*) into events_count_for_nextweek
FROM connecthub."Appointments" a
WHERE "StartTime" >= current_timestamp 
  AND "StartTime" < current_timestamp + INTERVAL '7 days' and "EntryType" = 'Event';
 
SELECT count(*) into appointments_count_for_nextweek
FROM connecthub."Appointments" a
WHERE "StartTime" >= current_timestamp
  AND "StartTime" < current_timestamp + INTERVAL '7 days' and "EntryType" = 'Appointment';


WITH requestor_data AS (
        (SELECT "RequestorID",
            CONCAT("RequestorFirstName", ' ', "RequestorLastName") AS "RequestorName",
            "Mobile" AS "RequestorMobileNumber", 
            'Individual' AS "RequestorType"
        FROM connecthub."RequestorInformation" ri)
        UNION
        (SELECT "ID" AS "RequestorID",
            "Name" AS "RequestorName",
            "Mobile" AS "RequestorMobileNumber", 
            'Association' AS "RequestorType"
        FROM connecthub."AssociationInfo" ai)
    ), grivenace_data AS (
        SELECT "GrievanceKey", "Status", "Department",
            TO_CHAR("CreatedOn"::timestamp, 'YYYY-MM-DD') AS "Created On",
            trim(replace((json_array_elements("RequestedBy"::json)->'ID')::text,'"',''))::integer AS "RequestorID", 
            trim(replace((json_array_elements("RequestedBy"::json)->'Type')::text,'"','')) AS "RequestorType"
        FROM connecthub."GrievanceInfo" gi
        WHERE gi."AssignedTo" = _userid
        AND gi."ClientKey" = _clientkey
        AND gi."Source" = 'Meenestham'
    ), formulate_griveance_with_requestor_information AS (
        SELECT t1.*, 
            CONCAT(t2."RequestorName", ' ( ', 
                CASE 
                    WHEN t2."RequestorMobileNumber" IS NULL THEN 'No Mobile Number' 
                    ELSE t2."RequestorMobileNumber" 
                END, ' )') AS "RequestorDeatails" 
        FROM grivenace_data t1
        LEFT OUTER JOIN requestor_data t2
            ON t1."RequestorID" = t2."RequestorID"
            AND t1."RequestorType" = t2."RequestorType"
    )
    SELECT 
	json_build_object('obj', json_agg(b)) 
	INTO grievances_assigned_to_me
	from 
	(
	select 
                json_build_object(
                    'GrievanceKey', "GrievanceKey",
					'Department',"Department",
                    'Created On', "Created On",
                    'Status', "Status",
--                    'RequstorDetails', details
					'RequestorDetails', details
                ) b
    FROM (

	select * from (
        SELECT "GrievanceKey","Department", "Created On", "Status", string_agg("RequestorDeatails", ',') AS details
        FROM formulate_griveance_with_requestor_information
        GROUP BY "GrievanceKey","Department", "Created On", "Status"
	) a
	where ("GrievanceKey" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or details ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or "Department" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%')
	ORDER BY 
	  CASE 
	    WHEN (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Field') = 'Created On' and (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Order') = 'desc' THEN "Created On"::text
	    WHEN (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Field') = 'GrievanceKey' and (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Order') = 'desc' THEN "GrievanceKey" 
	    WHEN (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Field') = 'RequstorDetails' and (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Order') = 'desc' THEN "details" 
		WHEN (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Field') = 'Department' and (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Order') = 'desc' THEN "Department"
	  end desc,
	   case
		WHEN (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Field') = 'Created On' and (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Order') = 'asc' THEN "Created On"::text
	    WHEN (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Field') = 'GrievanceKey' and (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Order') = 'asc' THEN "GrievanceKey" 
	    WHEN (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Field') = 'RequstorDetails' and (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Order') = 'asc' THEN "details" 
		WHEN (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Field') = 'Department' and (_parameterizedjson->'GrievancesAssignedToMe'->'Sort'->>'Order') = 'asc' THEN "Department"
	  	end asc, "Created On"::timestamp desc
        LIMIT assignedtome_limit OFFSET assignedtome_offset
) aa
    ) b;



WITH requestor_data AS (
        (SELECT "RequestorID",
            CONCAT("RequestorFirstName", ' ', "RequestorLastName") AS "RequestorName",
            "Mobile" AS "RequestorMobileNumber", 
            'Individual' AS "RequestorType"
        FROM connecthub."RequestorInformation" ri)
        UNION
        (SELECT "ID" AS "RequestorID",
            "Name" AS "RequestorName",
            "Mobile" AS "RequestorMobileNumber", 
            'Association' AS "RequestorType"
        FROM connecthub."AssociationInfo" ai)
    ), grivenace_data AS (
        SELECT "GrievanceKey", "Status", "Department",
            TO_CHAR("CreatedOn"::timestamp, 'YYYY-MM-DD') AS "Created On",
            trim(replace((json_array_elements("RequestedBy"::json)->'ID')::text,'"',''))::integer AS "RequestorID", 
            trim(replace((json_array_elements("RequestedBy"::json)->'Type')::text,'"','')) AS "RequestorType"
        FROM connecthub."GrievanceInfo" gi
        WHERE gi."AssignedTo" = _userid
        AND gi."ClientKey" = _clientkey
        AND gi."Source" = 'Meenestham'
    ), formulate_griveance_with_requestor_information AS (
        SELECT t1.*, 
            CONCAT(t2."RequestorName", ' ( ', 
                CASE 
                    WHEN t2."RequestorMobileNumber" IS NULL THEN 'No Mobile Number' 
                    ELSE t2."RequestorMobileNumber" 
                END, ' )') AS "RequestorDeatails" 
        FROM grivenace_data t1
        LEFT OUTER JOIN requestor_data t2
            ON t1."RequestorID" = t2."RequestorID"
            AND t1."RequestorType" = t2."RequestorType"
    )
    SELECT 
	count(*)
	INTO grievances_assigned_to_me_count
	from 
	(
        SELECT "GrievanceKey","Department", "Created On", "Status", string_agg("RequestorDeatails", ',') AS details
        FROM formulate_griveance_with_requestor_information
        GROUP BY "GrievanceKey","Department", "Created On", "Status"
	) a
	where ("GrievanceKey" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or details ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%' or "Department" ilike '%'||coalesce((_parameterizedjson->>'SearchTerm'),'')||'%');


    RETURN json_build_object('Status','Success','Details',json_build_object(
        'TotalGrievances', total_grievances,
        'TotalDraftGrievances', total_draft_grievances,
        'AppointmentsCount',appointments_count_for_nextweek,
        'EventsCount',events_count_for_nextweek,
        'GrievancesForCorrection', grievances_for_correction, 'GrievancesForCorrectionCount', grievances_for_correction_count,
	    'GrievancesCreatedByMe', grievances_created_by_me, 'GrievancesCreatedByMeCount', grievances_created_by_me_count,
        'Drafts', drafts, 'DraftsCount',drafts_count,
	    'GrievanceTemplates', grievance_templates,
	    'GrievancesInInReview', grievances_in_inreview, 'GrievancesInInReviewCount',grievances_in_inreview_count,
		'GrievancesAssignedToMe',grievances_assigned_to_me,'GrievancesAssignedToMeCount',grievances_assigned_to_me_count,
	    'SearchColumns','Search by Grievance Key, Grievance Note, Requestor Details, Department.'
    ));

return _return;

END;
$function$
;



update connecthub."PageRoleAccess" pra 
set "PageGroupKey" = 4
where "PageKey" = 17;

update connecthub."PageList" pl 
set "PageGroupKey" = 4
where "PageKey" = 17;

update connecthub."PageList_Lang" pl 
set "PageGroupKey" = 4
where "PageKey" = 17;

update connecthub."PageRoleAccess" pra 
set "IsActive" = False
where "PageKey" = 7 and "RoleKey" = 1;

drop view hubviews.vw_voterdataset;
drop view connecthub."VotersInformation";
drop view reporting.vw_party_cadre;
drop view hubviews."VotersInfo" ;
drop MATERIALIZED VIEW hubviews.mv_pc_incharges_dataset;


create MATERIALIZED VIEW hubviews.mv_pc_incharges_dataset
TABLESPACE pg_default
AS 
SELECT a."Ec_ClientKey" AS client_key,
    split_part(b.cluster, '_'::text, 2)::integer AS cluster_key,
    b.cluster,
    split_part(b.unit, '_UNIT_'::text, 2)::integer AS unit_key,
    b.unit,
    max(a."Ec_VillageKey") AS village_key,
    max(a."Ec_Village") AS village,
    a.booth::integer AS booth_key,
    a."Ec_PollingStation" AS booth,
    b.name AS b_name,
    b.gender AS b_gender,
    b.age AS b_age,
    b.contact AS b_contact,
    b.voter_id AS b_voterid,
    b.u_name,
    b.u_gender,
    b.u_age,
    b.u_contact,
    b.u_voterid,
    b.c_name,
    b.c_gender,
    b.c_age,
    b.c_contact,
    b.c_voterid
   FROM ( SELECT DISTINCT vie."Ec_ClientKey",
            vie."Ec_Village",
            vie."Ec_VillageKey",
            split_part(vie."Ec_PollingStation", '-'::text, 1) AS booth,
            vie."Ec_PollingStation"
           FROM connecthub."VotersInfoEC" vie
          WHERE vie."Ec_ClientKey" = 1) a
     FULL JOIN ( SELECT b_1.ac_name,
            b_1.cluster,
            b_1.unit,
            b_1.booth,
            b_1.name,
            b_1.gender,
            b_1.age,
            b_1.contact,
            b_1.voter_id,
            b_1.ps,
            b_1.client_key,
            c.name AS u_name,
            c.gender AS u_gender,
            c.age AS u_age,
            c.contact AS u_contact,
            c.voter_id AS u_voterid,
            d.name AS c_name,
            d.gender AS c_gender,
            d.age AS c_age,
            d.contact AS c_contact,
            d.voter_id AS c_voterid
           FROM ( SELECT ubmp.ac_name,
                    ubmp.cluster,
                    ubmp.unit,
                    ubmp.booth,
                    ubmp.name,
                    ubmp.gender,
                    ubmp.age,
                    ubmp.contact,
                    ubmp.voter_id,
                    ubmp.ps,
                    ubmp.client_key
                   FROM ukd_booth_mapping_pc ubmp
                  WHERE ubmp.ps = ''::text) b_1
             LEFT JOIN ( SELECT ubmp.ac_name,
                    ubmp.unit,
                    ubmp.name,
                    ubmp.gender,
                    ubmp.age,
                    ubmp.contact,
                    ubmp.voter_id,
                    ubmp.ps,
                    ubmp.client_key
                   FROM ukd_unit_mapping_pc ubmp
                  WHERE ubmp.ps = ''::text) c ON c.client_key = b_1.client_key AND c.unit = b_1.unit
             LEFT JOIN ( SELECT ubmp.ac_name,
                    ubmp.cluster,
                    ubmp.name,
                    ubmp.gender,
                    ubmp.age,
                    ubmp.contact,
                    ubmp.voter_id,
                    ubmp.ps,
                    ubmp.client_key
                   FROM ukd_cluster_mapping_pc ubmp
                  WHERE ubmp.ps = ''::text) d ON d.client_key = b_1.client_key AND d.cluster = b_1.cluster) b ON b.booth = a.booth
                  group by 
                  a."Ec_ClientKey" ,
				    split_part(b.cluster, '_'::text, 2)::integer ,
				    b.cluster,
				    split_part(b.unit, '_UNIT_'::text, 2)::integer ,
				    b.unit,
				    a.booth::integer ,
				    a."Ec_PollingStation" ,
				    b.name,
				    b.gender,
				    b.age ,
				    b.contact,
				    b.voter_id ,
				    b.u_name,
				    b.u_gender,
				    b.u_age,
				    b.u_contact,
				    b.u_voterid,
				    b.c_name,
				    b.c_gender,
				    b.c_age,
				    b.c_contact,
				    b.c_voterid
WITH DATA;

-- View indexes:
CREATE INDEX mv_pc_incharges_dataset_clientkey_booth ON hubviews.mv_pc_incharges_dataset USING btree (client_key, booth_key, booth);

-- hubviews."VotersInfo" source

CREATE OR REPLACE VIEW hubviews."VotersInfo"
AS SELECT COALESCE(a."RequestorID", a."Ec_VoterId") AS "RequestorID",
    COALESCE(a."ClientKey", a."Ec_ClientKey") AS "ClientKey",
    COALESCE(a."Lang", a."Ec_Lang"::text) AS "Lang",
    COALESCE(a."Photo", ''::text) AS "Photo",
    COALESCE(a."RequestorType", 7) AS "RequestorType",
    COALESCE(a."RequestorTypeName", 'Indvidual'::text) AS "RequestorTypeName",
    COALESCE(a."RequestorFirstName", a."Ec_First_Name", ''::text) AS "RequestorFirstName",
    COALESCE(a."RequestorLastName", a."Ec_Last_Name", ''::text) AS "RequestorLastName",
    COALESCE(a."Age"::bigint, a."Ec_Age") AS "Age",
    COALESCE(a."Gender", a."Ec_Gender", ''::text) AS "Gender",
    COALESCE(a."Mobile", a."Ec_Mobile", ''::text) AS "Mobile",
    COALESCE(a."AlternateMobile", ''::text) AS "AlternateMobile",
    COALESCE(a."Email", ''::text) AS "Email",
    COALESCE(a."Occupation", a."Ec_OccupationID") AS "Occupation",
    COALESCE(a."Union", '{}'::integer[]) AS "Union",
    COALESCE(a."UnionInfo", '[]'::json) AS "UnionInfo",
    COALESCE(a."ConstituencyType", 'AC'::text) AS "ConstituencyType",
    COALESCE(a."ConstituencyKey"::bigint, a."Ec_ACKey") AS "ConstituencyKey",
    COALESCE(a."ConstituencyName", b."ACName"::text) AS "ConstituencyName",
    COALESCE(a."MandalKey", a."Ec_MandalKey") AS "MandalKey",
    COALESCE(a."Mandal", a."Ec_Mandal") AS "Mandal",
    COALESCE(a."VillageKey", a."Ec_VillageKey") AS "VillageKey",
    COALESCE(a."Village", a."Ec_Village") AS "Village",
    COALESCE(a."Door/FlatNo", a."Ec_HouseNumber") AS "Door/FlatNo",
    COALESCE(a."PinCode", ''::text) AS "PinCode",
    COALESCE(a."AddressLine1", ''::text) AS "AddressLine1",
    COALESCE(a."AddressLine2", ''::text) AS "AddressLine2",
    COALESCE(a."VoterID", a."Ec_VoterId") AS "VoterID",
    COALESCE(a."RelationType",
        CASE
            WHEN a."Ec_Relation" = 'father'::text AND COALESCE(a."Gender", a."Ec_Gender") = 'Male'::text THEN 'S/O'::text
            WHEN a."Ec_Relation" = 'father'::text AND COALESCE(a."Gender", a."Ec_Gender") = 'Female'::text THEN 'D/O'::text
            WHEN a."Ec_Relation" = 'husband'::text THEN 'W/O'::text
            WHEN a."Ec_Relation" = 'mother'::text AND COALESCE(a."Gender", a."Ec_Gender") = 'Male'::text THEN 'S/O'::text
            WHEN a."Ec_Relation" = 'mother'::text AND COALESCE(a."Gender", a."Ec_Gender") = 'FeMale'::text THEN 'D/O'::text
            WHEN a."Ec_Relation" = 'others'::text AND COALESCE(a."Gender", a."Ec_Gender") = 'Male'::text THEN 'Others'::text
            WHEN a."Ec_Relation" = 'daughter'::text AND COALESCE(a."Gender", a."Ec_Gender") = 'Male'::text THEN 'F/O'::text
            WHEN a."Ec_Relation" = 'daughter'::text AND COALESCE(a."Gender", a."Ec_Gender") = 'FeMale'::text THEN 'M/O'::text
            WHEN a."Ec_Relation" = 'wife'::text THEN 'H/O'::text
            WHEN a."Ec_Relation" = 'son'::text AND COALESCE(a."Gender", a."Ec_Gender") = 'Male'::text THEN 'F/O'::text
            WHEN a."Ec_Relation" = 'son'::text AND COALESCE(a."Gender", a."Ec_Gender") = 'FeMale'::text THEN 'M/O'::text
            ELSE NULL::text
        END) AS "RelationType",
    COALESCE(a."RelationName", a."Ec_RelationName", ''::text) AS "RelationName",
    COALESCE(a."RelationContact", ''::text) AS "RelationContact",
    COALESCE(a."RelationAlternateContact", ''::text) AS "RelationAlternateContact",
    COALESCE(a."Tags", '{0}'::text[]) AS "Tags",
    COALESCE(a."AdditionalInfo", '[]'::json) AS "AdditionalInfo",
    COALESCE(a."IsActive", true) AS "IsActive",
    COALESCE(a."CreatedBy", ''::text) AS "CreatedBy",
    COALESCE(a."CreatedUserName", ''::text) AS "CreatedUserName",
    a."CreatedOn",
    COALESCE(a."ModifiedBy", ''::text) AS "ModifiedBy",
    COALESCE(a."ModifiedUserName", ''::text) AS "ModifiedUserName",
    a."ModifiedOn",
    COALESCE(a."IsReferedBy", ''::text) AS "IsReferedBy",
    COALESCE(a."PGRSApplicantName", ''::text) AS "PGRSApplicantName",
    COALESCE(a."QualityScore", 0::double precision) AS "QualityScore",
    a."InvalidComments",
    COALESCE(a.first_name_last_name_cleansed, ''::text) AS first_name_last_name_cleansed,
    COALESCE(a.last_name_first_name_cleansed, ''::text) AS last_name_first_name_cleansed,
        CASE
            WHEN a."Caste" = ''::text THEN a."Ec_Caste"
            ELSE a."Caste"
        END AS "Caste",
    a."Ec_PollingStation" AS ec_ps,
    c.cluster_key,
    c.cluster,
    c.unit_key,
    c.unit,
    c.booth_key,
    c.booth
   FROM connecthub."VotersInfoEC" a
     LEFT JOIN ( SELECT DISTINCT "PCACVillages"."ACKey",
            "PCACVillages"."ACName",
            "PCACVillages"."ClientKey",
            "PCACVillages"."Lang"
           FROM masters."PCACVillages") b ON a."Ec_ClientKey" = b."ClientKey" AND a."Ec_Lang" = b."Lang" AND a."Ec_ACKey" = b."ACKey"
     LEFT JOIN hubviews.mv_pc_incharges_dataset c ON c.client_key = a."Ec_ClientKey" AND c.booth_key = split_part(a."Ec_PollingStation", '-'::text, 1)::integer AND c.booth = a."Ec_PollingStation";

CREATE OR REPLACE VIEW reporting.vw_party_cadre
AS SELECT COALESCE(a.voter_id, 'NA'::text) AS voter_id,
    a.clientkey,
    a.requestor_type_name,
    a.name,
    a.gender,
    a.mobile,
    a.email,
    a.occupation,
    a.constituency_name,
    a.mandal,
    a.village,
    a.is_registered_as_voter,
    COALESCE(b."Tags", c."Tags") AS tags,
    a.grievances,
        CASE
            WHEN a.grievances = 'None'::text THEN 'InActive'::text
            ELSE 'Active'::text
        END AS user_active,
    a.caste,
    a.party_name,
    a.ec_ps,
    row_number() OVER (PARTITION BY a.voter_id) AS voter_unq_id
   FROM ( SELECT q.voter_id,
            q.clientkey,
            q.requestor_type_name,
            q.name,
            q.gender,
            q.mobile,
            q.email,
            q.occupation,
            q.constituency_name,
            q.mandal,
            q.village,
            q.is_registered_as_voter,
            unnest(q.tags) AS tags,
            COALESCE(q.req_grievances, q.vot_grievances) AS grievances,
            q.caste,
            q.party_name,
            q.ec_ps
           FROM ( SELECT a_1.voter_id,
                    COALESCE(b_1.client_key, a_1."ClientKey") AS clientkey,
                    COALESCE(b_1.requestor_type_name, a_1."RequestorTypeName"::character varying) AS requestor_type_name,
                    COALESCE(b_1.name, a_1."Name"::character varying) AS name,
                    COALESCE(b_1.gender, a_1."Gender"::character varying) AS gender,
                    COALESCE(b_1.requestor_contact, a_1."Mobile"::character varying) AS mobile,
                    COALESCE(b_1.email, 'NA'::character varying) AS email,
                    COALESCE(b_1.occupation, 'None'::character varying) AS occupation,
                    COALESCE(b_1.constituency_name, a_1."ConstituencyName"::character varying) AS constituency_name,
                    COALESCE(b_1.mandal, a_1."Mandal") AS mandal,
                    COALESCE(b_1.village, a_1."Village") AS village,
                    COALESCE(b_1.is_registered_as_voter, 'Registered'::text) AS is_registered_as_voter,
                    COALESCE(a_1."Tags", '{6}'::text[]) AS tags,
                    unnest(b_1.requestor_grievances) AS req_grievances,
                    unnest(a_1.vot_grievances) AS vot_grievances,
                    COALESCE(b_1.caste, 'None'::character varying) AS caste,
                    COALESCE(b_1.partyname, 'None'::character varying) AS party_name,
                    COALESCE(a_1.ec_ps, 'None'::text) AS ec_ps
                   FROM ( SELECT "VotersInfo"."RequestorID" AS voter_id,
                            "VotersInfo"."ClientKey",
                            "VotersInfo"."RequestorType",
                            "VotersInfo"."RequestorTypeName",
                            concat("VotersInfo"."RequestorFirstName", "VotersInfo"."RequestorLastName") AS "Name",
                            "VotersInfo"."Age",
                            "VotersInfo"."Gender",
                            "VotersInfo"."Mobile",
                            "VotersInfo"."ConstituencyName",
                            "VotersInfo"."Mandal",
                            "VotersInfo"."Village",
                            "VotersInfo"."Tags",
                            '{None}'::text[] AS vot_grievances,
                            "VotersInfo".ec_ps
                           FROM hubviews."VotersInfo"
                          WHERE ('6'::text = ANY ("VotersInfo"."Tags")) AND "VotersInfo"."ClientKey" = 1) a_1
                     FULL JOIN ( SELECT vw_requestordataset.id,
                            vw_requestordataset.client_key,
                            vw_requestordataset.lang,
                            vw_requestordataset.photo,
                            vw_requestordataset.requestor_type,
                            vw_requestordataset.requestor_type_name,
                            vw_requestordataset.name,
                            vw_requestordataset.registration_number,
                            vw_requestordataset.age,
                            vw_requestordataset.age_group,
                            vw_requestordataset.gender,
                            vw_requestordataset.requestor_contact,
                            vw_requestordataset.requestor_alternate_contact,
                            vw_requestordataset.mobile,
                            vw_requestordataset.email,
                            vw_requestordataset.occupation_id,
                            vw_requestordataset.occupation,
                            vw_requestordataset.constituency_type,
                            vw_requestordataset.constituency_key,
                            vw_requestordataset.constituency_name,
                            vw_requestordataset.state_key,
                            vw_requestordataset.state,
                            vw_requestordataset.district_key,
                            vw_requestordataset.district,
                            vw_requestordataset.mandal_key,
                            vw_requestordataset.mandal,
                            vw_requestordataset.village_key,
                            vw_requestordataset.village,
                            vw_requestordataset.door_flat_no,
                            vw_requestordataset.pin_code,
                            vw_requestordataset.address_line1,
                            vw_requestordataset.address_line2,
                            vw_requestordataset.voter_id,
                            vw_requestordataset.is_registered_as_voter,
                            vw_requestordataset.relation_type,
                            vw_requestordataset.relation_name,
                            vw_requestordataset.relation_contact,
                            vw_requestordataset.relation_alternate_contact,
                            vw_requestordataset.tags,
                            vw_requestordataset.requestor_grievance_info,
                            vw_requestordataset.requestor_grievances,
                            vw_requestordataset.requestor_reference_type,
                            vw_requestordataset.requestor_reference_relation,
                            vw_requestordataset.additional_info,
                            vw_requestordataset.members,
                            vw_requestordataset.is_active,
                            vw_requestordataset.created_by,
                            vw_requestordataset.created_user_name,
                            vw_requestordataset.created_on,
                            vw_requestordataset.created_on_month,
                            vw_requestordataset.created_on_year,
                            vw_requestordataset.created_on_month_year,
                            vw_requestordataset.caste,
                            vw_requestordataset.additional_info_json,
                            vw_requestordataset.partyname
                           FROM hubviews.vw_requestordataset
                          WHERE ('Party Cadre'::text = ANY (vw_requestordataset.tags)) AND vw_requestordataset.client_key = 1) b_1 ON a_1.voter_id = b_1.voter_id::text AND a_1."ClientKey" = COALESCE(b_1.client_key, 1)) q) a
     LEFT JOIN ( SELECT "TagList"."ID",
            "TagList"."TagType",
            "TagList"."Tags",
            "TagList"."IsActive",
            "TagList"."ClientKey",
            "TagList"."Lang",
            "TagList"."ReportingCategory"
           FROM masters."TagList"
          WHERE "TagList"."TagType"::text = 'RequestorType'::text AND "TagList"."ClientKey" = 1) b ON a.tags::integer = b."ID" AND a.requestor_type_name::text = 'Individual'::text
     LEFT JOIN ( SELECT "TagList"."ID",
            "TagList"."TagType",
            "TagList"."Tags",
            "TagList"."IsActive",
            "TagList"."ClientKey",
            "TagList"."Lang",
            "TagList"."ReportingCategory"
           FROM masters."TagList"
          WHERE "TagList"."TagType"::text = 'AssociationType'::text AND "TagList"."ClientKey" = 1) c ON a.tags::integer = c."ID" AND a.requestor_type_name::text = 'Association'::text;

-- connecthub."VotersInformation" source

CREATE OR REPLACE VIEW connecthub."VotersInformation"
AS WITH pcacmasters AS (
         SELECT "PCACVillages"."ACKey",
            "PCACVillages"."StateKey",
            "PCACVillages"."State",
            "PCACVillages"."DistrictKey",
            "PCACVillages"."District",
            "PCACVillages"."MandalKey",
            "PCACVillages"."ACName",
            "PCACVillages"."ClientKey",
            "PCACVillages"."Lang",
            row_number() OVER (PARTITION BY "PCACVillages"."ClientKey", "PCACVillages"."Lang", "PCACVillages"."MandalKey" ORDER BY "PCACVillages"."ACKey") AS rn
           FROM masters."PCACVillages"
        )
 SELECT 'Not Verified'::text AS "VerificationStatus",
    'Voter'::text AS "Source",
    a."RequestorID",
    a."ClientKey",
    a."Lang",
    a."Photo",
    a."RequestorType",
    a."RequestorTypeName",
    a."RequestorFirstName",
    a."RequestorLastName",
    a."Age",
    a."Gender",
    a."Mobile",
    a."AlternateMobile",
    a."Email",
    a."Occupation",
    o."Occupation" AS "OccupationName",
    a."Union",
    a."UnionInfo",
    a."ConstituencyType",
    a."ConstituencyKey",
    a."ConstituencyName",
    b."StateKey",
    b."State",
    b."DistrictKey",
    b."District",
    a."MandalKey",
    a."Mandal",
    a."VillageKey",
    a."Village",
    a."Door/FlatNo",
    a."PinCode",
    a."AddressLine1",
    a."AddressLine2",
    a."VoterID",
    a."RelationType",
    a."RelationName",
    a."RelationContact",
    a."RelationAlternateContact",
    a."Tags",
    a."AdditionalInfo",
    a."IsActive",
    a."CreatedBy",
    a."CreatedUserName",
    a."CreatedOn",
    a."ModifiedBy",
    a."ModifiedUserName",
    a."ModifiedOn",
    a."QualityScore",
    a."InvalidComments",
    a."Caste",
    a.ec_ps,
    a.cluster_key,
    a.cluster,
    a.unit_key,
    a.unit,
    a.booth_key,
    a.booth
   FROM hubviews."VotersInfo" a
     LEFT JOIN pcacmasters b ON a."ClientKey" = b."ClientKey" AND a."Lang" = b."Lang"::text AND a."MandalKey" = b."MandalKey" AND b.rn = 1
     LEFT JOIN masters."OccupationList" o ON o."ClientKey" = a."ClientKey" AND o."Lang"::text = a."Lang" AND o."ID" = a."Occupation";



-- hubviews.vw_voterdataset source

CREATE OR REPLACE VIEW hubviews.vw_voterdataset
AS SELECT "VoterID" AS id,
    "ClientKey" AS client_key,
    "Lang" AS lang,
    COALESCE("Photo", 'NA'::text) AS photo,
    "RequestorType" AS requestor_type,
    "RequestorTypeName" AS requestor_type_name,
    ("RequestorFirstName" || ' '::text) || COALESCE("RequestorLastName", ''::text) AS name,
    'NA'::text AS registration_number,
    COALESCE("Age", NULL::bigint) AS age,
        CASE
            WHEN "Age" < 18 THEN 'Minor'::text
            WHEN "Age" >= 18 AND "Age" <= 25 THEN 'AgeGroup(18-25)'::text
            WHEN "Age" >= 26 AND "Age" <= 30 THEN 'AgeGroup(26-30)'::text
            WHEN "Age" >= 31 AND "Age" <= 35 THEN 'AgeGroup(31-35)'::text
            WHEN "Age" >= 36 AND "Age" <= 40 THEN 'AgeGroup(36-40)'::text
            WHEN "Age" >= 41 AND "Age" <= 45 THEN 'AgeGroup(41-45)'::text
            WHEN "Age" >= 46 AND "Age" <= 50 THEN 'AgeGroup(46-50)'::text
            WHEN "Age" >= 51 AND "Age" <= 55 THEN 'AgeGroup(51-55)'::text
            WHEN "Age" >= 56 AND "Age" <= 60 THEN 'AgeGroup(56-60)'::text
            WHEN "Age" >= 61 AND "Age" <= 65 THEN 'AgeGroup(61-65)'::text
            WHEN "Age" >= 66 AND "Age" <= 70 THEN 'AgeGroup(66-70)'::text
            WHEN "Age" >= 71 AND "Age" <= 75 THEN 'AgeGroup(71-75)'::text
            WHEN "Age" > 75 THEN 'AgeGroup(>75)'::text
            ELSE 'NA'::text
        END AS age_group,
    COALESCE("Gender", 'NA'::text) AS gender,
    COALESCE("Mobile", 'NA'::text) AS requestor_contact,
    COALESCE("AlternateMobile", 'NA'::text) AS requestor_alternate_contact,
    string_to_array(concat("Mobile",
        CASE
            WHEN "AlternateMobile" = ''::text THEN NULL::text
            ELSE ','::text || "AlternateMobile"
        END), ','::text) AS mobile,
    COALESCE("Email", 'NA'::text) AS email,
    COALESCE("Occupation", 0) AS occupation_id,
    COALESCE("OccupationName", 'NA'::text::character varying) AS occupation,
    COALESCE("ConstituencyType", 'NA'::text) AS constituency_type,
    COALESCE("ConstituencyKey", 0::bigint) AS constituency_key,
    COALESCE("ConstituencyName", 'NA'::character varying::text) AS constituency_name,
    "StateKey" AS state_key,
    "State" AS state,
    COALESCE("DistrictKey", 0) AS district_key,
    COALESCE("District", 'NA'::text) AS district,
    COALESCE("MandalKey", 0) AS mandal_key,
    COALESCE("Mandal", 'NA'::text) AS mandal,
    COALESCE("VillageKey", 0) AS village_key,
    COALESCE("Village", 'NA'::text) AS village,
    COALESCE("Door/FlatNo", 'NA'::text) AS door_flat_no,
    COALESCE("PinCode", 'NA'::text) AS pin_code,
    COALESCE("AddressLine1", 'NA'::text) AS address_line1,
    COALESCE("AddressLine2", 'NA'::text) AS address_line2,
    COALESCE("VoterID", 'NA'::text) AS voter_id,
    COALESCE("RelationType", 'NA'::text) AS relation_type,
    COALESCE("RelationName", 'NA'::text) AS relation_name,
    COALESCE("RelationContact", 'NA'::text) AS relation_contact,
    COALESCE("RelationAlternateContact", 'NA'::text) AS relation_alternate_contact,
    "Tags" AS tags,
    '[]'::json AS requestor_grievance_info,
    '{}'::text[] AS requestor_grievances,
    '{}'::text[] AS requestor_reference_type,
    '{}'::text[] AS requestor_reference_relation,
    COALESCE("AdditionalInfo", '[]'::json) AS additional_info,
    '[]'::json AS members,
    "IsActive" AS is_active,
    COALESCE("CreatedBy", 'NA'::text) AS created_by,
    COALESCE("CreatedUserName", 'NA'::text) AS created_user_name,
    "CreatedOn" AS created_on,
    "ModifiedOn" AS modified_on,
    'NA'::text AS created_on_month,
    'NA'::text AS created_on_year,
    'NA'::text AS created_on_month_year,
    "QualityScore"::integer AS qualityscore,
    "InvalidComments" AS invalidcomments,
    "Caste" AS caste,
    ec_ps,
    cluster_key,
    cluster,
    unit_key,
    unit,
    booth_key,
    booth
   FROM connecthub."VotersInformation";

INSERT INTO connecthub."UserInfo"
("ClientKey", "UserKey", "Email", "IsActive", "UserName", "Lang")
VALUES(1, '0 - 0 - 0 - 0 - 0', '', true, 'UnAssigned', 'en'),
(3, '0 - 0 - 0 - 0 - 0', '', true, 'UnAssigned', 'en'),
(4, '0 - 0 - 0 - 0 - 0', '', true, 'UnAssigned', 'en')