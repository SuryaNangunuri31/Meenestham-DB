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
