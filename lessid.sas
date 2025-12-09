/* Expecting salt, input_path, output_type, output_path set in init_stmt */
/* output_type can be 'sas7bdat' or 'csv', default to sas7bdat */

/* Determin date_shift_days, default to 30 if not provided */
%let date_shift_days = %sysfunc(ifc(%symexist(date_shift_days), &date_shift_days, 30));

%let id_columns = 
"addressid", "conditionid", 
"diagnosisid", "dispensingid", "encounterid", 
"facilityid", "geocodeid", "immunizationid", 
"lab_facilityid", "lab_result_cm_id", "labhistoryid", 
"medadmin_providerid", "medadminid", 
"obsclin_providerid", "obsclinid", "obsgen_providerid", 
"obsgenid", "org_patid", "participantid", 
"patid", "person_id", "prescribingid", 
"pro_cm_id", "proceduresid", "providerid", 
"raw_siteid", "rx_providerid", "trial_siteid", 
"trialid", "visit_id", "vitalid", 
"vx_providerid";

/* Read in the original dataset from SAS7BDAT file */
data original_data;
    set "&input_path";
run;

proc print data=original_data (obs=10);
    title 'Sample Input Data';
run;

/* hash of id columns */
/* Using PROC SQL to get column select components */
proc sql;
    create table select_components as
    select
        name as original_column_name,
        CASE
            WHEN lower(name) in (&id_columns) THEN
                'CASE WHEN ' || name || ' IS NOT NULL THEN ' ||
                    'SHA256HEX(TRIM(' || name || ") || '&salt') " || 
                "ELSE " || name || " END AS " || name || ' LENGTH=64'
            ELSE name
        END as select_component
    from dictionary.columns
    where libname = 'WORK' and memname = 'ORIGINAL_DATA';
quit;

/* proc print data=select_components; */
/*     title 'Column select components'; */
/* run; */

/* Create macro variable with all select components */
proc sql noprint;
    select select_component into :select_list separated by ', '
    from select_components;
quit;

/* Display the generated select list */
%put Generated SELECT list: &select_list;

/* Build and execute the final SQL query for id hashing*/
proc sql;
    create table hashed_data as
    select &select_list
    from original_data;
quit;

/* date shifting */
/* check if patid exists */
proc sql noprint;
    select count(*) into :patid_exists
    from dictionary.columns
    where libname='WORK'
          and memname='HASHED_DATA'
          and lowcase(name)='patid';
quit;

/* Identify all numeric date columns in hashed_data */
proc sql noprint;
    select name
        into :date_cols separated by ' '
    from dictionary.columns
    where libname='WORK'
          and memname='HASHED_DATA'
          and lowcase(name) like '%_date'
          and type='num';
quit;

%let date_count = &sqlobs; 

%if &date_count > 0 %then %do;
    %put Date columns to be shifted: &date_cols;
    DATA hashed_data;
        set hashed_data;

        /* Calculate day_shift once per row */
        if &patid_exists > 0 then do;
            length patid_salted_hash $64;
            patid_salted_hash = SHA256HEX(trim(patid) || "&salt");
            day_shift = mod(input(substr(patid_salted_hash, 1, 8), hex8.), &date_shift_days * 2 + 1) - &date_shift_days;
            drop patid_salted_hash;
        end;
        else day_shift = mod(input(substr(SHA256HEX("&salt"), 1, 8), hex8.), &date_shift_days * 2 + 1) - &date_shift_days;

        /* Shift all date columns using the same day_shift */
        array date_vars {*} &date_cols;
        do i = 1 to dim(date_vars);
            /* Shift non-missing date values in all identified date columns */
            if not missing(date_vars{i}) then do;
                date_vars{i} = intnx('day', date_vars{i}, day_shift);
                date_vars{i} = min(max(date_vars{i}, '01JAN1900'd), '31DEC9999'd); /* ensure date within valid range */
            end;
        end;

        drop day_shift;
        drop i;
    run;

    /* Apply DATE9. format to these columns only if any exist */
    proc datasets lib=work nolist;
        modify hashed_data;
        format &date_cols YYMMDD10.;
    quit;
%end;

proc print data=hashed_data (obs=10);
    title 'Sample Output Data';
run;

/* Determine output type and write the output file accordingly */
%let output_type = %sysfunc(ifc(%superq(output_type)=, sas7bdat, %lowcase(%superq(output_type))));

%if &output_type = sas7bdat %then %do;
    /* Output sas7bdat File */
    data "&output_path" (compress=yes);
        set hashed_data;
    run;
%end;

%if &output_type = csv %then %do;
    /* Output CSV File */
    proc export data=hashed_data
        outfile="&output_path"
        dbms=csv
        replace;
    run;
%end;