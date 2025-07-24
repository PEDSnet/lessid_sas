/* Expecting salt, input_path, output_path set in init_stmt */
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

/* Using PROC SQL to get column select components */
proc sql;
    create table select_components as
    select
        name as original_column_name,
        CASE
            WHEN lower(name) in (&id_columns) THEN
                'CASE WHEN ' || name || ' IS NOT NULL THEN ' ||
                    'SHA256HEX(' || name || " || '&salt') " || 
                "ELSE " || name || " END AS " || name || ' LENGTH=64'
            ELSE name
        END as select_component
    from dictionary.columns
    where libname = 'WORK' and memname = 'ORIGINAL_DATA';
quit;

proc print data=select_components;
    title 'Column select components';
run;

/* Create macro variable with all select components */
proc sql noprint;
    select select_component into :select_list separated by ', '
    from select_components;
quit;

/* Display the generated select list */
%put Generated SELECT list: &select_list;

/* Build and execute the final SQL query */
proc sql;
    create table hashed_data as
    select &select_list
    from original_data;
quit;

proc print data=hashed_data (obs=10);
    title 'Sample Output Data';
run;

/* Output File */
data "&output_path";
    set hashed_data;
run;