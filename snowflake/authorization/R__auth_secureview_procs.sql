
CREATE OR REPLACE PROCEDURE
get_protected_tables_list(schema_name varchar(512))
RETURNS ARRAY
LANGUAGE JAVASCRIPT
as
    /*
    Returns an array list of all column names in a table that have a policy attached to it
    */
    $$
    "use strict";

    const tabs_query = `select distinct lower(table_name)
            from auth_rules ar
            join auth_policies_rules apr on (apr.rule_id = ar.id)
            join auth_policies ap on (ap.id = apr.policy_id)
            where schema_name = ? and ap.is_enabled`;

    const res = snowflake.createStatement({
        sqlText: tabs_query,
        binds: [SCHEMA_NAME]
    }).execute();

    const tables_arr = [];
    while (res.next()) {
        const tab = res.getColumnValue(1).toUpperCase();
        tables_arr.push(tab);
    }

    return tables_arr;
    $$
;

CREATE OR REPLACE PROCEDURE
get_table_columns_list(schema_name varchar(512), table_name varchar(512))
RETURNS ARRAY
LANGUAGE JAVASCRIPT
as
    $$
    /*
    Returns an array list of all column names in a table using information_schema
    */
    "use strict";

    const cols_query = `select column_name from information_schema.columns c
            where lower(table_schema) = CONCAT(:1, '_protected') and lower(c.table_name) = :2
            order by ordinal_position`;


    const res = snowflake.createStatement({
        sqlText: cols_query,
        binds: [SCHEMA_NAME, TABLE_NAME]
    }).execute();

    const all_cols_arr = [];
    while (res.next()) {
        const curr_col = res.getColumnValue(1).toUpperCase();
        all_cols_arr.push(curr_col);
    }

    return all_cols_arr;
    $$
;

CREATE OR REPLACE PROCEDURE
get_protected_columns_map(schema_name varchar, table_name varchar(512))
RETURNS VARIANT
LANGUAGE JAVASCRIPT
as
    /*
    Returns a map of protected column names in a table based on onemedical_dw_protected.auth_* policy data
    */
    $$
    "use strict";

    const cols_query = `select distinct column_name, column_mask_type
            from auth_rules ar
                join auth_policies_rules apr on (apr.rule_id = ar.id)
                join auth_policies ap on (ap.id = apr.policy_id)
            where
                lower(ar.schema_name) = :1
                and lower(ar.table_name) = :2
                and ap.is_enabled and ar.filter_exp is NULL
            `;

    const res = snowflake.createStatement({
        sqlText: cols_query,
        binds: [SCHEMA_NAME, TABLE_NAME]
    }).execute();

    let protected_cols = {};
    while (res.next()) {
        const curr_col = res.getColumnValue(1).toUpperCase();
        protected_cols[curr_col] = res.getColumnValue(2);
    }

    return protected_cols;

    $$
;


CREATE OR REPLACE PROCEDURE
get_row_filter_expression(schema_name varchar, table_name varchar(512))
RETURNS ARRAY
LANGUAGE JAVASCRIPT
as
    /*
    Returns a map of protected column names in a table based on onemedical_dw_protected.auth_* policy data
    */
    $$
    "use strict";

    const filt_query = `select distinct filter_exp
            from auth_rules ar
                join auth_policies_rules apr on (apr.rule_id = ar.id)
                join auth_policies ap on (ap.id = apr.policy_id)
            where
                lower(ar.schema_name) = :1
                and lower(ar.table_name) = :2
                and ap.is_enabled and ar.filter_exp is NOT NULL
            `;

    const res = snowflake.createStatement({
        sqlText: filt_query,
        binds: [SCHEMA_NAME, TABLE_NAME]
    }).execute();

    const filt_exp = [];
    while (res.next()) {
        filt_exp.push(res.getColumnValue(1));
    }

    return filt_exp
    $$
;


CREATE OR REPLACE FUNCTION is_column_protected_actor(schema_name varchar, table_name varchar, col_name varchar)
RETURNS BOOLEAN
AS
    /*
    Returns boolean based on whether current_user or current_role has auth rules defined for given table.column
    */
$$
        select count(1)>0 as has_rules
          from auth_enabled_rules aer
          where 1=1
            and aer.schema_name = schema_name and aer.table_name = table_name and aer.column_name = lower(col_name)
            and ((aer.actor_type = 'user_id' and upper(aer.actor_name) = CURRENT_USER()) OR (aer.actor_type = 'role_id' and upper(aer.actor_name) = CURRENT_ROLE()))
$$
;

CREATE OR REPLACE FUNCTION is_row_protected_actor(schema_name varchar, table_name varchar, curr_filter_exp varchar)
RETURNS BOOLEAN
AS
    /*
    Returns boolean based on whether current_user or current_role has row_filter rules defined for given table
    */
$$
        select count(1)>0 as has_rules
          from auth_enabled_rules aer
          where 1=1
            and aer.schema_name = schema_name and aer.table_name = table_name and aer.filter_exp = curr_filter_exp
            and ((aer.actor_type = 'user_id' and upper(aer.actor_name) = CURRENT_USER()) OR (aer.actor_type = 'role_id' and upper(aer.actor_name) = CURRENT_ROLE()))
$$
;

CREATE OR REPLACE PROCEDURE create_protected_view(schema_name varchar, table_name varchar)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
    /*
    Dynamically constructs View statement for schema_name.table_name based on masking rules from onemedical_dw_protected.auth_* metadata.
    The view statement is executed and Returns string representing "Success" or "<failure error message>"
    */
$$
    "use strict";

    function execAndReturn(sql, bindVars, retColPosition=1) {
        const stmt = snowflake.createStatement({
            sqlText: sql,
            binds: bindVars
        });
        const res = stmt.execute();
        res.next();

        return res.getColumnValue(retColPosition);
    }

    function addViewColumnMasks(vw_stmt, all_cols_list, protected_cols) {
        for (let i = 0; i < all_cols_list.length; i++) {
            let curr_col = all_cols_list[i];

            if (curr_col in protected_cols) {
                vw_stmt += `CASE WHEN onemedical_dw_protected.is_column_protected_actor('${SCHEMA_NAME}', '${TABLE_NAME}', '${curr_col}') ` +
                    `THEN ` +
                    `CASE '${protected_cols[curr_col]}' ` +
                    `  WHEN 'HASH' THEN MD5(${curr_col}) ` +
                    `  WHEN 'LAST4' THEN
                          IFF(
                            LENGTH(${curr_col})>4,
                            CONCAT(
                                REGEXP_REPLACE(
                                    SUBSTR(${curr_col}, 1, LENGTH(${curr_col})-4),
                                    '\\\\S', 'X'
                                ),
                                RIGHT(${curr_col}, 4)
                            ),
                            'XXXX'
                          )` +
                    `  ELSE 'XXXX' ` +
                    `END ` +
                    `ELSE ${all_cols_list[i]} ` +
                    `END as ${curr_col}, \n`;
            } else {
                vw_stmt += `${all_cols_list[i]}, \n`;
            }
        }

        return vw_stmt.replace(/, \n$/, "");
    }

    function addViewWhereClause(vw_stmt, filter_exp_list) {
        if (filter_exp_list.length > 0) {
            vw_stmt += `\n WHERE 1=1 \n `;

            for (let i = 0; i < filter_exp_list.length; i++) {
                const filt_exp = filter_exp_list[i];
                const filt_exp_esc = filt_exp.replace(/'/g, '\\\'');

                // short-circuit the filters applicable only for current user
                vw_stmt += ` AND (NOT onemedical_dw_protected.is_row_protected_actor('${SCHEMA_NAME}', '${TABLE_NAME}', '${filt_exp_esc}') OR ${filt_exp}) \n`;
            }
        }

        return vw_stmt;
    }

    // get list of all column names for the table
    const all_cols_list = execAndReturn(
        "call get_table_columns_list(?, ?);", [SCHEMA_NAME, TABLE_NAME],
        1
    );

    // get list of protected column names for the table
    const protected_cols = execAndReturn(
        "call get_protected_columns_map(?, ?);", [SCHEMA_NAME, TABLE_NAME],
        1
    );

    // construct the secure view for <schema_name.table_name>
    let vw_stmt = `CREATE OR REPLACE SECURE VIEW ${SCHEMA_NAME}.${TABLE_NAME} AS SELECT \n`;
    vw_stmt = addViewColumnMasks(vw_stmt, all_cols_list, protected_cols);
    vw_stmt += `\n FROM ${SCHEMA_NAME}_protected.${TABLE_NAME} `;

    // check for row filters and extend where clause if necessary
    const filter_exp_list = execAndReturn(
        "call get_row_filter_expression(?, ?);", [SCHEMA_NAME, TABLE_NAME],
        1
    );
    vw_stmt = addViewWhereClause(vw_stmt, filter_exp_list);


    // execute the view statement
    try {
        var rs = snowflake.execute({
            sqlText: vw_stmt + ";"
        });
        return "Success"
    } catch (err) {
        return "Failed: " + vw_stmt + "\n\n" + err;
    }


$$
;

CREATE OR REPLACE PROCEDURE create_refresh_metadata()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
    /*
    Util script to recreate denormalized metadata table auth_enabled_rules;
    Should be called when Policy data is updated or before calling create_refresh_protected_views()
    */
$$
    const sqlText = `
    create or replace table auth_enabled_rules as
        select
            ar.schema_name,
            ar.table_name,
            ar.column_name,
            ar.column_mask_type,
            ar.filter_exp,
            ap.name policy_name,
            ap.is_enabled,
            aap.actor_id,
            aap.policy_id,
            act.type actor_type,
            act.name actor_name
        from auth_rules ar
            join auth_policies_rules apr on (ar.id = apr.rule_id)
            join auth_policies ap on (ap.id = apr.policy_id)
            join auth_actors_policies aap on (aap.policy_id = apr.policy_id)
            join auth_actors act on (act.id = aap.actor_id)
        where 1=1
        and ap.is_enabled;
    `
    try {
        const rs = snowflake.execute({
            sqlText: sqlText
        });
        return "Success"
    } catch (err) {
        return "Failed: " + sqlText + "\n\n" + err;
    }
$$
;


CREATE OR REPLACE PROCEDURE create_refresh_protected_views(schema_name varchar)
RETURNS ARRAY
LANGUAGE JAVASCRIPT
AS
    /*
    Wrapper script to call PROCEDURE create_protected_view() for each protected table as defined in onemedical_dw_protected.auth_* metadata
    Returns an array messages as retunred by create_protected_view() per table
    */
$$
    "use strict";

    // get list of all protected table names for the schema
    let stmt = snowflake.createStatement({
        sqlText: "call get_protected_tables_list(?);",
        binds: [SCHEMA_NAME]
    });
    let res = stmt.execute();
    res.next();
    const tables_list = res.getColumnValue(1);

    let ret = [];
    for (let i = 0; i < tables_list.length; i++) {
        const curr_tab = tables_list[i].toLowerCase();

        stmt = snowflake.createStatement({
            sqlText: "call create_protected_view(?, ?);",
            binds: [SCHEMA_NAME, curr_tab]
        });
        res = stmt.execute();
        res.next();
        ret.push(res.getColumnValue(1));
    }

    return ret;


$$
;

call create_refresh_protected_views('onemedical_dw');
call create_refresh_protected_views('onelife_replica');
