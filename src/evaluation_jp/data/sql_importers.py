from typing import List

import pandas as pd

from evaluation_jp.data.sql_utils import sqlserver_engine, temp_table_connection, unpack, sql_format


def get_edw_customer_details(
    ppsns_to_lookup: pd.Series,
    ppsn_column_name: str = "ppsn",
    lookup_columns: List = None,
    reference_date: pd.Timestamp = pd.Timestamp.now(),
) -> pd.DataFrame:
    """Generic EDW SCD import function
    Requires: a pd.Series or pd.DataFrame of ppsns_to_lookup (from ppsn_column_name, default "ppsn")
    Returns time-specific values at reference_date (or now if reference_date omitted)
    for lookup_columns (or all columns if no columns specified)
    """
    edw_database = "Stat"
    edw_schema = "edw"
    tempdb_engine = sqlserver_engine("CSKMA0400\\STATS1", "tempdb")
    with temp_table_connection(
        tempdb_engine, ppsns_to_lookup, "ppsn_table"
    ) as temp_table_con:
        query_columns = (
            unpack([f"edw.{col}" for col in lookup_columns])
            if lookup_columns is not None
            else "edw.*"
        )
        edw_table_query = f"""\
            SELECT  ppsn_table.{ppsn_column_name}, {query_columns}
            FROM    ppsn_table
                    INNER JOIN {edw_database}.{edw_schema}.dim_customer_details AS edw
                            ON ppsn_table.{ppsn_column_name} = edw.pps_no 
            WHERE   edw.scd_start_date <= {sql_format(reference_date)}
            AND     {sql_format(reference_date)} <= edw.scd_end_date
        """
        return pd.read_sql(edw_table_query, temp_table_con)


def get_earnings_contributions_data(
    ppsns_to_lookup: pd.Series,
    ppsn_column_name: str = "ppsn",
    lookup_columns: List = None,
    start_year: int = None,
    end_year: int = None,
) -> pd.DataFrame:
    """Get earnings and contributions data from CRS for DataFrame or Series of ppsns_to_lookup.
    ppsn_column_name for ppsns_to_lookup is "ppsn" by default.
    Will return lookup_columns from earnings/contributions database if specified, otherwise all columns.
    Optionally use start_year and/or end_year to restrict range of years returned.
    """
    # Get BOMi ids for ppsns
    ids_to_lookup = get_edw_customer_details(
        ppsns_to_lookup, ppsn_column_name, lookup_columns=["customer_id"]
    )
    # Use table with ppsns and BOMi ids to lookup earnings and contributions data
    tempdb_engine = sqlserver_engine("CSGPC-BPRD-SQ06\\PA1", "tempdb")
    with temp_table_connection(
        tempdb_engine, ids_to_lookup, "##id_table"
    ) as temp_table_con:
        query = f"""\
            SELECT  ##id_table.{ppsn_column_name},
                    abstract_cons.year,
                    {unpack([f"concrete_cons.{col}" for col in lookup_columns])
                        if lookup_columns is not None
                        else "concrete_cons.*"
                    }
            FROM    ##id_table
                    INNER JOIN dbbomcrs.employment.abstract_contribution_year AS abstract_cons
                        ON ##id_table.customer_id = abstract_cons.customer_id
                    RIGHT JOIN dbbomcrs.employment.contribution_year_payment_line AS concrete_cons
                        ON abstract_cons.contribution_year_id = concrete_cons.contribution_year_id
            WHERE   ##id_table.customer_id IS NOT NULL
            AND     concrete_cons.valid_return = 1
            {f'AND  {start_year} <= abstract_cons.year' if start_year is not None else ''}
            {f'AND  abstract_cons.year <=  {end_year}' if end_year is not None else ''}
            AND     concrete_cons.valid_return = 1
            ORDER BY {ppsn_column_name}, year  
        """
        return pd.read_sql(query, temp_table_con)


# //TODO Data cleaning for earnings data 


# earn = pd.read_csv('\\\\cskma0294\\F\\HC\\anon_earn_sample.zip') 


# #Exclude less than 2 working weeks per year and less than 500/year income 
# earn = earn[earn['NO_OF_CONS'] >= 2] 
# earn = earn[earn['EARNINGS_AMT'] > 500] 

# #If numer of contriubtions is greater than 53, set equal to 53, exclude class S and M 
# earn.loc[earn['NO_OF_CONS'] > 53, 'NO_OF_CONS'] = 53 
# earn = earn[~earn['CONS_CLASS_CODE'].str.contains('S')] 
# earn = earn[~earn['CONS_CLASS_CODE']]



# * Separately look up deceased status from SCD table
# death_event_date

# # Earnings and contributions from BOMi CRS
# contribution_year_id_table = "Abstract_Contribution_Year"
# # ctb table
# contribution_year_table = "Contribution_Year_Payment_Line"
# # credit table
# credit_year_table = "Contribution_Year_Credit_Line"

cons_query = """\
    SELECT  t1.customer_id,
            t2.year               AS year,
			t3.contribution_class AS cons_class,
            t3.wies               AS paid_cons
    FROM    ##temptable_cust AS t1
            INNER JOIN dbbomcrs.employment.abstract_contribution_year AS t2
                    ON t1.customer_id = t2.customer_id
            RIGHT JOIN dbbomcrs.employment.contribution_year_payment_line AS t3
                    ON t2.contribution_year_id = t3.contribution_year_id
    WHERE   t1.customer_id IS NOT NULL
    AND     t2.[valid_return] = 1
    ORDER   BY  customer_id,
                year;"""

# credits_query = """\
#     SELECT	t1.customer_id,
# 			t2.year,
# 			t3.credit_type_code,
# 			t4.description,
# 			t3.number_of_credits
#     FROM    ##temptable_cust AS t1
#             LEFT join dbbomcrs.employment.abstract_contribution_year AS t2
#                 ON t1.customer_id = t2.customer_id
#             RIGHT join dbbomcrs.employment.contribution_year_credit_line AS t3
#                 ON t3.contribution_year_id = t2.contribution_year_id
# 			INNER JOIN dbBomCrs.Employment.Contribution_Credit_Type AS t4
# 				ON t3.credit_type_code = t4.contribution_credit_type_code
#     WHERE   t1.customer_id IS NOT NULL AND t2.year <=2019;"""

# tax_year_query = """\
#     SELECT [con_year] AS year
#         ,[sex] AS gender
#         ,[con_year_start_date] AS start_date
#         ,[con_year_end_date] AS end_date
#         ,[no_of_weeks] AS YA_adjusted_weeks
#     FROM [dbBommain].[dbo].[Contribution_Year_Definition]
#     WHERE con_year >'1965' AND con_year < '2020';"""
