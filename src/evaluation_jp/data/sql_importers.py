import pandas as pd


from evaluation_jp.data.sql_utils import *


def get_edw_customer_details(
    ppsns_to_lookup: pd.Series,
    ppsn_column_name: str = "ppsn",
    lookup_columns: List = None,
    reference_date: pd.Timestamp = pd.Timestamp.now(),
) -> pd.DataFrame:
    """Generic EDW SCD import function
    Requires: a pd.Series or pd.DataFrame of ppsns_to_lookup (from ppsn_column_name )
    Returns time-specific values at reference_date (or now if reference_date omitted)
    for lookup_columns (or all columns if no columns specified)
    """
    server = "CSKMA0400\\STATS1"
    edw_database = "Stat"
    edw_schema = "edw"
    tempdb_engine = sqlserver_engine(server, "tempdb")
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
            WHERE   edw.scd_start_date <= {sql_clause_format(reference_date)}
            AND     {sql_clause_format(reference_date)} <= edw.scd_end_date
        """
        return pd.read_sql(edw_table_query, temp_table_con)


def get_earnings_contributions_data(
    ppsns_to_lookup: pd.Series,
    ppsn_column_name: str = "ppsn",
    lookup_columns: List = None,
    start_year: int = None,
    end_year: int = None,
) -> pd.DataFrame:
    # Get BOMi ids for ppsns
    ids_to_lookup = get_edw_customer_details(
        ppsns_to_lookup, ppsn_column_name, lookup_columns=["customer_id"]
    )
    # Create temp_customerid_year table
    server = "CSGPC-BPRD-SQ06\\PA1"
    tempdb_engine = sqlserver_engine(server, "tempdb")
    with temp_table_connection(
        tempdb_engine, ids_to_lookup, "##id_table"
    ) as temp_table_con:
        query_columns = (
            unpack([f"concrete_cons.{col}" for col in lookup_columns])
            if lookup_columns is not None
            else "concrete_cons.*"
        )
        query = f"""\
            SELECT  ##id_table.{ppsn_column_name},
                    abstract_cons.year,
                    {query_columns}
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
                    


    #


# *Get vital statistics for a reference date [<- PopulationSlice]
#

# *Death check


# # STATS1 server tempdb
# stats_tempdb_engine = st.server_connection(server="CSKMA0400\STATS1", db="tempdb")
# # STATS1 server ia db
# stats_ia_db_engine = st.server_connection(server="CSKMA0400\STATS1", db="ia")
# # PA1 server tempdb
# pa1_tempdb_engine = st.server_connection(server="CSGPC-BPRD-SQ06\PA1", db="tempdb")
# # PA1 server dbBomCrs
# pa1_bomcrs_db_engine = st.server_connection(server="CSGPC-BPRD-SQ06\PA1", db="dbBomCrs")
# # PA1 server dbBommain
# pa1_bommain_db_engine = st.server_connection(
#     server="CSGPC-BPRD-SQ06\PA1", db="dbBommain"
# )


# BOMi IDs from PPS numbers
# Vital statistics from SCD database


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
