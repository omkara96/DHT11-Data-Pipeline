import dbconnect
import datetime

class DeltaDetectionProcessor:
    def __init__(self, source_schema, target_schema, source_table, target_table, natural_keys, device_id, cols_to_exclude_from_load, columns_to_exclude_from_delta, load_key):
        self.source_schema = source_schema
        self.target_schema = target_schema
        self.source_table = source_table
        self.target_table = target_table
        self.natural_keys = natural_keys
        self.device_id = device_id
        self.cols_to_exclude_from_load = cols_to_exclude_from_load
        self.columns_to_exclude_from_delta = columns_to_exclude_from_delta
        self.hist_temp = f"{source_table}_HIST_TEMP"
        self.cursor = dbconnect.get_cursor()
        self.connection = self.cursor.connection
        self.ak_col, self.key_col = self._get_key_ak_cols()
        self.delta_detection_query_string = ""
        self.load_key = load_key

    def _get_key_ak_cols(self):
        ak_key_sql = f"""
            SELECT COLUMN_NAME
            FROM all_tab_cols
            WHERE table_name = '{self.target_table}' 
              AND (COLUMN_NAME LIKE '%_AK' OR COLUMN_NAME LIKE '%_KEY')
              AND OWNER = '{self.target_schema}'
            ORDER BY COLUMN_NAME
        """
        self.cursor.execute(ak_key_sql)
        res = self.cursor.fetchall()
        if not res:
            raise ValueError("Error in getting Key and AK column. Please recheck target table definition and try again.")
        key_ak = sorted(column[0] for column in res)
        return key_ak[0], key_ak[1]

    def _get_cross_join_ak_key(self):
        return f"""
            CROSS JOIN (SELECT MAX({self.ak_col}) AS max_ak, MAX({self.key_col}) AS max_key FROM {self.target_schema}.{self.target_table}) max_val
        """

    def _get_delta_hash(self, columns):
        delta_hash_cols = [f"COALESCE(CAST({col} AS VARCHAR(1000)), '')" for col in columns]
        return " || ".join(delta_hash_cols)

    def _prepare_select_clause(self):
        natural_key_coalesce = [f"COALESCE(CAST(stg.{key} AS VARCHAR(10000)), CAST(tgt.{key} AS VARCHAR(10000))) AS {key}" for key in self.natural_keys]
        insert_condition = [f"tgt.{key} IS NULL" for key in self.natural_keys]
        update_condition = [f"tgt.{key} = stg.{key}" for key in self.natural_keys]
        
        select_clause = f"""
            SELECT tgt.{self.ak_col}, tgt.{self.key_col}, {', '.join(natural_key_coalesce)},
                   CASE 
                       WHEN ({' OR '.join(insert_condition)}) THEN 'I'
                       WHEN {' AND '.join(update_condition)} AND stg.delta_hash != tgt.delta_hash THEN 'U'
                       ELSE 'NC'
                   END AS upsert_cd
        """
        return select_clause

    def _get_prepare_tgt_data_clause(self, delta_hash):
        if not delta_hash:
            raise ValueError("Delta Hash Not generated properly.")
        natural_key_str = ", ".join(self.natural_keys)
        tgt_data_sql = f"""
            SELECT {self.key_col}, {self.ak_col}, {natural_key_str}, SHA256_HASH({delta_hash}) AS DELTA_HASH
            FROM {self.target_schema}.{self.target_table}
            WHERE DA_CURRENT_FLAG = 'Y'
        """
        return tgt_data_sql

    def _get_prepare_staging_clause(self, delta_hash, load_key):
        if not delta_hash:
            raise ValueError("Delta Hash Not generated properly.")
        natural_key_str = ", ".join(self.natural_keys)
        stg_sql = f"""
            SELECT {natural_key_str}, SHA256_HASH({delta_hash}) AS DELTA_HASH
            FROM {self.source_schema}.{self.source_table}
            WHERE LOAD_KEY = {load_key}
        """
        return stg_sql

    def _get_join_condition_stg_tgt(self):
        join_condition = [f"stg.{key} = tgt.{key}" for key in self.natural_keys]
        return " AND ".join(join_condition)

    def _prepare_delta_detection_query(self, select_clause, staging_clause, target_clause, cross_join_clause, join_condition_clause):
        delta_detection_query = f"""
            SELECT CAST(nvl(max_val.max_key,0) AS NUMERIC(18)) AS max_key,
                   CAST(nvl(max_val.max_ak,0) AS NUMERIC(18)) AS max_ak,
                   tmp.*
            FROM (
                   {select_clause}
                   FROM ({staging_clause}) stg
                   FULL OUTER JOIN ({target_clause}) tgt ON 
                   {join_condition_clause}
                 ) tmp
                 {cross_join_clause}
        """
        print("Final Delta Detection Query...........")
        print(delta_detection_query)
        return delta_detection_query

    def truncate_table(self, schema, table):
        sql = f"TRUNCATE TABLE {schema}.{table}"
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            return True
        except Exception as e:
            print(f"Error Truncating table: {e}")
            return False
    
    def drop_temp_table(self):
        print("Dropping Temp Table.....\n\n")
        sql = f"SELECT table_name FROM all_tables WHERE owner='{self.target_schema}' AND table_name = '{self.hist_temp}'"
        self.cursor.execute(sql)
        table_nm = self.cursor.fetchall()
        
        natural_key_str = ", ".join(self.natural_keys)
        ddl_drop = f"""
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE {self.target_schema}.{self.hist_temp}';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
            """
        try:
            self.cursor.execute(ddl_drop)
            self.connection.commit()
            print("Temp Table Dropped Successfully. Creating new temp table for this batch load.")
                
        except Exception as e:
            print(f"Error in Dropping table: {e}")
            exit(1)
            
    def create_temp_table(self):
        print("Creating Temp Table.....")
        ddl_create = f"""
                CREATE TABLE {self.target_schema}.{self.hist_temp} AS {self.delta_detection_query_string} 
            """
        
        #print(f"Create Table DDL: {ddl_create}")
        
        try:
            self.cursor.execute(ddl_create)
            self.connection.commit()
            print("Temp Table Created Successfully.")

        except Exception as e:
            print(f"Error in Creating Temp table: {e}")
            exit(1)
        

    def process_delta_detection(self):
        print("Delta Detection process is starting......")
        
        src_cols_sql = f"""
            SELECT COLUMN_NAME
            FROM all_tab_cols
            WHERE table_name='{self.source_table}'
              AND COLUMN_NAME NOT IN {tuple(self.natural_keys + self.columns_to_exclude_from_delta + self.cols_to_exclude_from_load)}
              AND OWNER = '{self.source_schema}'
        """
        print(src_cols_sql)

        self.cursor.execute(src_cols_sql)
        res_src_col_sql = self.cursor.fetchall()
        if res_src_col_sql:
            delta_detection_columns = [column[0] for column in res_src_col_sql]
            print(f"Delta Detection Columns: {delta_detection_columns}")
            delta_hash_col_str = self._get_delta_hash(delta_detection_columns)
            staging_clause = self._get_prepare_staging_clause(delta_hash_col_str, self.load_key)
            target_clause = self._get_prepare_tgt_data_clause(delta_hash_col_str)
            select_clause = self._prepare_select_clause()
            join_condition_clause = self._get_join_condition_stg_tgt()
            cross_join_ak_key = self._get_cross_join_ak_key()
            self.delta_detection_query_string = self._prepare_delta_detection_query(select_clause, staging_clause, target_clause, cross_join_ak_key, join_condition_clause)
        else:
            print("Error! Unable to get delta detection columns.")
            exit(1)

    
    def get_prepare_update_clause(self):
        print("Preparing Update statement for Updating Flags and SysGen Columns.....")
        update_sql = f"""
                    MERGE INTO {self.target_schema}.{self.target_table} a
                    USING {self.target_schema}.{self.hist_temp} tmp
                    ON (a.{self.key_col} = tmp.{self.key_col})
                    WHEN MATCHED THEN
                    UPDATE SET
                        a.da_current_flag = 'N',
                        a.da_valid_to_date = SYSTIMESTAMP,
                        a.da_updated_datetime = SYSTIMESTAMP,
                        a.da_deleted_flag = CASE 
                                            WHEN tmp.upsert_cd = 'PD' THEN 'Y' 
                                            ELSE 'N' 
                                            END
                    WHERE tmp.upsert_cd IN ('U', 'PD')
            """
        print(update_sql)
        try:
            self.cursor.execute(update_sql)
            self.connection.commit()
            print("Records Merge/Updated Successfully...")

        except Exception as e:
            print(f"Error in Records Merge/Updated: {e}")
            exit(1)
        pass

    def get_prepare_insert_clause_and_perfrom_insert(self):
        print("Preparing Insert Statement for records................")
        tgt_table_cols_sql = f"""
                    SELECT COLUMN_NAME
                    FROM all_tab_cols
                    WHERE table_name = '{self.target_table}' 
                    AND OWNER = '{self.target_schema}'
                    ORDER BY COLUMN_NAME 
            """
        

        self.cursor.execute(tgt_table_cols_sql)
        res_col = self.cursor.fetchall()
        tgt_table_col = sorted(column[0] for column in res_col)
        tgt_table_col_str = ", ".join(tgt_table_col)
        print(tgt_table_col_str)

        src_cols_sql = f"""
            SELECT 'SRC.' || COLUMN_NAME
            FROM all_tab_cols
            WHERE table_name='{self.source_table}'
              AND COLUMN_NAME NOT IN {tuple(self.cols_to_exclude_from_load + self.cols_to_exclude_from_load)}
              AND OWNER = '{self.source_schema}'
        """
        print(src_cols_sql)
        self.cursor.execute(src_cols_sql)
        res_src_col = self.cursor.fetchall()
        src_table_col = sorted(column[0] for column in res_src_col)
        src_table_col_str = ", ".join(src_table_col)
        print("Sorcce TBL COls...\n")
        print(src_table_col_str)

        natural_col_list = ", ".join(self.natural_keys)
        print("\n NAtural Keys :" , natural_col_list)

        ak_key_col_insert = f"""
                            SELECT
                            ''
                        || ( coalesce(
                            CASE
                                WHEN SUBSTR(CAST(ext.{self.ak_col} AS VARCHAR(18)), 1, 4) <> 999999 THEN
                                    ext.{self.ak_col}
                                ELSE
                                    CAST(SUBSTR(CAST(ext.{self.ak_col} AS VARCHAR(18)), 5) AS NUMERIC(18))
                            END,
                            CASE
                                WHEN SUBSTR(CAST(ext.max_ak AS VARCHAR(18)), 1, 4) <> 999999 THEN
                                    ext.max_ak
                                ELSE
                                    CAST(SUBSTR(CAST(ext.max_ak AS VARCHAR(18)), 5) AS NUMERIC(18))
                            END
                            + ROW_NUMBER()
                            OVER(
                            ORDER BY
                                'JP'
                            )) )                                     AS {self.ak_col},
                        ''
                        || (
                            CASE
                                WHEN SUBSTR(CAST(ext.max_key AS VARCHAR(18)), 1, 4) <> 999999 THEN
                                    ext.max_key
                                ELSE
                                    CAST(SUBSTR(CAST(ext.max_key AS VARCHAR(18)), 5) AS NUMERIC(18))
                            END
                            + ROW_NUMBER()
                            OVER(
                            ORDER BY
                                'OMKAR'
                            ) )                                      AS {self.key_col}
                            
            """

        da_cols_str = f"""

                SYSDATE                                  AS da_updated_datetime,
                SYSDATE                                  AS da_inserted_datetime,
                TO_DATE('3000-01-01' , 'YYYY-MM-DD')     AS da_valid_to_date,
                SYSDATE                                  AS da_valid_from_date,
                'N'                                      AS da_deleted_flag,
                'Y'                                      AS da_current_flag    

            """
        join_condition = [f"SRC.{key} = EXT.{key}" for key in self.natural_keys]
        join_condition_str = " AND ".join(join_condition)
        from_join_condition_str = f""" FROM {self.source_schema}.{self.source_table} SRC JOIN {self.target_schema}.{self.hist_temp} EXT on {join_condition_str} WHERE 1=1
        AND ext.upsert_cd IN ( 'I', 'U' ) and SRC.LOAD_KEY={self.load_key} """

        print(from_join_condition_str)

        final_statement_str = f""" {ak_key_col_insert}, {src_table_col_str}, {da_cols_str}  {from_join_condition_str}"""
        print("Final Statement......................................................................")
        print(final_statement_str)

        self.cursor.execute(final_statement_str)
        final_selected_records = self.cursor.fetchone()

        # Get the column names from the cursor description
        column_list_from_cursor = sorted([desc[0] for desc in self.cursor.description])
        print(column_list_from_cursor)

        if sorted(column_list_from_cursor) == sorted(tgt_table_col):
            print("Columns Retured by select query and columns in target table are same.... \nPreparing Insert Query for data insertion.............")
            insert_statement_str = f""" INSERT INTO {self.target_schema}.{self.target_table} ({tgt_table_col_str}) SELECT {tgt_table_col_str} FROM ({final_statement_str}) """
            print(insert_statement_str)
            try:
                self.cursor.execute(insert_statement_str)
                insert_count = self.cursor.rowcount
                self.connection.commit()
                print(f"{insert_count} Records Inseted Successfully Successfully...")

            except Exception as e:
                print(f"Error in Records Merge/Updated: {e}")
                exit(1)
        else:
            print("Columns Retured by select query and columns in target table are not same... \nCheck columns or excluded columns correctly... \nAborting Load...")
            exit(1)



# Example usage
processor = DeltaDetectionProcessor(
    source_schema="ESP_SCHEMA",
    target_schema="ESP_SCHEMA",
    source_table="DHT11_DATA_INT",
    target_table="HIST_DHT11_DATA",
    natural_keys=['DEVICEID', 'TIMESTAMP'],
    device_id="DEV01OMKARVARMA",
    cols_to_exclude_from_load=['LOAD_KEY'],
    columns_to_exclude_from_delta=['TIMEZONE'],
    load_key = 0
)

processor.process_delta_detection()
processor.drop_temp_table()
processor.create_temp_table()
processor.get_prepare_update_clause()
processor.get_prepare_insert_clause_and_perfrom_insert()
