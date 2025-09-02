#!/usr/bin/env python3
"""
Oracle to Snowflake Data Migration Tool

This script safely moves data from Oracle database tables to Snowflake.
It includes extensive error checking and user-friendly progress updates.

Author: Data Migration Team
Version: 2.0 - Beginner Friendly
"""

import os
import sys
import logging
import argparse
import pandas as pd
import oracledb
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from typing import Optional, Tuple
import time
from datetime import datetime

# Load our configuration from the .env file
load_dotenv()

class DatabaseConnector:
    """
    Handles all database connections for Oracle and Snowflake
    
    This class makes it easy to connect to both databases and handles
    any connection errors in a user-friendly way.
    """
    
    def __init__(self):
        self.oracle_conn = None
        self.snowflake_conn = None
        self.logger = logging.getLogger(__name__)
    
    def connect_oracle(self) -> bool:
        """
        Connect to Oracle database using information from .env file
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            print("🔌 Connecting to Oracle database...")
            
            # Get connection details from environment variables
            user = os.getenv('ORACLE_USER')
            password = os.getenv('ORACLE_PASSWORD')
            host = os.getenv('ORACLE_HOST')
            port = os.getenv('ORACLE_PORT', '1521')
            service_name = os.getenv('ORACLE_SERVICE_NAME')
            
            # Validate we have all required information
            if not all([user, password, host, service_name]):
                missing = []
                if not user: missing.append('ORACLE_USER')
                if not password: missing.append('ORACLE_PASSWORD')
                if not host: missing.append('ORACLE_HOST')
                if not service_name: missing.append('ORACLE_SERVICE_NAME')
                
                print(f"❌ Missing Oracle connection information: {', '.join(missing)}")
                print("   Please check your .env file and add the missing values.")
                return False
            
            # Build connection string
            dsn = f"{host}:{port}/{service_name}"
            print(f"   Connecting to: {host}:{port}/{service_name}")
            print(f"   As user: {user}")
            
            # Attempt connection
            self.oracle_conn = oracledb.connect(
                user=user,
                password=password,
                dsn=dsn
            )
            
            # Test the connection with a simple query
            cursor = self.oracle_conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.fetchone()
            cursor.close()
            
            print("✅ Oracle connection established successfully!")
            self.logger.info("Successfully connected to Oracle database")
            return True
            
        except oracledb.DatabaseError as e:
            print(f"❌ Oracle connection failed: {e}")
            print("\n🔧 Troubleshooting tips:")
            print("   - Verify Oracle server is running")
            print("   - Check username and password")
            print("   - Confirm host address and port number")
            print("   - Verify service name is correct")
            self.logger.error(f"Oracle connection failed: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error connecting to Oracle: {e}")
            self.logger.error(f"Unexpected Oracle connection error: {e}")
            return False
    
    def connect_snowflake(self) -> bool:
        """
        Connect to Snowflake using information from .env file
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            print("🔌 Connecting to Snowflake...")
            
            # Get connection details from environment variables
            user = os.getenv('SNOWFLAKE_USER')
            password = os.getenv('SNOWFLAKE_PASSWORD')
            account = os.getenv('SNOWFLAKE_ACCOUNT')
            warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
            database = os.getenv('SNOWFLAKE_DATABASE')
            schema = os.getenv('SNOWFLAKE_SCHEMA')
            
            # Validate we have all required information
            required_fields = {
                'SNOWFLAKE_USER': user,
                'SNOWFLAKE_PASSWORD': password,
                'SNOWFLAKE_ACCOUNT': account,
                'SNOWFLAKE_WAREHOUSE': warehouse,
                'SNOWFLAKE_DATABASE': database,
                'SNOWFLAKE_SCHEMA': schema
            }
            
            missing = [field for field, value in required_fields.items() if not value]
            if missing:
                print(f"❌ Missing Snowflake connection information: {', '.join(missing)}")
                print("   Please check your .env file and add the missing values.")
                return False
            
            print(f"   Account: {account}")
            print(f"   User: {user}")
            print(f"   Warehouse: {warehouse}")
            print(f"   Database: {database}")
            print(f"   Schema: {schema}")
            
            # Attempt connection
            self.snowflake_conn = snowflake.connector.connect(
                user=user,
                password=password,
                account=account,
                warehouse=warehouse,
                database=database,
                schema=schema
            )
            
            # Test the connection
            cursor = self.snowflake_conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            
            print("✅ Snowflake connection established successfully!")
            self.logger.info("Successfully connected to Snowflake")
            return True
            
        except snowflake.connector.errors.DatabaseError as e:
            print(f"❌ Snowflake connection failed: {e}")
            print("\n🔧 Troubleshooting tips:")
            print("   - Verify account identifier is correct (include region)")
            print("   - Check username and password")
            print("   - Confirm warehouse exists and is not suspended")
            print("   - Verify database and schema exist")
            self.logger.error(f"Snowflake connection failed: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error connecting to Snowflake: {e}")
            self.logger.error(f"Unexpected Snowflake connection error: {e}")
            return False
    
    def close_connections(self):
        """Close all database connections"""
        if self.oracle_conn:
            try:
                self.oracle_conn.close()
                print("🔌 Oracle connection closed")
                self.logger.info("Oracle connection closed")
            except Exception as e:
                self.logger.warning(f"Error closing Oracle connection: {e}")
        
        if self.snowflake_conn:
            try:
                self.snowflake_conn.close()
                print("🔌 Snowflake connection closed")
                self.logger.info("Snowflake connection closed")
            except Exception as e:
                self.logger.warning(f"Error closing Snowflake connection: {e}")

class DataMigrator:
    """
    Main class that handles the data migration process
    
    This class orchestrates the entire migration: reading from Oracle,
    processing the data, and writing to Snowflake.
    """
    
    def __init__(self):
        self.db_connector = DatabaseConnector()
        self.logger = logging.getLogger(__name__)
        self.batch_size = int(os.getenv('BATCH_SIZE', '10000'))
        
        # Setup logging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'migration_{timestamp}.log'),
                logging.StreamHandler()
            ]
        )
        print(f"📝 Detailed logs saved to: migration_{timestamp}.log")
        print(f"⚙️  Migration configured with batch size: {self.batch_size:,} rows")
    
    def get_table_info(self, table_name: str) -> dict:
        """
        Get information about an Oracle table before migration
        
        Args:
            table_name: Name of the Oracle table
            
        Returns:
            dict: Table information including row count and column details
        """
        try:
            if not self.db_connector.oracle_conn:
                raise ValueError("Oracle connection not established")
            
            cursor = self.db_connector.oracle_conn.cursor()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            # Get column information
            cursor.execute(f"""
                SELECT column_name, data_type, data_length, nullable
                FROM user_tab_columns 
                WHERE table_name = UPPER('{table_name}')
                ORDER BY column_id
            """)
            columns = cursor.fetchall()
            
            cursor.close()
            
            return {
                'row_count': row_count,
                'columns': columns
            }
            
        except Exception as e:
            self.logger.warning(f"Could not get table info for {table_name}: {e}")
            return {'row_count': 'Unknown', 'columns': []}
    
    def extract_data_from_oracle(self, table_name: str, where_clause: str = None) -> pd.DataFrame:
        """
        Extract data from Oracle table and load into a pandas DataFrame
        
        Args:
            table_name: Name of the Oracle table to read from
            where_clause: Optional WHERE clause to filter data
            
        Returns:
            pandas.DataFrame: The extracted data
        """
        try:
            if not self.db_connector.oracle_conn:
                raise ValueError("Oracle connection not established")
            
            # Build the SQL query
            base_query = f"SELECT * FROM {table_name}"
            if where_clause:
                query = f"{base_query} WHERE {where_clause}"
                print(f"📊 Extracting data with filter: {where_clause}")
            else:
                query = base_query
                print(f"📊 Extracting all data from table: {table_name}")
            
            print(f"   SQL Query: {query}")
            
            # Get table info first
            table_info = self.get_table_info(table_name)
            if table_info['row_count'] != 'Unknown':
                print(f"   Expected rows to process: {table_info['row_count']:,}")
            
            # Execute query and load data
            print("   Reading data from Oracle...")
            start_time = time.time()
            
            df = pd.read_sql(query, self.db_connector.oracle_conn)
            
            end_time = time.time()
            duration = end_time - start_time
            
            print(f"✅ Successfully extracted {len(df):,} rows from Oracle")
            print(f"   Time taken: {duration:.2f} seconds")
            print(f"   Data size: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
            
            # Show column information
            if len(df.columns) <= 10:
                print(f"   Columns: {', '.join(df.columns)}")
            else:
                print(f"   Columns: {len(df.columns)} total ({', '.join(df.columns[:5])}, ...)")
            
            self.logger.info(f"Successfully extracted {len(df)} rows from Oracle table: {table_name}")
            return df
            
        except Exception as e:
            print(f"❌ Failed to extract data from Oracle: {e}")
            self.logger.error(f"Failed to extract data from Oracle: {e}")
            raise
    
    def load_data_to_snowflake(self, df: pd.DataFrame, table_name: str, 
                              if_exists: str = 'append') -> Tuple[bool, int]:
        """
        Load data from pandas DataFrame into Snowflake table
        
        Args:
            df: pandas DataFrame containing the data
            table_name: Name of the Snowflake table to write to
            if_exists: What to do if table exists ('append' or 'replace')
            
        Returns:
            Tuple[bool, int]: Success status and number of rows loaded
        """
        try:
            if not self.db_connector.snowflake_conn:
                raise ValueError("Snowflake connection not established")
            
            if df.empty:
                print("⚠️  DataFrame is empty - no data to load")
                return True, 0
            
            print(f"📤 Loading {len(df):,} rows to Snowflake table: {table_name}")
            print(f"   Mode: {if_exists} (will {'replace' if if_exists == 'replace' else 'add to'} existing data)")
            
            start_time = time.time()
            
            # Use Snowflake's optimized pandas loading function
            success, nchunks, nrows, _ = write_pandas(
                conn=self.db_connector.snowflake_conn,
                df=df,
                table_name=table_name.upper(),  # Snowflake prefers uppercase table names
                schema=os.getenv('SNOWFLAKE_SCHEMA'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                auto_create_table=True,  # Create table if it doesn't exist
                overwrite=(if_exists == 'replace'),
                quote_identifiers=False  # Don't quote column names
            )
            
            end_time = time.time()
            duration = end_time - start_time
            
            if success:
                print(f"✅ Successfully loaded {nrows:,} rows into Snowflake")
                print(f"   Time taken: {duration:.2f} seconds")
                print(f"   Processing rate: {nrows/duration:.0f} rows/second")
                print(f"   Data was processed in {nchunks} chunks")
                self.logger.info(f"Successfully loaded {nrows} rows into Snowflake table: {table_name}")
                return True, nrows
            else:
                print("❌ Failed to load data into Snowflake")
                self.logger.error("Failed to load data into Snowflake")
                return False, 0
                
        except Exception as e:
            print(f"❌ Failed to load data to Snowflake: {e}")
            self.logger.error(f"Failed to load data to Snowflake: {e}")
            raise
    
    def migrate_table(self, oracle_table: str, snowflake_table: str, 
                     where_clause: str = None, if_exists: str = 'append') -> bool:
        """
        Migrate a complete table from Oracle to Snowflake
        
        This is the main migration function that coordinates the entire process.
        
        Args:
            oracle_table: Source table name in Oracle
            snowflake_table: Target table name in Snowflake
            where_clause: Optional filter for data extraction
            if_exists: What to do if target table exists
            
        Returns:
            bool: True if migration successful, False otherwise
        """
        migration_start_time = time.time()
        
        try:
            print("🚀 Starting Data Migration")
            print("=" * 50)
            print(f"Source: Oracle table '{oracle_table}'")
            print(f"Target: Snowflake table '{snowflake_table}'")
            if where_clause:
                print(f"Filter: {where_clause}")
            print(f"Mode: {if_exists}")
            print()
            
            # Step 1: Connect to Oracle
            print("Step 1: Connecting to Oracle...")
            if not self.db_connector.connect_oracle():
                return False
            print()
            
            # Step 2: Connect to Snowflake
            print("Step 2: Connecting to Snowflake...")
            if not self.db_connector.connect_snowflake():
                return False
            print()
            
            # Step 3: Extract data from Oracle
            print("Step 3: Extracting data from Oracle...")
            df = self.extract_data_from_oracle(oracle_table, where_clause)
            print()
            
            # Step 4: Load data to Snowflake
            print("Step 4: Loading data to Snowflake...")
            
            # Handle large datasets by processing in batches
            if len(df) > self.batch_size:
                print(f"📦 Large dataset detected. Processing in batches of {self.batch_size:,} rows...")
                total_rows_loaded = 0
                num_batches = (len(df) + self.batch_size - 1) // self.batch_size  # Ceiling division
                
                for batch_num in range(num_batches):
                    start_idx = batch_num * self.batch_size
                    end_idx = min((batch_num + 1) * self.batch_size, len(df))
                    batch_df = df.iloc[start_idx:end_idx]
                    
                    print(f"   Processing batch {batch_num + 1}/{num_batches} "
                          f"(rows {start_idx + 1:,} to {end_idx:,})...")
                    
                    # For the first batch, use the specified if_exists mode
                    # For subsequent batches, always append
                    batch_if_exists = if_exists if batch_num == 0 else 'append'
                    
                    success, batch_rows = self.load_data_to_snowflake(
                        batch_df, snowflake_table, batch_if_exists
                    )
                    
                    if not success:
                        print(f"❌ Failed on batch {batch_num + 1}")
                        return False
                    
                    total_rows_loaded += batch_rows
                    progress = (batch_num + 1) / num_batches * 100
                    print(f"   ✅ Batch {batch_num + 1} complete. Progress: {progress:.1f}%")
                
                print(f"✅ All batches processed successfully!")
                total_rows = total_rows_loaded
            else:
                # Process all data at once for smaller datasets
                success, total_rows = self.load_data_to_snowflake(df, snowflake_table, if_exists)
                if not success:
                    return False
            
            # Calculate and display final statistics
            migration_end_time = time.time()
            total_duration = migration_end_time - migration_start_time
            
            print()
            print("🎉 Migration Completed Successfully!")
            print("=" * 40)
            print(f"📊 Migration Statistics:")
            print(f"   Source table: {oracle_table}")
            print(f"   Target table: {snowflake_table}")
            print(f"   Total rows migrated: {total_rows:,}")
            print(f"   Total time: {total_duration:.2f} seconds")
            print(f"   Average rate: {total_rows/total_duration:.0f} rows/second")
            print(f"   Data size processed: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
            
            self.logger.info(f"Migration completed successfully. {total_rows} rows migrated in {total_duration:.2f} seconds")
            return True
            
        except Exception as e:
            print(f"\n❌ Migration failed with error: {e}")
            print("\n🔧 What to do next:")
            print("   1. Check the detailed log file for more information")
            print("   2. Verify your database connections are still active")
            print("   3. Check if you have sufficient permissions")
            print("   4. For large tables, try adding a WHERE clause to process smaller chunks")
            self.logger.error(f"Migration failed: {e}")
            return False
        
        finally:
            # Always clean up connections
            print("\n🧹 Cleaning up connections...")
            self.db_connector.close_connections()

def validate_environment() -> bool:
    """
    Check that all required environment variables are set
    
    Returns:
        bool: True if all variables are present, False otherwise
    """
    required_vars = [
        'ORACLE_USER', 'ORACLE_PASSWORD', 'ORACLE_HOST', 'ORACLE_SERVICE_NAME',
        'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Configuration Error: Missing required environment variables")
        print(f"   Missing variables: {', '.join(missing_vars)}")
        print("\n🔧 How to fix this:")
        print("   1. Check that your .env file exists in the current directory")
        print("   2. Make sure all required variables are defined in the .env file")
        print("   3. Verify there are no typos in variable names")
        print("   4. Ensure the .env file uses the format: VARIABLE_NAME=value")
        return False
    
    print("✅ All required configuration variables found")
    return True

def main():
    """
    Main function that handles command-line arguments and runs the migration
    """
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(
        description='Migrate data from Oracle database to Snowflake',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --oracle-table EMPLOYEES
  %(prog)s --oracle-table ORDERS --where-clause "ORDER_DATE >= DATE '2024-01-01'"
  %(prog)s --oracle-table PRODUCTS --snowflake-table PRODUCT_CATALOG --if-exists replace
        """
    )
    
    parser.add_argument('--oracle-table', 
                       required=True, 
                       help='Name of the Oracle table to migrate')
    
    parser.add_argument('--snowflake-table', 
                       help='Name of the Snowflake table to create (defaults to Oracle table name)')
    
    parser.add_argument('--where-clause', 
                       help='Optional WHERE clause to filter Oracle data (e.g., "STATUS = \'ACTIVE\'")')
    
    parser.add_argument('--if-exists', 
                       choices=['append', 'replace'], 
                       default='append',
                       help='What to do if Snowflake table exists: append new data or replace entirely')
    
    parser.add_argument('--log-level', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO',
                       help='Set the logging level for detailed output')
    
    # Parse the arguments
    args = parser.parse_args()
    
    print("🔧 Oracle to Snowflake Migration Tool")
    print("=" * 50)
    print()
    
    # Validate environment configuration
    if not validate_environment():
        print("\n❌ Please fix the configuration issues above before running the migration.")
        sys.exit(1)
    
    # Use Oracle table name for Snowflake if not specified
    snowflake_table = args.snowflake_table or args.oracle_table
    
    # Display migration plan
    print("📋 Migration Plan:")
    print(f"   Oracle source table: {args.oracle_table}")
    print(f"   Snowflake target table: {snowflake_table}")
    if args.where_clause:
        print(f"   Data filter: {args.where_clause}")
    print(f"   If table exists: {args.if_exists}")
    print(f"   Logging level: {args.log_level}")
    print()
    
    # Ask for user confirmation for replace operations
    if args.if_exists == 'replace':
        response = input(f"⚠️  This will REPLACE all data in {snowflake_table}. Continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Migration cancelled by user.")
            sys.exit(0)
        print()
    
    # Create migrator and run the migration
    try:
        migrator = DataMigrator()
        
        success = migrator.migrate_table(
            oracle_table=args.oracle_table,
            snowflake_table=snowflake_table,
            where_clause=args.where_clause,
            if_exists=args.if_exists
        )
        
        if success:
            print("\n🎉 Migration completed successfully!")
            print("   You can now find your data in Snowflake.")
            print(f"   Table name: {snowflake_table}")
            sys.exit(0)
        else:
            print("\n❌ Migration failed!")
            print("   Please check the error messages above and the log file for details.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Migration interrupted by user (Ctrl+C)")
        print("   The migration was stopped. Some data may have been partially transferred.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
