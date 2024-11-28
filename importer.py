import json
from datetime import datetime, timedelta
import time
import re
import os
import pyspark.sql.functions as f
from pyspark.sql.functions import col, when, concat, lit
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
import cx_Oracle
import pandas as pd
import numpy as np
import math
from builtins import min
import traceback
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

email = "duy.caov@homecredit.vn"

def init_spark():
    sparkProps = {
        "spark.master": "yarn",
        "spark.driver.cores": 2,  
        "spark.driver.memory": "6g", 
        "spark.executor.cores": 2, 
        "spark.executor.memory": "3g",  
        "spark.executor.instances": 6,  
        "spark.port.maxRetries": "100",  

        # Dynamic Allocation Settings
        "spark.dynamicAllocation.minExecutors": 2, 
        "spark.dynamicAllocation.initialExecutors": 2, 
        "spark.dynamicAllocation.maxExecutors": 8,  
    }
    conf = SparkConf().setAll(list(sparkProps.items()))
    spark = (
        SparkSession.builder
                    .appName("duysession9")
                    .config(conf=conf)
                    .enableHiveSupport()
                    .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_ora_conn():
    with open(os.path.join(os.getcwd(), 'config.json'), 'r') as f:
        config = json.load(f)


    email = config["email"]
    ora_conn = config["ora_conn"] 
    ora_user = config["ora_user"] 
    ora_password = config["ora_password"]
    host = config["host"] 
    port = config["port"] 
    service_name = config["service_name"] 

    spark = init_spark()

    # Oracle Session
    dsn_tns = cx_Oracle.makedsn(host, port, service_name= service_name)
    conn = cx_Oracle.connect(user=ora_user, password=ora_password, dsn=dsn_tns)
    cursor = conn.cursor()
    
    return conn

def get_start_end_time_tuples(start_date, end_date, split_duration):
    # Generate list of dates
    date_range = pd.date_range(start=start_date, end=end_date)

    # Convert to list
    date_list = [str(d).split(' ')[0] for d in date_range.tolist()]

    # loop
    date_tuples = []
    length_dates = len(date_list)
    interval = int(np.floor(length_dates/split_duration)) + 1
    start_i = 0
    end_i = 0
    for i in range(interval):
        end_i = min(start_i + split_duration - 1, length_dates-1)
        date_tuples.append((date_list[start_i], date_list[end_i]))
        start_i = end_i + 1
        if start_i == length_dates:
            return date_tuples
    
    return date_tuples

def reformat_date(d):
    return  re.sub('-', '', d) # 20240701

def get_diff_days(d, diff_days):
    date_in = datetime.strptime(d, "%Y-%m-%d")
    day_out = date_in + timedelta(days=diff_days)
    day_out = day_out.strftime("%Y-%m-%d")
    return day_out

def refresh_tables(spec_table):
    spark.catalog.refreshTable(spec_table)

def send_email(email, app_name, content, msg_type="info"):
    message = MIMEMultipart()
    message["from"] = "CRM BI<crmbot@homecredit.vn>"
    message["to"] = email
    if msg_type == "error":
        message["subject"] = f"[ERROR] {app_name}"
    else:
        message["subject"] = f"{app_name}"
    html = f"""
        <html>
            <body>
                <xmp>
                    {content}
                </xmp>
            </body>
        </html>
        """
    part = MIMEText(html, "html")
    message.attach(part)
    with smtplib.SMTP(host="smtp.homecredit.vn", port=25) as smtp:
        smtp.ehlo()
        #smtp.starttls()
        smtp.send_message(message)
        
def truncate_table(table, date_col, start_date, end_date,f_reformat_date=0):
    date_diff = datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")
    date_tuples = get_start_end_time_tuples(
        start_date=start_date, 
        end_date=end_date, 
        split_duration = math.ceil(date_diff.days/2)
    )
    
    tuple_index = 1
    for date_tuple in date_tuples:
        start_time = time.time()
        tmp_table_name = f"bdp_ap_crm.duy_tmp_{datetime.strftime(datetime.now(), '%H%M%S')}"
        start_date = date_tuple[0] if not f_reformat_date else reformat_date(date_tuple[0])
        end_date = date_tuple[1]if not f_reformat_date else reformat_date(date_tuple[1])
        sdf = spark.sql(f"""
            SELECT * 
            FROM {table}
            WHERE NOT (
                {date_col} >= '{start_date}' 
                and {date_col} < '{end_date}'
            )
        """) 
        spark.sql(f"DROP TABLE IF EXISTS {tmp_table_name}")
        sdf.write.mode("overwrite").saveAsTable(tmp_table_name)
        sdf = spark.sql(f"select * from {tmp_table_name}")
        sdf.write.mode("append").saveAsTable(table)
        spark.sql(f"DROP TABLE IF EXISTS {tmp_table_name}")
        end_time = time.time()
        # log
        print(f"==== TRUNCATED {tuple_index}/{len(date_tuples)} DATE TUPLES, TABLE {table} FROM {start_date} TO {end_date} IN {end_time-start_time}")

def set_https_proxy(proxy):
    # set proxy for reaching out to Bigquery
    os.environ["http_proxy"] = proxy
    os.environ["HTTP_PROXY"] = proxy
    os.environ["https_proxy"] = proxy
    os.environ["HTTPS_PROXY"] = proxy

def read_oracle(query, custom_settings=None):
    """
    Fetch data from an Oracle database using Spark's JDBC connector.

    Args:
        query (str): The SQL query to execute on the Oracle database.
        custom_settings (dict, optional): Custom settings for JDBC fetch and partitioning.
            - row_fetch (int): Number of rows to prefetch per fetch call (optional).
            - column (str): Column used for partitioning the data (optional).
            - low_bound (int): Lower bound of the partition column (required if 'column' is set).
            - upp_bound (int): Upper bound of the partition column (required if 'column' is set).
            - num_part (int): Number of partitions to split the data into (required if 'column' is set).

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame containing the query results.
    """
    # Load database configuration from config.json
    with open("config.json", "r") as f:
        config = json.load(f)
    
    ora_conn = config["ora_conn"]
    ora_user = config["ora_user"]
    ora_password = config["ora_password"]

    # Default to an empty dictionary if no settings are provided
    custom_settings = custom_settings or {}

    # Common JDBC properties
    jdbc_properties = {
        "user": ora_user,
        "password": ora_password,
        "driver": "oracle.jdbc.driver.OracleDriver",
    }

    # Add row fetch size if provided
    if "row_fetch" in custom_settings:
        jdbc_properties["oracle.jdbc.defaultRowPrefetch"] = str(custom_settings["row_fetch"])

    # Partitioning logic
    if "column" in custom_settings:
        required_keys = ["low_bound", "upp_bound", "num_part"]
        if not all(key in custom_settings for key in required_keys):
            raise ValueError("Missing keys in custom_settings for partitioning: 'low_bound', 'upp_bound', 'num_part' are required.")
        
        # Use partitioning parameters
        return spark.read.jdbc(
            url=ora_conn,
            table=f"({query}) TMP",
            properties=jdbc_properties,
            column=custom_settings["column"],
            lowerBound=custom_settings["low_bound"],
            upperBound=custom_settings["upp_bound"],
            numPartitions=custom_settings["num_part"],
        )
    
    # Non-partitioned read
    return spark.read.jdbc(
        url=ora_conn,
        table=f"({query}) TMP",
        properties=jdbc_properties,
    )

    
if __name__ == '__main__':
    ###### INIT
    spark = init_spark()
    conn = get_ora_conn()
    
    set_https_proxy('http://proxy-pdc.vn.prod:3128')
    
    
        
    
    

