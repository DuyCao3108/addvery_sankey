from subprocess import Popen, PIPE, STDOUT
import argparse
import logging
import sys
from datetime import datetime
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

PROCESS_NAME = "PLOTLY_ADDVERY"
LOG_STORE_FOLDER = "/home/duy.caov/work_space/log_store"
EMAIL = 'duy.caov@homecredit.vn'

def log_subprocess_output(pipe):
    for line in iter(pipe.readline, b''):  # b'\n'-separated lines
        # Decode the line to convert bytes to string
        decoded_line = line.decode('utf-8').rstrip()  # Remove trailing newline
        logging.info('%r', decoded_line)

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

def parse_arguments():
    parser = argparse.ArgumentParser(description="Run a Spark Submit process.")
    parser.add_argument('python_script', type=str, help="Python script to execute (e.g. 'importer.py').")
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command-line arguments
    args = parse_arguments()
    
    # to-run python script
    pyscript = args.python_script  if args.python_script is not None else 'importer.py'
    
    # Set up log folder
    LOG_STORE_TOSAVE_FOLDER = os.path.join(LOG_STORE_FOLDER, str(datetime.strftime(datetime.now(), r'%Y%m%d')))

    if not os.path.exists(LOG_STORE_TOSAVE_FOLDER):
        os.makedirs(LOG_STORE_TOSAVE_FOLDER)
        
    FileHandlerName = f"{PROCESS_NAME}-{datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S')}.log"
    FileHandlerPath = os.path.join(LOG_STORE_TOSAVE_FOLDER, FileHandlerName)
    
    # Set up logging to file and console
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[
                            logging.FileHandler(FileHandlerPath),  # Log to a file
                            logging.StreamHandler(sys.stdout)  # Log to console
                        ])
    logging.info(f"====== Saving logs at {FileHandlerPath} =======")
    # Start the subprocess
    
    send_email(
        EMAIL, 
        f"STARTING Submit - Process {PROCESS_NAME}", 
        f"[DuyJobMonitor] Run file: {pyscript}; Log file at {FileHandlerPath}", 
        msg_type="info"
    )
    
    process = Popen(
        [
            'spark2-submit',
            '--master', 'yarn',
            '--deploy-mode', 'client',
            '--jars', '/opt/cloudera/parcels/JAVA_LIBS/ojdbc8.jar',
            '--files', 'config.json',
            pyscript
        ],
        stdout=PIPE,
        stderr=STDOUT
    )

    # Log the output in real-time
    with process.stdout:
        log_subprocess_output(process.stdout)

    exitcode = process.wait()  # 0 means success

    # Log exit code
    if exitcode == 0:
        logging.info("============= SUCCEEDED! ==================")
        send_email(
            EMAIL, 
            f"SUCEEDED Submit - Process {PROCESS_NAME}", 
            f"[DuyJobMonitor] Run file: {pyscript}; Log file at {FileHandlerPath}", 
            msg_type="info"
        )
    else:
        logging.error(f"================ FAIL! ({exitcode}) ==============")
        send_email(
            EMAIL, 
            f"FAILED Submit - Process {PROCESS_NAME}", 
            f"[DuyJobMonitor] Run file: {pyscript}; Log file at {FileHandlerPath}", 
            msg_type="info"
        )