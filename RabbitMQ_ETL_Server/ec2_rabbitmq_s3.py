import time
import pika
import boto3
import ssl
import gzip
import os
from datetime import datetime
import json
import logging
from dotenv import load_dotenv
from threading import Thread, Lock
from queue import Queue
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


dotenv_path = os.path.join("/home/ubuntu/s3_upload", '.env')

lock = Lock()
load_dotenv(dotenv_path)
messages_dict_queue = Queue()
logfilepath = "/home/ubuntu/s3_upload/newfile.log"
# Create and configure logger
logging.basicConfig(filename=logfilepath,
                    format='%(asctime)s %(message)s',
                    filemode='a')

# Creating an object
log = logging.getLogger()
# Setting the threshold of logger to DEBUG
log.setLevel(logging.WARNING) 


#S3
s3_bucket = os.environ["S3_BUCKET"]
s3_prefix = os.environ["S3_PREFIX"]

# AWS_Account
AWS_ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.environ["AWS_REGION"]
EC2_INSTANCE_ID = os.environ["EC2_INSTANCE_ID"]

# RabbitMQ
rabbitmq_host = os.environ["RABBITMQ_HOST"]
rabbitmq_port = int(os.environ["RABBITMQ_PORT"])
rabbitmq_vhost = os.environ["RABBITMQ_VHOST"]
rabbitmq_user = os.environ["RABBITMQ_USER"]
rabbitmq_pass = os.environ["RABBITMQ_PASS"]
rabbitmq_queue = os.environ["RABBITMQ_QUEUE"]

#EMAIL SETTINGS
EMAIL_FROM = os.environ["EMAIL_FROM"]
EMAIL_FROM_PASSWORD = os.environ["EMAIL_FROM_PASSWORD"]
EMAIL_SERVER = os.environ["EMAIL_SERVER"]
EMAIL_SERVER_PORT = os.environ["EMAIL_SERVER_PORT"]
EMAIL_TO = os.environ["EMAIL_TO"]


# pika
credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
conn_params = pika.ConnectionParameters(
    rabbitmq_host, rabbitmq_port, rabbitmq_vhost, credentials,
    ssl_options=pika.SSLOptions(ssl.SSLContext()))

def ec2_reboot():
    session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY,aws_secret_access_key=AWS_SECRET_KEY)
    ec2 = session.resource('ec2', AWS_REGION)
    ec2.Instance(EC2_INSTANCE_ID).reboot()

# Function for sending the email
def send_email():

    try:
        password = EMAIL_FROM_PASSWORD
        email_from = EMAIL_FROM
        server = EMAIL_SERVER
        port = int(EMAIL_SERVER_PORT)
        email_to = EMAIL_TO

        server = smtplib.SMTP(server, port)
        server.starttls()
        server.login(email_from, password)
        
        mail_content = "Rabbitmq ETL server rebooted at " + str(datetime.now().strftime("%d.%m.%Y %H:%M.%S"))
        msg = MIMEMultipart()
        msg['From'] = email_from
        msg['To'] = email_to
        msg["Cc"] = ""
        msg['Subject'] = "Rabbitmq ETL server reboot"
        msg.attach(MIMEText(mail_content, 'plain'))
        text = msg.as_string()
        server.sendmail(
                email_from, email_to, text)
        server.quit()
        log.warning(f">-email sent to {email_to}")
    except Exception as e:
        log.warning(">-email send exception:" + str(e))


def unix_timestamp_us():
    return int(time.time_ns() / 1000)


def generate_filename(VbuNo: str, name_suffix: str):
    return VbuNo + "/locs/locs_"+ VbuNo + name_suffix

def split_dict_equally(input_dict, chunks=5):
    "Splits dict by keys. Returns a list of dictionaries."
    # prep with empty dicts
    return_list = [dict() for idx in range(chunks)]
    idx = 0
    for k,v in input_dict.items():
        return_list[idx][k] = v
        if idx < chunks-1:  # indexes start at 0
            idx += 1
        else:
            idx = 0
    return return_list

def s3_upload_thread(copy_messages_dict: dict, date_time_info: str):
    try:
        name_suffix = "_" + date_time_info + ".json"
        session = boto3.Session(
                aws_access_key_id=AWS_ACCESS_KEY,
                    aws_secret_access_key=AWS_SECRET_KEY
                    )
        s3 = session.resource('s3')
        bucket = s3.Bucket(s3_bucket)
        for VbuNo, jsonObj in copy_messages_dict.items():
            bucket.put_object(Body=jsonObj, ContentType="application/json", Key=generate_filename(VbuNo, name_suffix))
    except Exception as e:
        lmsg = "Exception in s3 upload thread " + str(e)
        with lock:
            print (lmsg)
            log.warning(lmsg)

    
def s3_upload():
    keep_running = True
    num_threads = 15
    with lock:
            lmsg = "Number of threads configured " + str(num_threads)
            print (lmsg)
            log.warning(lmsg)
    while keep_running:
        copy_messages_dict = messages_dict_queue.get()
        upload_start_time = datetime.now()
        # date_time_info = upload_start_time.strftime("%Y_%m_%d_%H_%M_%S")
        date_time_info = upload_start_time.strftime("%Y_%m_%d")
        with lock:
            lmsg = "Creating and starting threads for upload"
            print (lmsg)
            log.warning(lmsg)
        msgs_list = split_dict_equally(copy_messages_dict,num_threads)
        threads = []
        for i in range(0,num_threads):
            t = Thread(target=s3_upload_thread, args=(msgs_list[i], date_time_info,))
            threads.append(t)
        # Start all threads
        for t in threads:
            t.start()
        # Wait for all of them to finish
        for t in threads:
            t.join()
        diff_time = datetime.now() - upload_start_time
        lmsg = "Total upload time " + str(diff_time.seconds) + " seconds"
        with lock:
            print (lmsg)
            log.warning(lmsg)
    with lock:
        lmsg = "s3 upload thread exited"
        print (lmsg)
        log.warning(lmsg)

    
def rabbitmq_handler():
    check_interval_seconds = 60
    all_messages_dict = {}
    lmsg = "Starting message handler..."
    with lock:
        print (lmsg)
        log.warning(lmsg)
    start_time = datetime.now()
    pre_time = datetime.now()
    channel = None
    try:
        connection = pika.BlockingConnection(conn_params)
        lmsg = "Connection established"
        with lock:
            print (lmsg)
            log.warning(lmsg)
        channel = connection.channel()
        lmsg = "Channel established"
        with lock:
            print (lmsg)
            log.warning(lmsg)
        for method, properties, body in channel.consume(queue=rabbitmq_queue, auto_ack=False, inactivity_timeout=3600):
            if method != None:
                jsonObj = gzip.decompress(body)
                json_obj = json.loads(jsonObj)
                if "VbuNo" in json_obj:
                    VbuNo = str(json_obj["VbuNo"])
                    all_messages_dict[VbuNo] = jsonObj
                else:
                    lmsg = "VbuNo is missing"
                    with lock:
                        print (lmsg)
                        log.warning(lmsg)
                channel.basic_ack(method.delivery_tag)
                msgs = method.delivery_tag
                now_time = datetime.now()
                delta_time = now_time - start_time
                delta_seconds = delta_time.seconds
                diff_seconds = now_time - pre_time
                diff_seconds = diff_seconds.seconds
                #one message per minute per car
                if diff_seconds >= check_interval_seconds:
                    copy_messages_dict = all_messages_dict
                    all_messages_dict = {}
                    with lock:
                        lmsg = "Number of items in queue " + str(messages_dict_queue.qsize())
                        print (lmsg)
                        log.warning(lmsg)
                        lmsg = "Number of messages in new dict " + str(len(copy_messages_dict))
                        print (lmsg)
                        log.warning(lmsg)
                        lmsg = "messages dict put in queue..."
                        print (lmsg)
                        log.warning(lmsg)
                    messages_dict_queue.put(copy_messages_dict)
                    pre_time = now_time
            else:
                lmsg = "No messages to process"
                print (lmsg)
                log.warning(lmsg)
                break
        print("messages processed: %r" % msgs)
        channel.cancel()
    except Exception as ex:

        try:
            channel.cancel()
        except Exception as e:
            pass

        with lock:
            lmsg = "Some exceptions occured: " + str(ex)
            print (lmsg)
            log.warning(lmsg)
            try:
                send_email()
            except Exception as e:
                log.warning("Exceptioin in email send.")
            reboot_msg = "ETL server is rebooting due to rabbit MQ channel drop"
            print (reboot_msg)
            log.warning(reboot_msg)
            time.sleep(30)
        ec2_reboot()
        # os.system('sudo reboot')


def main():
    lmsg = "Starting rabbitmq hanlder script."
    print (lmsg)
    log.warning(lmsg)
    keep_running = True

    # S3 Upload thread
    upload_thread = Thread(target=s3_upload)
    upload_thread.start()
    while keep_running:
        try:
            rabbitmq_handler()
            time.sleep(30)
        except Exception as e:
            lmsg = "Exception in main function: " + str(e)
            reboot_msg = "ETL server is rebooting now"
            with lock:
                print (lmsg)
                log.warning(lmsg)
                print (reboot_msg)
                try:
                    send_email()
                except Exception as e:
                    log.warning("Exceptioin in email send.")
                log.warning(reboot_msg)
                time.sleep(30)
                ec2_reboot()
                # os.system('sudo reboot')
            time.sleep(10)
    upload_thread.join()
    lmsg = "Stoping rabbitmq hanlder script."
    with lock:
        print (lmsg)
        log.warning(lmsg)


if __name__ == "__main__":
    main()
