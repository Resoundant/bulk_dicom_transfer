# batch_xfer.py
# requires python3.9 or later
#
# option but recommended, create a virtual environment:
# python3 -m venv venv
# source venv/bin/activate (linux)
# venv\Scripts\activate (windows)
#
# install dependencies:
# pip install -r requirements.txt

import pydicom
from pynetdicom import AE, build_context, debug_logger
from pynetdicom.sop_class import MRImageStorage
import os
import sys
import datetime
import logging
from server_config import SERVER_IP, SERVER_PORT, CALLED_AET, CALLING_AET
TIMESTAMP = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_TIME =  datetime.datetime.now().strftime("%Y-%m-%d")

if (not SERVER_IP):
    print("Please set SERVER_IP in server_config.py")
    exit()
if (not SERVER_PORT):
    print("Please set SERVER_PORT in server_config.py")
    exit()
if (not CALLED_AET):
    print("Please set CALLED_AET in server_config.py")
    exit() 
if (not CALLING_AET):
    print("Please set CALLING_AET in server_config.py")
    exit()

logging.basicConfig(
    level=logging.INFO, # Minimum level of messages to log
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', # Format of log messages
    filename=f'xfer_{LOG_TIME}.log', # File to write log messages to
    filemode='a' # Mode to open the file in (w for write, a for append)
)
logger = logging.getLogger(__name__).addHandler(logging.StreamHandler(sys.stdout))

def savetofile_to_send(to_send:list):
    sendfile = "to_send.txt"
    if os.path.exists(sendfile):
        os.rename(sendfile, "to_send.txt"+TIMESTAMP)
    with open("to_send.txt", "w") as f:
        for item in to_send:
            f.write(f"{item}\n")

def append_success(filepath):
    with open("send_success.txt", "a") as f:
        f.write(f"{filepath}\n")

def append_error(filepath, error_info):
    with open("send_error.txt", "a") as f:
        f.write(f"{filepath}\n")

def import_file(filename) -> list:
    file_list = []
    if (not os.path.exists(filename)):
        return file_list 
    with open(filename, "r") as f:
        for line in f:
            file_list.append(line.strip())
    return file_list

def process_to_send(to_send:list=[]):
    if len(to_send) == 0:
        to_send = import_file("to_send.txt")
    send_success = import_file("send_success.txt")
    for exam_dir in to_send:
        if exam_dir in send_success:
            logging.info(f"Already sent: {exam_dir}")
            continue
        send_dicoms(exam_dir)

def send_dicoms(exam_dir:list) -> int:
    logger = logging.getLogger(__name__) 
    # Initialize Application Entity (AE) and establish association
    ae = AE(ae_title=CALLING_AET)
    mr_context = build_context(MRImageStorage)
    assoc = ae.associate(SERVER_IP, SERVER_PORT, ae_title=CALLED_AET, contexts=[mr_context])
    if not assoc.is_established:
        logging.error("Association failed: could not connect to gateway.")
        print("Exiting...")
        return 0
    
    logger.info(f"Association established. {datetime.datetime.now()}")
    logger.info(f"sending DICOM files from {exam_dir}...")
    dicom_files = 0
    dicom_images = 0
    files_sent = 0
    # Iterate through files in directory and send
    for root, _, files in os.walk(exam_dir):
        for filename in files:
            filepath = os.path.join(root, filename)
            try:
                ds = pydicom.dcmread(filepath)
                dicom_files += 1
                frames = int(ds.get("NumberOfFrames", 1))
                dicom_images += frames
            except Exception as e:
                # print(f"Error reading {filename}: {e}")
                continue

            try:
                # Send dataset
                status = assoc.send_c_store(ds)
                if status:
                    logger.info(f"Sent: {filename}, Status: {status.Status}")
                    files_sent += 1
                else:
                    logger.warning(f"Failed to send: {filename}")
            except Exception as e:
                logger.warning(f"Exception sending {filename}: {e}")
    assoc.release()

    if (files_sent != dicom_files):
        error_str = f"WARNING: sent {files_sent} of {dicom_files} files read."
        logging.warning(error_str)
        append_error(exam_dir, error_str)
        return files_sent

    logging.info(f"Successfully sent {files_sent} of {dicom_files} files in {exam_dir}.")
    logging.info(f"  Found {dicom_images} images in this dataset.")
    append_success(exam_dir)
    return dicom_files

def make_tosend_file(top_dir) -> list:
    # process dir, save as to_send.txt, and return list to send
    to_send = [os.path.join(top_dir, x) for x in os.listdir(top_dir)] 
    savetofile_to_send(to_send)
    return to_send


# sys.argv=['this', r'C:\sharing\testdata\pynet_send']
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: ")
        print("  Parse a directory of DICOM files and send them to the gateway:")
        print("  python batch_xfer.py <directory>")
        print("")
        print('  Parse a directory of DICOM files but only store paths to "to_send.txt":')
        print("  python batch_xfer.py --tofile <directory>")
        print("")
        print('  Parse a file of DICOM file paths to transfer to gateway')
        print("  python batch_xfer.py --fromfile [<file_to_parse, default to_send.txt]")
        print("")
        exit()

    if sys.argv[1] == "--tofile":
        if len(sys.argv) < 3:
            print("Please specify data directory to parse into to_send.txt")
            exit()
        make_tosend_file(sys.argv[2])
        exit()
    if sys.argv[1] == "--fromfile":
        from_file = "to_send.txt"
        if len(sys.argv) > 2:
            from_file = sys.argv[2]
        data_to_send = import_file(from_file)
        process_to_send(data_to_send)
        exit()
    
    # Load the DICOM files from the current directory
    to_send = make_tosend_file(sys.argv[1])
    process_to_send(to_send)