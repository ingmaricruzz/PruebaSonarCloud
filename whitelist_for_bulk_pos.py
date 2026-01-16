#!/usr/bin/python3 -W ignore

import json, sys, os, logging, cx_Oracle
import logging.config
from datetime import datetime

sys.path.insert(0, '../../broker/lib')

from utryt import cgi as cgi

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(CURRENT_DIR, "..","logs")
ALLOWED_METHOD = 'GET'
DB_CONFIG = {
    'DNS':'DNS',
    'USER':'USER',
    'PASSWORD':'PASSWORD'
}
PL_BLOCK = """
BEGIN
    :whitelist := emv_config_equipos.fn_get_listablanca_POS;
END;
"""

request_method = os.environ.get('REQUEST_METHOD', '').upper()
whitelist_command = None

logging.basicConfig(
    filename=f"{LOGS_DIR}/entity.whitelist_for_bulk_pos.log",
    filemode="a",
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    level=logging.INFO
)

# FUNCTIONS

def send_response(status: int, detail: str, response_content: str) -> None:
    """
    Sends a JSON HTTP response with the given status, detail, and content.

    Args:
        status (int): The HTTP status code to send.
        detail (str): A brief description of the status.
        response_content (str): The content to include in the response body.

    Returns:
        None
    """
    
    print(f"Status: {status} {detail}")
    print("Content-Type: application/json")
    print()
    print(response_content)

def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if defaultType == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)

def fetch_whitelist():
    BYTES_IN_ONE_UCS2_CODE_POINT = 2
    ONE_GIGABYTE = 1
    GIGABYTES_IN_BYTES = 1e-9
    THREE_HUNDRED_UCS2_CODE_POINTS = 300
    whitelist_clob = None
    whitelist_command = ''
    total_gigabytes = 0
    chunk = None
    ucs2_code_points_offset = 1

    with cx_Oracle.connect(DB_CONFIG["USER"], DB_CONFIG["PASSWORD"], DB_CONFIG["DNS"]) as connection:
        
        connection.outputtypehandler = OutputTypeHandler
        
        cursor = connection.cursor()
        
        whitelist_clob = cursor.var(cx_Oracle.CLOB)

        cursor.execute(PL_BLOCK, whitelist=whitelist_clob)

        total_gigabytes = (whitelist_clob.getvalue().size() * BYTES_IN_ONE_UCS2_CODE_POINT) * GIGABYTES_IN_BYTES

        logger.info(f"Whitelist size GB: {total_gigabytes}")

        if total_gigabytes < ONE_GIGABYTE:
            whitelist_command = f'{whitelist_clob.getvalue().read()}'
        else:
            while True:
                chunk = whitelist_clob.getvalue().read(ucs2_code_points_offset, THREE_HUNDRED_UCS2_CODE_POINTS)
                
                if chunk:
                    whitelist_command+=chunk
                
                if len(chunk) < THREE_HUNDRED_UCS2_CODE_POINTS:
                    break

                ucs2_code_points_offset+=len(chunk)
    
    return whitelist_command


logger = logging.getLogger()

logger.info("Starting")

if request_method != ALLOWED_METHOD:
    logger.info(f"Method not allowed: {request_method}")
    send_response(405,"Method Not Allowed",'{\"error\":\"El método permitido es \'GET\'\"}')
    sys.exit(0)

logger.info(f"Connecting to DB")

try:
    whitelist_command = fetch_whitelist()
except Exception as e:
    logger.error(f"Error while fetching blacklist: {str(e)}")
    send_response(500, 'Internal Server Error','')
    sys.exit(0)

logger.info("Sending response")

send_response(200, 'Ok', '{"comando":'+whitelist_command+'}')





