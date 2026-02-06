import logging
from logging.handlers import RotatingFileHandler
def log(message,logger_loc='logger/log.log'):
    # Create and configure logger
    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)

    # Prevent adding multiple handlers to the logger if the function is called multiple times
    if not logger.handlers:
        # Create console handler for output to the console
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # Create file handler for output to a file, with log rotation
        fh = RotatingFileHandler(logger_loc, maxBytes=1024*1024*5, backupCount=5)
        fh.setLevel(logging.DEBUG)

        # Create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)

        # Add the handlers to the logger
        logger.addHandler(ch)
        logger.addHandler(fh)

    # Log the message
    logger.info(message)
from dotenv import load_dotenv
import os
import requests
load_dotenv()
TOKEN = os.getenv('notify')
chat_id = os.getenv('chat_id')

api_key_cn = os.getenv('api_key_cn')

# Global variable to keep track of the last message sent
last_sent_message = None


def send_notification_custom(message):
    global last_sent_message

    if message == last_sent_message:
        return  # Do not send the message
    last_sent_message = message
    # Set up the notification content
    chat_id = os.getenv('chat_id')
    # Send the notification
    load_dotenv()

    url = f'https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}'
    try:
        response = requests.get(url)
        return 
    except Exception as e:
        log(e)
    return


send_notification_custom(f'hi there boss')

import base64
from io import BytesIO


def get_logo_encoding(max_width: int = 180) -> str:
    """Return the base64-encoded PNG of `white_back.png`.

    If Pillow is available the image will be resized down to `max_width`
    while preserving aspect ratio. If Pillow is not available the raw
    file bytes will be encoded and returned.
    """
    img_path = "white_back.png"
    if not os.path.exists(img_path):
        raise FileNotFoundError(f"Logo file not found: {img_path}")

    try:
        from PIL import Image
    except Exception:
        # Pillow not installed — return the raw file encoding
        with open(img_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

    # Open and optionally resize
    with Image.open(img_path) as im:
        w, h = im.size
        if max_width and w > max_width:
            new_h = max(1, int(h * (max_width / float(w))))
            im = im.resize((max_width, new_h), Image.LANCZOS)

        buf = BytesIO()
        im.save(buf, format="PNG")
        return base64.b64encode(buf.getvalue()).decode("utf-8")
