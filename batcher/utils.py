
import os
import numpy as np
import logging

import base64
import cv2

import json

def get_path(relative_path):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        relative_path)

def resize_and_pad(image, new_size):
    h, w = image.shape[:2]
    ratio = float(new_size) / max(h,w)

    new_hw = tuple(int(ratio * x) for x in (h,w))
    image = cv2.resize(image, (new_hw[1], new_hw[0]))  # resize expects (w,h)

    delta_h = new_size - new_hw[0]
    delta_w = new_size - new_hw[1]
    top, bottom = delta_h//2, delta_h-(delta_h//2)
    left, right = delta_w//2, delta_w-(delta_w//2)
    image = cv2.copyMakeBorder(image, top, bottom, left, right, cv2.BORDER_CONSTANT, value=[0,0,0])

    # image = cv2.copyMakeBorder(image, 0, delta_h, 0, delta_w, cv2.BORDER_WRAP)
    return image

def resize_pad_scale(img, new_size, bgr = True):
    #TODO May need remove this conversion
    if bgr:
        img = img[:,:,[2,1,0]]  # BGR to RGB
    img = resize_and_pad(img, new_size)
    img = (img - np.array([123.68, 116.779, 103.939])) / 255.0 
    return img

def encode_b64_img(img):
    b64_str = cv2.imencode('.jpg', img)[1].tostring()
    b64_str = base64.b64encode(b64_str)
    return b64_str

def decode_b64_img(b64_img):
    img = base64.b64decode(b64_img)
    img = np.fromstring(img, np.uint8)
    img = cv2.imdecode(img, cv2.IMREAD_COLOR)
    if len(img.shape) == 4: # remove alpha channel
        return img[:,:,:3]
    elif len(img.shape) != 3:
        return np.zeros((*img.shape[:2], 3))
    return img

def decode_and_preprocess(b64_img, new_size):
    # import pdb; pdb.set_trace()
    img = decode_b64_img(b64_img)
    img = resize_pad_scale(img, new_size, bgr=True)
    return img

class GCloudFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        lr = logging.LogRecord(None, None, '', 0, '', (), None, None)
        self.default_keys = [key for key in lr.__dict__]

    def extra_data(self, record):
        return {
            key: getattr(record, key)
            for key in record.__dict__ if key not in self.default_keys
        }

    def format(self, record):
        log_data = {
            "severity": record.levelname,
            "path_name": record.pathname,
            "function_name": record.funcName,
            "message": record.getMessage(),
            **self.extra_data(record)
        }
        return json.dumps(log_data)

def init_logging(logLevel='info'):
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    gcloud_formatter = GCloudFormatter()
    stream_handler.setFormatter(gcloud_formatter)
    log = logging.getLogger(__name__)
    if 'debug' == logLevel:
        log.setLevel(logging.DEBUG)
    elif 'info' == logLevel:
        log.setLevel(logging.INFO)
    elif 'warning' == logLevel:
        log.setLevel(logging.WARNING)
    elif 'error' == logLevel:
        log.setLevel(logging.ERROR)
    elif 'critical' == logLevel:
        log.setLevel(logging.CRITICAL)
    else:
        log.setLevel(logging.FATAL)
    log.addHandler(stream_handler)
    return log


