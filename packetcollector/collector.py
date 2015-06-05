from BaseHTTPServer import BaseHTTPRequestHandler
from StringIO import StringIO
from cgi import parse_header, parse_multipart
from urlparse import parse_qs


import pycap.capture
import redis
import json

class HTTPRequest(BaseHTTPRequestHandler):
    def __init__(self, request_text):
        self.rfile = StringIO(request_text)
        self.raw_requestline = self.rfile.readline()
        self.error_code = self.error_message = None
        self.parse_request()

    def send_error(self, code, message):
        self.error_code = code
        self.error_message = message


class Ethernet:
    def __init__(self, type, src, dest):
        self.type = type
        self.src = src
        self.dest = dest

class ArpEvent:
    def __init__(self, eth, op, srcmac, srcip, destmac, destip):
        self.op = op
        self.ethernet = json.loads(json.dumps(eth.__dict__))
        self.src_mac = srcmac
        self.src_ip = srcip
        self.dest_mac = destmac
        self.dest_ip = destip


class HTTPEvent():
    def __init__(self, eth, srcip, destip, http):
        self.eth = json.loads(json.dumps(eth.__dict__))
        self.src_ip = srcip
        self.dest_ip = destip
        self.method = http.command if http.command else ''
        self.path = http.path if 'path' in http.__dict__ else ''
        self.data = ''
        if 'headers' not in http.__dict__:
            return
        if 'host' not in http.headers:
            return
        self.host = http.headers['host']

        if self.method != 'POST':
            return

        ctype, pdict = parse_header(http.headers['content-type'])

        if ctype == 'multipart/form-data':
            self.data = parse_multipart(self.rfile, pdict)
        else:
            length = int(http.headers['content-length'])
            self.data = parse_qs(http.rfile.read(length),
                                 keep_blank_values=1)

def make_ethernet(packet):
    type = packet[0].type
    src = packet[0].source
    dest = packet[0].destination
    e = Ethernet(type, src, dest)
    return e

def make_arp(packet, e):
    srcmac = packet[1].sourcehardware
    srcip = packet[1].sourceprotocol
    destmac = packet[1].targethardware
    destip = packet[1].targetprotocol
    op = packet[1].operation
    a = ArpEvent(e, op, srcmac, srcip, destmac, destip)
    return a

def make_http(packet, e):
    srcip = packet[1].source
    destip = packet[1].destination
    h = HTTPRequest(packet[3])

    he = HTTPEvent(e, srcip, destip, h)
    if he.method is '':
        return None
    return he

rd = redis.Redis(host='125.209.196.135', port=6379, db=0)
p = pycap.capture.capture()
p.filter('tcp or arp')

while True:
    packet = p.next()
    if not packet:
        continue

    e = make_ethernet(packet)

    event = None
    j = ''
    if e.type == 8 and packet[1].protocol == 6:
        # In this case packet is tcp
        if packet[2].destinationport == 80:
            event = make_http(packet, e)
            if event is None:
                continue
            j = json.dumps(event.__dict__)
            rd.rpush('http', j)

    elif e.type == 1544 and packet[1].protocol == 2048:
        event = make_arp(packet, e)
        if event is None:
                continue
        j = json.dumps(event.__dict__)
        rd.rpush('arp', j)



