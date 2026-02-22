import http.server
from typing import Any, Dict
import json
from dataclasses import dataclass
import requests


class RequestHandler(http.server.BaseHTTPRequestHandler):
    def handle(self):
        self.wfile.write(b"HTTP/1.1 200 OK\n\n")
        status = json.dumps(self.server.seeder.torrents_status)
        self.wfile.write(status.encode())


class SeederServer(http.server.HTTPServer):
    seeder: Any


def start_server(seeder):
    srv = SeederServer(('127.0.0.1', 7878), RequestHandler)
    srv.seeder = seeder
    srv.serve_forever()


@dataclass
class TorrentStatus:
    progress: float
    download_rate: int
    upload_rate: int
    

def get_status():
    resp = requests.get("http://127.0.0.1:7878")
    resp.raise_for_status()
    data = resp.json()

    statuses = {}
    for key in data:
        item = data[key]       
        del item['files'] 
        statuses[int(key)] = TorrentStatus(**item)

    return statuses

if __name__ == '__main__':
    print(get_status())
    
