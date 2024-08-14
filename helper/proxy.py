import threading
import queue

import requests

q = queue.Queue()
valid_proxies = []

with open('proxy_list.txt', 'r') as f:
    proxies = f.read(). split('\n')
    for p in proxies:
        q.put(p)

def check_proxies(): 
    global q
    while not q.empty():
        proxy = q.get()
        try: 
            res = requests.get('http://finfo')