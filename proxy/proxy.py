import threading
import requests
import queue
import csv
import json
import random

# Define a lock for thread-safe file writing
lock = threading.Lock()

# List user agents
with open('helper\list_user_agent.txt', 'r') as f:
    user_agents = f.read().split('\n')

def fetch_url_with_proxy(proxy_queue, output_file, url='https://finfo-api.vndirect.com.vn/v4/stock_prices'):
    """

    Fetches the given URL with the given proxy queue and writes the good proxies to the given output_file.

    Args:
        proxy_queue (queue.Queue): Queue of proxies to try.
        output_file (str): File to output good proxies to.
        url (str, optional): URL to fetch. Defaults to 'https://finfo-api.vndirect.com.vn/v4/stock_prices'.
    """
    while not proxy_queue.empty():
        proxy = proxy_queue.get()
        url = url
        proxies = {
            "http": proxy,
            "https": proxy
        }
        headers = {
            'User-Agent': random.choice(user_agents)
        }
        try:
            response = requests.get(url, proxies=proxies, headers=headers, timeout=5)
            try:
                response.json()
                with lock:
                    with open(output_file, 'a', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow([proxy])
            except json.JSONDecodeError:
                print(f"Proxy {proxy} failed to decode json. {response.url}")
        except requests.RequestException as e:
            print(f"Proxy {proxy} failed: {e}")
        finally:
            proxy_queue.task_done()

# List of proxies
with open('proxy\proxy_list_raw.txt', 'r') as f:
    proxy_list = f.read().split('\n')

# Output CSV file
output_file = 'proxy\proxy_list_filter.txt'

# Create the CSV file and write the header
with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    # writer.writerow(['Proxy'])

# Create a queue and add proxies to it
proxy_queue = queue.Queue()
for proxy in proxy_list:
    proxy_queue.put(proxy)

# List to hold thread objects
threads = []

# Create and start threads
for _ in range(len(proxy_list)): # Adjust the number of threads as needed: range(len(proxy_list))
    thread = threading.Thread(target=fetch_url_with_proxy, args=(proxy_queue,output_file))
    thread.start()
    threads.append(thread)

# Wait for all threads to complete
for thread in threads:
    thread.join()

print("All requests have been made.")