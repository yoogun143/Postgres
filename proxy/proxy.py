import threading
import requests
import queue
import csv

# Define a lock for thread-safe file writing
lock = threading.Lock()

# Define the function to make requests with a proxy
def fetch_url_with_proxy(proxy_queue, output_file, url='https://finfo-api.vndirect.com.vn/v4/stock_prices'):
    while not proxy_queue.empty():
        proxy = proxy_queue.get()
        url = url
        proxies = {
            "http": proxy,
            "https": proxy
        }
        try:
            response = requests.get(url, proxies=proxies, timeout=5)
            if response.status_code == 200:
                print(f"Proxy {proxy}: {response.status_code}")
                with lock:
                    with open(output_file, 'a', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow([proxy])
            else:
                print(f"Proxy {proxy}: {response.status_code}")
        except requests.RequestException as e:
            print(f"Proxy {proxy} failed: {e}")
        finally:
            proxy_queue.task_done()

# List of proxies
with open('proxy\proxy_list_raw.txt', 'r') as f:
    proxy_list = f.read(). split('\n')

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
for _ in range(len(proxy_list)):
    thread = threading.Thread(target=fetch_url_with_proxy, args=(proxy_queue,output_file))
    thread.start()
    threads.append(thread)

# Wait for all threads to complete
for thread in threads:
    thread.join()

print("All requests have been made.")