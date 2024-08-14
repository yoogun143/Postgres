import requests
import stem.process
from stem import Signal
from stem.control import Controller

# Function to change IP
def change_tor_ip():
    with Controller.from_port(port=9051) as controller:
        controller.authenticate(password='welcome')  # Use the password you hashed
        controller.signal(Signal.NEWNYM)

# Function to make a request through Tor and get IP address
def get_ip():
    session = requests.session()
    session.proxies = {
        'http': 'socks5h://localhost:9050',
        'https': 'socks5h://localhost:9050'
    }
    response = session.get('https://api.ipify.org?format=json')
    return response.json()

# URL to scrape
url = 'https://finfo-api.vndirect.com.vn/v4/stock_prices'
# url = 'http://example.com'

for _ in range(5):  # Adjust the number of requests as needed
    change_tor_ip()  # Rotate IP
    ip = get_ip()  # Get the current IP
    print(f'Current IP: {ip["ip"]}')
    
    # Make a request to the target URL
    response = requests.get(url, proxies={'http': 'socks5h://localhost:9050', 'https': 'socks5h://localhost:9050'})
    print(response.status_code)
    print(response.text)
