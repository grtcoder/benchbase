#!./env/bin python
import requests
import time
data = {
    "transactions": [
        {
        "key": "example_key",
        "value": "example_value",
        "isDelete": False
        },
    ],
}

url = 'http://localhost:8080'

while True:
    response = requests.post(url,json=data)
    if response.status_code == 200:
        print(f'Request successful\n{response.text}')
    else:
        print('Request failed')
    
    time.sleep(0.001)