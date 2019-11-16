import requests
import json

if __name__ == '__main__':
    with open('urls.txt') as fp:
        url = fp.readline().strip()
        while url:
            data = {
                'url': url
            }
            headers = { "Content-Type": "application/json"}
            response = requests.put("http://127.0.0.1:5000/api/v1/media/add", data=json.dumps(data), headers=headers)
            url = fp.readline().strip()
