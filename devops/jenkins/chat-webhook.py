from httplib2 import Http
from json import dumps
import sys

#
# Hangouts Chat incoming webhook quickstart
#
def main(message):
    url = 'https://chat.googleapis.com/v1/spaces/AAAABDLkcb0/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=mjbAUMUiZS0Kl40RhOsrTXIiFApK36Fy15cmqQu7RhE%3D'
    bot_message = {'text' : message, 'thread': {'name': 'spaces/AAAABDLkcb0/threads/cnbMVljHQBY'}}

    message_headers = { 'Content-Type': 'application/json; charset=UTF-8'}

    http_obj = Http()

    response = http_obj.request(
        uri=url,
        method='POST',
        headers=message_headers,
        body=dumps(bot_message),
    )

    print(response)

if __name__ == '__main__':
    main(sys.argv[1])