import os
import sys

from receive import consume
from send_producer_2 import send

if __name__ == '__main__':
    try:
        consume()
        send()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)
