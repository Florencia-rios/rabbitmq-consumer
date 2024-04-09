import os
import sys

from queues_jobs_service.receive import consume_jobs_service
from queues_nlp_worker.receive import consume_nlp_worker
from queues_ocr_worker.receive import consume_ocr_worker

if __name__ == '__main__':
    try:
        consume_jobs_service()
        consume_nlp_worker()
        consume_ocr_worker()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)
