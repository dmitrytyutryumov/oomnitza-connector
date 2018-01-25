import json
import logging

from lib.config import get_dss_url


class LogHandler(logging.StreamHandler):
    def emit(self, record):
        portion_id = None
        session = None
        msg = record.msg
        status = record.level

        if hasattr(record, 'session'):
            session = record.session
        if hasattr(record, 'portion_id'):
            portion_id = record.portion_id

        if not portion_id or not session:
            return super(LogHandler, self).emit(record)

        data = json.dumps({
            'status': status,
            'text': msg
        })

        url = ''.join(
            [get_dss_url(), '/api/v2/log/{}'.format(portion_id)])
        response = session.post(url=url, data=data)
        assert response.status_code == 201

        super(LogHandler, self).emit(record)


logger = logging.getLogger(__name__)
logger.addHandler(LogHandler(logging.INFO))
logger.addHandler(LogHandler(logging.DEBUG))
logger.addHandler(LogHandler(logging.ERROR))
logger.addHandler(LogHandler(logging.WARNING))

