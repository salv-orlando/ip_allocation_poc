import datetime
import logging


class ColorHandler(logging.StreamHandler):
    LEVEL_COLORS = {
        logging.DEBUG: '\033[00;32m',  # GREEN
        logging.INFO: '\033[00;36m',  # CYAN
        logging.WARN: '\033[01;33m',  # BOLD YELLOW
        logging.ERROR: '\033[01;31m',  # BOLD RED
        logging.CRITICAL: '\033[01;31m',  # BOLD RED
    }

    def format(self, record):
        record.color = self.LEVEL_COLORS[record.levelno]
        return logging.StreamHandler.format(self, record)


def setup():
    log_root = getLogger(None).logger
    log_root.setLevel(logging.INFO)
    streamlog = ColorHandler()
    log_root.addHandler(streamlog)
    for handler in log_root.handlers:
        handler.setFormatter(
            logging.Formatter(
                '%(asctime)s.%(msecs)03d %(process)d %(levelname)s '
                '%(name)s [-] %(message)s'))


class TimelineAdapter(logging.LoggerAdapter):

    def __init__(self, logger):
        super(TimelineAdapter, self).__init__(logger, {})
        self.events = {}
        self.transactions = {}
        self.attempts = 0

    def info(self, msg, *args, **kwargs):
        event = kwargs.pop('event', None)
        t_start = kwargs.pop('transaction_start', None)
        t_commit = kwargs.pop('transaction_commit', None)
        t_abort = kwargs.pop('transaction_abort', None)
        if event:
            self.events[event] = datetime.datetime.utcnow()
        if t_start:
            self.transactions[t_start] = "unknown"
        if t_abort:
            self.transactions[t_abort] = "abort"
        if t_commit:
            self.transactions[t_commit] = "commit"
        super(TimelineAdapter, self).info(msg, *args, **kwargs)

    def transaction_stats(self):

        def count_by_state(state):
            return len([t for t in self.transactions if
                        self.transactions[t] == state])

        committed = count_by_state('commit')
        aborted = count_by_state('abort')
        unknown = count_by_state('unknown')
        print self.logger.name
        print("Committed transactions:%d" % committed)
        print("Aborted transactions:%d" % aborted)
        print("Unknown state transactions:%d" % unknown)
        return committed, aborted, unknown

    def dump_events(self):
        sorted_events = sorted(self.events.iteritems(),
                               key=lambda event: event[1])
        first_ts = sorted_events[0][1]
        print self.logger.name
        for (event, timestamp) in sorted_events:
            print "%s - %s - %s" % (event,
                                    timestamp,
                                    timestamp - first_ts)

    def completed(self):
        return 'end' in self.events

    def execution_time(self):
        return (self.events['end'] - self.events['start']).total_seconds()


_loggers = {}


def getLogger(name='unknown'):
    if name not in _loggers:
        _loggers[name] = TimelineAdapter(logging.getLogger(name))
    return _loggers[name]
