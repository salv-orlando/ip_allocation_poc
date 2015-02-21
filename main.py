import json
import numpy
import sys
import time
import threading
import uuid

from oslo_db import exception as db_exc

from algorithms import db_lock
from algorithms import two_step_with_retry as twostep
from algorithms import three_steps
import db
import log

thread_logs = []
algorithms = {
    'lock-for-update': db_lock.run,
    '2-step-seq-range-check': twostep.run_with_range_check,
    '2-step-seq-no-range-check': twostep.run_without_range_check,
    '2-step-rnd-range-check': twostep.run_rnd_with_range_check,
    '2-step-rnd-no-range-check': twostep.run_rnd_without_range_check,
    '3-step': three_steps.run,
    '3-step-rnd': three_steps.run_rnd}

subnet_create_func = {
    'lock-for-update': db.create_subnet,
    '2-step-seq-range-check': db.create_subnet_alt,
    '2-step-seq-no-range-check': db.create_subnet_alt,
    '2-step-rnd-range-check': db.create_subnet_alt,
    '2-step-rnd-no-range-check': db.create_subnet_alt,
    '3-step': db.create_subnet,
    '3-step-rnd': db.create_subnet}

success_funcs = {
    'lock-for-update': db_lock.verify_correctness,
    '3-step': three_steps.verify_correctness,
    '3-step-rnd': three_steps.verify_correctness}

log.setup()
LOG = log.getLogger(__name__)
sql_connection = sys.argv[1]
if len(sys.argv) > 3:
    thread_desc = sys.argv[3]
else:
    thread_desc = 'test_threads.json'

subnet_id = None
if len(sys.argv) > 4:
    subnet_id = sys.argv[4]
algorithm = sys.argv[2]
db.set_av_range_model(algorithm)
db.set_ip_request_model(algorithm)


def thread_wrapper(*args, **kwargs):
    t_name = kwargs.get('name')
    thread_log = log.getLogger(t_name)
    thread_log.info("Start", event='start')
    algorithms[algorithm](*args, **kwargs)
    thread_log.info("End", event='end')


LOG.info("BEGIN", event='start')
threads_data = json.load(open(thread_desc))
LOG.info("Will spawn %d threads", len(threads_data))
engine = db.get_engine(sql_connection)
db.BASE.metadata.create_all(engine)

session = db.get_session(sql_connection)

# Create a subnet
if not subnet_id:
    subnet = subnet_create_func[algorithm](
        session, str(uuid.uuid4()), '192.168.0.0/24', 4,
        [{'start': '192.168.0.2', 'end': '192.168.0.254'}])
    subnet_id = subnet['id']
    LOG.info("Created subnet with id:%s", subnet_id)
else:
    subnet = session.query(db.Subnet).filter_by(id=subnet_id).one()
    LOG.info("Loaded subnet with id:%s", subnet_id)

threads = []
for thread_name in threads_data:
    thread = threading.Thread(
        target=thread_wrapper,
        name=thread_name,
        args=[sql_connection],
        kwargs={'name': thread_name,
                'subnet_id': subnet_id,
                'ip_address': threads_data[thread_name].get('ip_address'),
                'thread_logs': thread_logs,
                'steps': threads_data[thread_name]['steps_wait']})
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

LOG.info("END", event='end')

print("")
LOG.dump_events()
for thread_log in thread_logs:
    thread_log.dump_events()
print("")
committed = 0
aborted = 0
unknown = 0
failures = 0
exec_times = []
attempts = []
for thread_log in thread_logs:
    c, a, u = thread_log.transaction_stats()
    print("Retries:%d" % thread_log.attempts)
    if thread_log.completed():
        exec_times.append(thread_log.execution_time())
        attempts.append(thread_log.attempts)
        print("Execution time:%s" % thread_log.execution_time())
    else:
        failures = failures + 1
        print("The thread failed to complete")
    committed = committed + c
    aborted = aborted + a
    unknown = unknown + u

print("")
print("Total committed:%d" % committed)
print("Total aborted:%d" % aborted)
print("Total unknown state:%d" % unknown)
print("")

success_func = success_funcs.get(algorithm)
success = None
if success_func:
    success = success_func(session, subnet_id)

print("#OUTCOME The execution was successful:%s" % success)
print("#TRH_FAIL Total failed threads:%d" % failures)
print("#THR_RETR_MEAN Mean retries per thread:%.3f" % numpy.mean(attempts))
print("#THR_RETR_VAR Retries per thread variance:%.3f" % numpy.var(attempts))
print("#THR_TIME_MEAN Mean thread run time:%.3f" % numpy.mean(exec_times))
print("#THR_TIME_VAR Thread run time variance:%.3f" % numpy.var(exec_times))
print("")
print("#SELECT statements:%d - total:%.5f - mean:%.5f - var:%.5f" %
      (len(db.query_stats['SELECT']),
       sum(db.query_stats['SELECT']),
       numpy.mean(db.query_stats['SELECT']),
       numpy.var(db.query_stats['SELECT'])))
print("#INSERT statements:%d - total:%.5f - mean:%.5f - var:%.5f" %
      (len(db.query_stats['INSERT']),
       sum(db.query_stats['INSERT']),
       numpy.mean(db.query_stats['INSERT']),
       numpy.var(db.query_stats['INSERT'])))
print("#UPDATE statements:%d - total:%.5f - mean:%.5f - var:%.5f" %
      (len(db.query_stats['UPDATE']),
       sum(db.query_stats['UPDATE']),
       numpy.mean(db.query_stats['UPDATE']),
       numpy.var(db.query_stats['UPDATE'])))
print("#DELETE statements:%d - total:%.5f - mean:%.5f - var:%.5f" %
      (len(db.query_stats['DELETE']),
       sum(db.query_stats['DELETE']),
       numpy.mean(db.query_stats['DELETE']),
       numpy.var(db.query_stats['DELETE'])))
