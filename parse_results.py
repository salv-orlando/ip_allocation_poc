import json

import numpy

RESULT_FILE = 'results.json'

results = json.load(open(RESULT_FILE))

for test_name in results:
    experiments = results[test_name]
    avg_thr_times = [exp['AVG_THR_TIME'] for exp in experiments
                     if exp['success']]
    var_thr_times = [exp['VAR_THR_TIME'] for exp in experiments
                     if exp['success']]
    avg_thr_retries = [exp['AVG_THR_RETRIES'] for exp in experiments
                       if exp['success']]
    var_thr_retries = [exp['VAR_THR_RETRIES'] for exp in experiments
                       if exp['success']]
    avg_num_select = []
    avg_time_select = []
    avg_num_insert = []
    avg_time_insert = []
    avg_num_update = []
    avg_time_update = []
    avg_num_delete = []
    avg_time_delete = []
    for exp in experiments:
        if 'NUM_SELECT' in exp:
            avg_num_select.append(exp['NUM_SELECT'])
            avg_time_select.append(exp['TIME_SELECT'])
        else:
            avg_num_select.append(0)
        if 'NUM_INSERT' in exp:
            avg_num_insert.append(exp['NUM_INSERT'])
            avg_time_insert.append(exp['TIME_INSERT'])
        else:
            avg_num_insert.append(0)
        if 'NUM_UPDATE' in exp:
            avg_num_update.append(exp['NUM_UPDATE'])
            avg_time_update.append(exp['TIME_UPDATE'])
        else:
            avg_num_update.append(0)
        if 'NUM_DELETE' in exp:
            avg_num_delete.append(exp['NUM_DELETE'])
            avg_time_delete.append(exp['TIME_DELETE'])
        else:
            avg_num_delete.append(0)

    print("Test Name: %s" % test_name)
    print("Number of experiments: %d" % len(experiments))
    print("Successful experiments: %d" % len([exp for exp in experiments if
                                              exp['success']]))
    print("-------------------------------------------")
    print("Average thread execution time: %.5f sec (variance: %.5f)" %
          (numpy.mean(avg_thr_times), numpy.mean(var_thr_times)))
    print("Average retries per thread: %.3f sec (variance: %.3f)" %
          (numpy.mean(avg_thr_retries), numpy.mean(var_thr_retries)))
    print("SELECT queries: Average:%.3f, Variance:%.3f" %
          (numpy.mean(avg_num_select), numpy.var(avg_num_select)))
    print("SELECT time: Average:%.3f, Variance:%.3f" %
          (numpy.mean(avg_time_select), numpy.var(avg_time_select)))
    print("INSERT queries: Average:%.3f, Variance:%.3f" %
          (numpy.mean(avg_num_insert), numpy.var(avg_num_insert)))
    print("INSERT time: Average:%.3f, Variance:%.3f" %
          (numpy.mean(avg_time_insert), numpy.var(avg_time_insert)))
    print("UPDATE queries: Average:%.3f, Variance:%.3f" %
          (numpy.mean(avg_num_update), numpy.var(avg_num_update)))
    print("UPDATE time: Average:%.3f, Variance:%.3f" %
          (numpy.mean(avg_time_update), numpy.var(avg_time_update)))
    print("DELETE queries: Average:%.3f, Variance:%.3f" %
          (numpy.mean(avg_num_delete), numpy.var(avg_num_delete)))
    print("DELETE time: Average:%.3f, Variance:%.3f" %
          (numpy.mean(avg_time_delete), numpy.var(avg_time_delete)))
    print("")
