# -*- coding: utf-8 -*-
import json
import sys
import collections
import logging
import matplotlib.pyplot as plt
import pylab
import numpy as np

def get_json(line):
    # Need to first strip the trailing newline, and then escape newlines (which can appear
    # in the middle of some of the JSON) so that JSON library doesn't barf.
    return json.loads(line.strip("\n").replace("\n", "\\n")) # transfer to str

def analysis(filename):

    f = open(filename, "r")
    test_line = f.readline()

    try:
        get_json(test_line)
        is_json = True
        print ("Parsing file %s as JSON" % filename)
    except:
        is_json = False
        print ("Parsing file %s as JobLogger output" % filename)
    f.seek(0)

    # compute the stage number
    stageNumber = 0
    for line in f:
        if is_json:
            json_data = get_json(line)
            event_type = json_data["Event"]

            if event_type == "SparkListenerTaskEnd":
                stage_id = json_data["Stage ID"]
                if stage_id > stageNumber:
                    stageNumber = stage_id
    print "stage number: %s" % stageNumber
    if stageNumber == 0:
        print "There is only one stage in the log"
        exit(1)

    f.seek(0)

    execTime = [0 for x in range(0, stageNumber)]
    gcTime = [0 for x in range(0, stageNumber)]
    FetchWaitTime = [0 for x in range(0, stageNumber)]
    ShuffleWriteTime = [0 for x in range(0, stageNumber)]

    for line in f:
        if is_json:
            json_data = get_json(line)
            event_type = json_data["Event"]

            if event_type == "SparkListenerTaskEnd":
                stage_id = json_data["Stage ID"]
                if json_data.has_key("Task Metrics") == True:
                    task_metrics = json_data["Task Metrics"]

                    for i in range(0, stageNumber):
                        if stage_id == i:
                            execTime[i] += task_metrics["Executor Run Time"]
                            gcTime[i] += task_metrics["JVM GC Time"]
                            if task_metrics.has_key("Shuffle Read Metrics") == True:
                                Shuffle_Read_Metrics = task_metrics["Shuffle Read Metrics"]
                                FetchWaitTime[i] += Shuffle_Read_Metrics["Fetch Wait Time"]
                            elif task_metrics.has_key("Shuffle Write Metrics") == True:
                                Shuffle_Write_Metrics = task_metrics["Shuffle Write Metrics"]
                                ShuffleWriteTime[i] += Shuffle_Write_Metrics["Shuffle Write Time"]

    # convert time unit
    gcTime = [i*10**(-3)for i in gcTime]
    execTime = [i*10**(-3) for i in execTime]
    FetchWaitTime = [i*10**(-3) for i in FetchWaitTime]
    ShuffleWriteTime = [i*10**(-9)for i in ShuffleWriteTime]

    print "JVM GC Time: %s" % gcTime
    print "Executor Run Time %s" % execTime
    print "Fetch Wait Time:%s"% FetchWaitTime
    print "Shuffle Write Time:%s"% ShuffleWriteTime

    totalTime = np.array(gcTime)+np.array(execTime)+np.array(FetchWaitTime)+np.array(ShuffleWriteTime)
    totalTime = [float('%.3f'% i) for i in totalTime]
    print "total time:%s"% totalTime

    for i in range(0, len(totalTime)):
        if totalTime[i] == 0:
            totalTime[i] = 1 #avoid the ZeroDivisionError

    gcTimeRatio = map(lambda(a, b): 100*a/b, zip(gcTime, totalTime))
    execTimeRatio = map(lambda(a, b): 100*a/b, zip(execTime, totalTime))
    FetchWaitTimeRatio = map(lambda(a, b): 100*a/b, zip(FetchWaitTime, totalTime))
    ShuffleWriteTimeRatio = map(lambda(a, b): 100*a/b, zip(ShuffleWriteTime, totalTime))
    #gcTimeRatio = [float('%.3f'% i) for i in gcTimeRatio]
    #execTimeRatio = [float('%.3f'% i) for i in execTimeRatio]
    #FetchWaitTimeRatio = [float('%.3f'% i) for i in FetchWaitTimeRatio]
    #ShuffleWriteTimeRatio = [float('%.3f'% i) for i in ShuffleWriteTimeRatio]
    print "JVM GC Time Ratio:%s" % gcTimeRatio
    print "Executor Run Time Ratio:%s" % execTimeRatio
    print "Fetch Wait Time Ratio:%s"% FetchWaitTimeRatio
    print "Shuffle Write Time Ratio:%s" % ShuffleWriteTimeRatio


    stageID =['stage %d' % i for i in range(0,stageNumber)]
    #plt.rc('font', family='SimHei', size=13)
    num1 = np.array(execTimeRatio)
    num2 = np.array(gcTimeRatio)
    num3 = np.array(ShuffleWriteTimeRatio)
    num4 = np.array(FetchWaitTimeRatio)
    width = 0.4
    idx = np.arange(len(totalTime))
    plt.bar(idx, num1, width, color='purple', label='Executor Run Time')
    plt.bar(idx, num2, width, bottom=num1, color='green', label='GC Time')
    plt.bar(idx, num3, width, bottom=num1+num2, color='blue', label='Shuffle Write Time')
    plt.bar(idx, num4, width, bottom=num1+num2+num3,  color='azure', label='Fetch Wait Time')
    plt.xlabel("stage ID")
    plt.ylabel("Time Ratio(%)")
    plt.ylim(0, 100)
    plt.xticks(idx+width/2, stageID, rotation=40, fontsize=8)
    plt.legend(loc='best', prop={'size': 12})
    #plt.subplots_adjust(left=0.05, right=0.95, bottom=0.12, top=0.95)
    plt.show()


def main(args):
    file = "./pr_log"
    analysis(file)

if __name__ == "__main__":
    main(sys.argv[1:])
