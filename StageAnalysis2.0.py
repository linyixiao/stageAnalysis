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
    return json.loads(line.strip("\n").replace("\n", "\\n"))#transfer to str

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


    execTime = [0, 0, 0, 0, 0, 0]
    gcTime = [0, 0, 0, 0, 0, 0]
    FetchWaitTime = [0, 0, 0, 0, 0, 0]
    ShuffleWriteTime = [0, 0, 0, 0, 0, 0]
    for line in f:
        if is_json:
            json_data = get_json(line)
            event_type = json_data["Event"]

            if event_type == "SparkListenerTaskEnd":
                stage_id = json_data["Stage ID"]
                if json_data.has_key("Task Metrics") == True:
                    task_metrics = json_data["Task Metrics"]
                    if stage_id == 0:
                        execTime[0] += task_metrics["Executor Run Time"]*10**(-3)
                        gcTime[0] += task_metrics["JVM GC Time"]*10**(-3)
                        if task_metrics.has_key("Shuffle Read Metrics") == True:
                            Shuffle_Read_Metrics = task_metrics["Shuffle Read Metrics"]
                            FetchWaitTime[0] += float(Shuffle_Read_Metrics["Fetch Wait Time"]*10**(-3))
                        elif task_metrics.has_key("Shuffle Write Metrics") == True:
                            Shuffle_Write_Metrics = task_metrics["Shuffle Write Metrics"]
                            ShuffleWriteTime[0] += float(Shuffle_Write_Metrics["Shuffle Write Time"]*10**(-9))
                    elif stage_id == 1:
                        execTime[1] += task_metrics["Executor Run Time"]*10**(-3)
                        gcTime[1] += task_metrics["JVM GC Time"]*10**(-3)
                        if task_metrics.has_key("Shuffle Read Metrics") == True:
                            Shuffle_Read_Metrics = task_metrics["Shuffle Read Metrics"]
                            FetchWaitTime[0] += float(Shuffle_Read_Metrics["Fetch Wait Time"]*10**(-3))
                        elif task_metrics.has_key("Shuffle Write Metrics") == True:
                            Shuffle_Write_Metrics = task_metrics["Shuffle Write Metrics"]
                            ShuffleWriteTime[0] += float(Shuffle_Write_Metrics["Shuffle Write Time"]*10**(-9))

                    elif stage_id == 2:
                        execTime[2] += task_metrics["Executor Run Time"]*10**(-3)
                        gcTime[2] += task_metrics["JVM GC Time"]*10**(-3)
                        if task_metrics.has_key("Shuffle Read Metrics") == True:
                            Shuffle_Read_Metrics = task_metrics["Shuffle Read Metrics"]
                            FetchWaitTime[2] += float(Shuffle_Read_Metrics["Fetch Wait Time"]*10**(-3))
                        elif task_metrics.has_key("Shuffle Write Metrics") == True:
                            Shuffle_Write_Metrics = task_metrics["Shuffle Write Metrics"]
                            ShuffleWriteTime[2] += float(Shuffle_Write_Metrics["Shuffle Write Time"]*10**(-9))
                    elif stage_id == 5:
                        execTime[3] += task_metrics["Executor Run Time"]*10**(-3)
                        gcTime[3] += task_metrics["JVM GC Time"]*10**(-3)
                        if task_metrics.has_key("Shuffle Read Metrics") == True:
                            Shuffle_Read_Metrics = task_metrics["Shuffle Read Metrics"]
                            FetchWaitTime[3] += float(Shuffle_Read_Metrics["Fetch Wait Time"]*10**(-3))
                        elif task_metrics.has_key("Shuffle Write Metrics") == True:
                            Shuffle_Write_Metrics = task_metrics["Shuffle Write Metrics"]
                            ShuffleWriteTime[3] += float(Shuffle_Write_Metrics["Shuffle Write Time"]*10**(-9))


            else:
                pass

    print "JVM GC Time: %s" % gcTime
    print "Executor Run Time %s" % execTime

    print "Fetch Wait Time:%s"% FetchWaitTime
    print "Shuffle Write Time:%s"% ShuffleWriteTime

    totalTime = np.array(gcTime)+np.array(execTime)+np.array(FetchWaitTime)+np.array(ShuffleWriteTime)

    print "total time:%s"% totalTime

    for i in range(0, len(totalTime)):
        if totalTime[i] == 0:
            totalTime[i] = 1 #avoid the ZeroDivisionError

    gcTimeRatio = map(lambda(a, b): 100*float(a)/float(b), zip(gcTime, totalTime))
    execTimeRatio = map(lambda(a, b): 100*float(a)/float(b), zip(execTime, totalTime))
    FetchWaitTimeRatio =  map(lambda(a, b): 100*float(a)/float(b), zip(FetchWaitTime, totalTime))
    ShuffleWriteTimeRatio = map(lambda(a, b): 100*float(a)/float(b), zip(ShuffleWriteTime, totalTime))
    print "JVM GC Time Ratio:%s" % gcTimeRatio
    print "Executor Run Time Ratio:%s" % execTimeRatio
    print "Fetch Wait Time Ratio:%s"% FetchWaitTimeRatio
    print "Shuffle Write Time Ratio:%s" % ShuffleWriteTimeRatio


    stageID =["stage0", "stage1", "stage2", "stage3", "stage4", "stage5"]
    #plt.rc('font', family='SimHei', size=13)

    num1 = np.array(execTimeRatio)
    num2 = np.array(gcTimeRatio)
    num3 = np.array(ShuffleWriteTimeRatio)
    num4 = np.array(FetchWaitTimeRatio)
    width = 0.4
    idx = np.arange(len(totalTime))
    plt.bar(idx, num1, width,  color='purple', label='execTime ratio')
    plt.bar(idx, num2, width,  bottom=num1, color='green', label='GCtime ratio')
    plt.bar(idx, num3, width,   bottom=num1+num2, color='yellow', label='Shuffle Write Time Ratio')
    plt.bar(idx, num4, width, bottom=num1+num2+num3,  color='blue', label='Fetch Wait Time Ratio')
    plt.xlabel("stage ID")
    plt.ylabel("Time Ratio(%)")
    plt.ylim(0, 100)
    plt.xticks(idx+width/2, stageID, rotation=0)
    plt.legend(loc='best', prop={'size': 12})
    plt.show()


def main(args):
    file = "./logs/logs/spark logs/aggre_log"
    analysis(file)

if __name__ == "__main__":
    main(sys.argv[1:])

