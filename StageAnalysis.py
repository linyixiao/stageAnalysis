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

    stageTime = []
    execTime = [0, 0, 0, 0, 0]
    stageRunTime = [0, 0, 0, 0, 0]
    gcTime = [0, 0, 0, 0, 0]
    for line in f:
        if is_json:
            json_data = get_json(line)
            event_type = json_data["Event"]

            if event_type == "SparkListenerTaskEnd":
                stage_id = json_data["Stage ID"]
                task_metrics = json_data["Task Metrics"]
                task_info = json_data["Task Info"]

                if stage_id == 0:
                    execTime[0] += task_metrics["Executor Run Time"]
                    gcTime[0] += task_metrics["JVM GC Time"]
                    stageRunTime[0] += int(task_info["Finish Time"]-task_info["Launch Time"])
                elif stage_id == 1:
                    execTime[1] += task_metrics["Executor Run Time"]
                    gcTime[1] += task_metrics["JVM GC Time"]
                    stageRunTime[1] += int(task_info["Finish Time"]-task_info["Launch Time"])
                elif stage_id == 2:
                    execTime[2] += task_metrics["Executor Run Time"]
                    gcTime[2] += task_metrics["JVM GC Time"]
                    stageRunTime[2] += int(task_info["Finish Time"]-task_info["Launch Time"])
                elif stage_id == 3:
                    execTime[3] += task_metrics["Executor Run Time"]
                    gcTime[3] += task_metrics["JVM GC Time"]
                    stageRunTime[3] += int(task_info["Finish Time"]-task_info["Launch Time"])
                elif stage_id == 4:
                    execTime[4] += task_metrics["Executor Run Time"]
                    gcTime[4] += task_metrics["JVM GC Time"]
                    stageRunTime[4] += int(task_info["Finish Time"]-task_info["Launch Time"])
                else:
                    pass

            elif event_type == "SparkListenerStageCompleted":
                stage_infos = json_data["Stage Info"]
                stageTime.append(stage_infos["Completion Time"]-stage_infos["Submission Time"])
            else:
                pass

    print ("JVM GC Time: %s" % gcTime)
    print "Executor Run Time %s" % execTime
    print "Stage Run Time %s" % stageRunTime
    #print stageTime

    gcTimeRatio = map(lambda(a, b): 100*float(a)/float(b), zip(gcTime, stageRunTime))
    execTimeRatio = map(lambda(a, b): 100*float(a)/float(b), zip(execTime, stageRunTime))
    print "JVM GC Time Ratio:%s" % gcTimeRatio
    print "Executor Run Time Ratio:%s" % execTimeRatio

    stageID =["stage0", "stage1", "stage2", "stage3", "stage4"]
    #plt.rc('font', family='SimHei', size=13)
    num1 = np.array(gcTimeRatio)
    num2 = np.array(execTimeRatio)
    width = 0.6
    idx = np.arange(len(stageID))
    plt.bar(idx, num1,  width, bottom=num2, color='red', label='GCtime ratio')
    plt.bar(idx, num2, width,  color='blue', label='execTime ratio')
    plt.xlabel("stage ID")
    plt.ylabel("Time Ratio(%)")
    plt.ylim(0, 100)
    plt.xticks(idx+width/2, stageID, rotation=40)
    plt.legend(loc='best', prop={'size': 12})
    plt.show()


def main(args):
    file = "./logs/logs/spark logs/aggre_log"
    analysis(file)

if __name__ == "__main__":
    main(sys.argv[1:])
