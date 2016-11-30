#!/bin/bash

import sys
import argparse
import gnomeptb as ptb
import time


def main_function():
    default_comb_regex = "^(?P<day>\d{2})(?P<month>\d{2})(?P<year>\d{2})(?P<sync>(\*|\s+))(?P<hour>\d{2})(?P<min>\d{2})(?P<sec>\d{2})(?:.)(?P<msec>\d+)[(?:\s+)](?P<flags>[A-Z]{8})(?:\s+)(?P<f1>\d+\.{0,1}\d*)(?:\s+)((?P<f2>\d+\.{0,1}\d*))(?:\s+)(?P<f3>\d+\.{0,1}\d*)(?:\s+)(?P<f4>\d+\.{0,1}\d*)(?:\s+)(?P<f5>\d+\.{0,1}\d*)(?:\s+)(?P<f6>\d+\.{0,1}\d*)(?:\s+)(?P<f7>\d+\.{0,1}\d*)(?:\s+)(?P<f8>\d+\.{0,1}\d*)(?:\s+)(?P<f9>\d+\.{0,1}\d*)(?:\s+)(?P<f10>\d+\.{0,1}\d*)(?:\s+)(?P<f11>\d+\.{0,1}\d*)(?:\s+)(?P<f12>\d+\.{0,1}\d*)(?:\s+)(?P<f13>\d+\.{0,1}\d*)(?:\s+)(?P<f14>\d+\.{0,1}\d*)(?:\s+)(?P<f15>\d+\.{0,1}\d*)(?:\s+)(?P<f16>\d+\.{0,1}\d*)(?:\s+)(?P<f17>\d+\.{0,1}\d*)(?:\s+)(?P<f18>\d+\.{0,1}\d*)(?:\s+)(?P<f19>\d+\.{0,1}\d*)(?:\s+)(?P<f20>\d+\.{0,1}\d*)(?:\s+)(?P<f21>\d+\.{0,1}\d*)(?:\s*)$"
    default_cavity_regex = "^(?P<day>\d{2})(?P<month>\d{2})(?P<year>\d{2})(?P<sync>(\*|\s+))(?P<hour>\d{2})(?P<min>\d{2})(?P<sec>\d{2})(?:.)(?P<msec>\d+)(?:\s+)(?P<f1>\d+\.{0,1}\d*)(?:\s+)((?P<f2>\d+\.{0,1}\d*))(?:\s+)(?P<f3>\d+\.{0,1}\d*)(?:\s*)$"

    parser = argparse.ArgumentParser()

    parser.add_argument("-ra", "--cavityregex", dest="cavityregex", default=default_cavity_regex, help="The regular expression of the cavity data")
    parser.add_argument("-ro", "--combregex", dest="combregex", default=default_comb_regex, help="The regular expression of the comb data")
    parser.add_argument("-dw", "--workdir", dest="workdir", default="D:/ClockData", help="Working directory, where data exists")
    parser.add_argument("-da", "--cavisubdir", dest="cavitysubdir", default="Cavities", help="Sub-directory of cavities data")
    parser.add_argument("-do", "--combsubdir", dest="combsubdir", default="Comb", help="Sub-directory of comb data")
    parser.add_argument("-df", "--finishedsubdir", dest="finishedsubdir", default="finished", help="Sub-directory of comb data")

    args = parser.parse_args()

    finished_subdir = args.finishedsubdir

    ptb.LineData.set_decimal_precision(30)
    col = ptb.DataCollection(args.cavityregex, args.combregex)
    for data_queues in ptb.get_data(args.workdir,args.cavitysubdir,args.combsubdir,finished_subdir):
        if not data_queues["empty"]:
            # print(data_queues)
            col.append_cavi_data(data_queues["cavi_queue"])
            col.append_comb_data(data_queues["comb_queue"])
            col.process_data()

        else:
            print("No new data found...")
            time.sleep(1)

    regex_exp_comb = args.combregex
    regex_exp_cavi = args.cavityregex

    # # line = "150305 080643.211 FFFFFFFE  10000000.12179999980  63381998.56613390150      4057.90040000000  27003124.14354269950     0.0000000"
    # linedata_comb = ptb.LineData(regex_exp_comb)
    # linedata_cavi = ptb.LineData(regex_exp_cavi)
    # # linedata.parse_line(line)
    # # print(linedata)
    #
    # content = read_file(r"C:\Users\Samer\Desktop\tmp2\freqs.txt")
    # content = content.replace("\r","").split("\n")
    # content = list(filter(None, content))
    # res=[0]*len(content)
    # for i in range(len(content)):
    #     try:
    #         linedata_comb.parse_line(content[i])
    #         res[i] = copy.copy(linedata_comb)
    #     except re.error:
    #         res[i] = ptb.LineData()
    # # print(res)
    # [print(v) for v in res if v.success]


if __name__ == '__main__':
    main_function()
    sys.exit(0)
