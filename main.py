#!/bin/bash

import sys
import argparse
import gnomeptb as ptb
import time
import ast

def main_function():
    default_comb_regex = "^(?P<year>\d{2})(?P<month>\d{2})(?P<day>\d{2})(?P<sync>(\*|\s+))(?P<hour>\d{2})(?P<min>\d{2})(?P<sec>\d{2})(?:.)(?P<msec>\d+)[(?:\s+)](?P<flags>[A-Z]{8})(?:\s+)(?P<f1>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)((?P<f2>(\-\d+|\d+)\.{0,1}\d*))(?:\s+)(?P<f3>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f4>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f5>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f6>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f7>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f8>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f9>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f10>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f11>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f12>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f13>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f14>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f15>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f16>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f17>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f18>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f19>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f20>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)(?P<f21>(\-\d+|\d+)\.{0,1}\d*)(?:\s*)$"
    default_cavity_regex = "^(?P<year>\d{2})(?P<month>\d{2})(?P<day>\d{2})(?P<sync>(\*|\s+))(?P<hour>\d{2})(?P<min>\d{2})(?P<sec>\d{2})(?:.)(?P<msec>\d+)(?:\s+)(?P<f1>(\-\d+|\d+)\.{0,1}\d*)(?:\s+)((?P<f2>(\-\d+|\d+)\.{0,1}\d*))(?:\s+)(?P<f3>(\-\d+|\d+)\.{0,1}\d*)(?:\s*)$"

    parser = argparse.ArgumentParser()

    parser.add_argument("-ra", "--cavityregex", dest="cavityregex", default=default_cavity_regex, help="The regular expression of the cavity data")
    parser.add_argument("-ro", "--combregex", dest="combregex", default=default_comb_regex, help="The regular expression of the comb data")
    parser.add_argument("-dw", "--workdir", dest="workdir", default="D:/ClockData", help="Working directory, where data exists")
    parser.add_argument("-da", "--cavisubdir", dest="cavitysubdir", default="Cavities", help="Sub-directory of cavities data")
    parser.add_argument("-dm", "--combsubdir", dest="combsubdir", default="Comb", help="Sub-directory of comb data")
    parser.add_argument("-df", "--finishedsubdir", dest="finishedsubdir", default="finished", help="")
    parser.add_argument("-do", "--outputdir", dest="outputdir", default="output", help="Output directory of files to be uploaded (HDF5 files)")
    parser.add_argument("-sn", "--stationname", dest="stationname", default="ptb01", help="Station name")
    parser.add_argument("-ca", "--cavicolumns", dest="cavicolumns", default="[0, 1, 2]", help="Cavities data columns to include in the output data file")
    parser.add_argument("-cm", "--combcolumns", dest="combcolumns", default="[1, 2, 3, 4, 5, 6]", help="Comb data columns to include in the output data file")
    parser.add_argument("-eq", "--equations", dest="equations", default='CombData[[1]]+CombData[[0]]/2["First Var",Hz]::CavitiesData[[2]]/3+CavitiesData[[0]]["Second Var",Hz]', help="Main Equation to embed in the HDF5 file")

    args = parser.parse_args()

    # args.outputdir = "D:/gnomeclock/"

    ptb.SingleFileData.SetMainEquations(args.equations)
    # columns to include in the output file
    ptb.cavi_columns_to_include = ast.literal_eval(args.cavicolumns)
    ptb.comb_columns_to_include = ast.literal_eval(args.combcolumns)

    ptb.LineData.set_decimal_precision(30)
    col = ptb.DataCollection(args.cavityregex, args.combregex, args.outputdir, args.stationname)
    for data_queues in ptb.get_data(args.workdir, args.cavitysubdir, args.combsubdir, args.finishedsubdir):
        if not data_queues["empty"]:
            # print(data_queues)
            col.append_cavi_data(data_queues["cavi_queue"])
            col.append_comb_data(data_queues["comb_queue"])
            col.process_data()

        else:
            print("No new data found...")
            time.sleep(1)

if __name__ == '__main__':
    main_function()
    sys.exit(0)
