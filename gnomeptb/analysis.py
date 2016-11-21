import datetime
import re
import decimal
import numpy as np
import copy
import time
import glob
import os
import sys
import shutil
import errno


# default decimal class precision
decimal.getcontext().prec = 30


def print_error(err):
    sys.stderr.write(str(err) + "\n")


def check_files(workdir, cavity_subdir, comb_subdir):
    """
    Checks for available data files in cavities and comb subdirectories
    It starts by looking in cavities, and then tries to find the equivalent comb file
    On failure, it keeps waiting until a file is available
    :param workdir: the working absolute dir (the one that contains the comb and cavities directries)
    :param cavity_subdir: sub-directory of cavities data
    :param comb_subdir: sub-directory of comb data
    :return: a dict that contains the files found
    """

    # try to find cavities files
    cavi_files_paths = glob.glob(os.path.join(workdir, cavity_subdir, '*.txt'))
    while len(cavi_files_paths) == 0:
        cavi_files_paths = glob.glob(os.path.join(workdir, cavity_subdir, '*.txt'))
        to_wait = 5  # seconds
        print_error("Unable to find any cavity files... trying again in " + str(to_wait) + " seconds")
        time.sleep(to_wait)
    chosen_cavi_file = cavi_files_paths[0]

    # matching comb file will have 6 chars of time then "anything"
    cavity_file_match = os.path.basename(chosen_cavi_file)[:6]+'*.txt'

    # try to find corresponding comb file
    comb_files_paths = glob.glob(os.path.join(workdir, comb_subdir, cavity_file_match))
    while len(comb_files_paths) == 0:
        comb_files_paths = glob.glob(os.path.join(workdir, comb_subdir, cavity_file_match))
        to_wait = 5  # seconds
        print_error("Unable to find comb files that match " + cavity_file_match + "... trying again in " + str(to_wait) + " seconds.")
        time.sleep(to_wait)
    chosen_comb_file = comb_files_paths[0]
    return {"comb_file": chosen_comb_file, "cavity_file": chosen_cavi_file,
            "num_comb_files": len(comb_files_paths), "num_cavity_files": len(cavi_files_paths)}


def get_data(workdir, cavity_subdir, comb_subdir, finished_subdir):
    """
    a generator of all the available data from both the comb and cavities files
    :param workdir: the working absolute dir (the one that contains the comb and cavities directries)
    :param cavity_subdir: sub-directory of cavities data
    :param comb_subdir: sub-directory of comb data
    :param finished_subdir: the sub-directory, to which files has to be moved after it's processed
    :return: a generator of a dict, whose values are lists of the data available in the file until now
    """

    #
    while True:
        # get the first available, equivalent files (time-wise)
        files = check_files(workdir, cavity_subdir, comb_subdir)
        comb_file_path = files["comb_file"]
        cavi_file_path = files["cavity_file"]

        # open the files
        fcomb = open(comb_file_path)
        fcavi = open(cavi_file_path)

        # time to wait, before giving up that no new data will be added to the current file
        timeout_recheck_new_files = 30

        # last time I found something in the files
        last_time = datetime.datetime.now()

        # keep reading the file (and yield in the middle)
        while True:
            cavi_queue = []
            # keep reading until no lines are found
            while True:
                cavi_line = next(tail_line(fcavi))
                if cavi_line is None:
                    break
                else:
                    cavi_queue.append(cavi_line.replace("\n", ""))
                    last_time = datetime.datetime.now()

            comb_queue = []
            # keep reading until no lines are found
            while True:
                comb_line = next(tail_line(fcomb))
                if comb_line is None:
                    break
                else:
                    comb_queue.append(comb_line.replace("\n", ""))
                    last_time = datetime.datetime.now()

            # return the lines found in queues
            yield {"cavi_queue": cavi_queue, "comb_queue": comb_queue,
                   "empty": True if (len(cavi_queue) == 0 and len(comb_queue) == 0) else False}

            # if no data was found for some time (=timeout_recheck_new_files), close the files, and try to move them
            if datetime.datetime.now() - last_time > datetime.timedelta(seconds=timeout_recheck_new_files):
                # keep record of the last position
                fcomb_ptr = fcomb.tell()
                fcavi_ptr = fcavi.tell()
                fcomb.close()
                fcavi.close()

                # prepare the "finished" sub-directory
                new_comb_dir = os.path.join(os.path.dirname(comb_file_path), finished_subdir)
                mkdir_p(new_comb_dir)
                new_comb_file_path = os.path.join(new_comb_dir, os.path.basename(comb_file_path))

                new_cavi_dir = os.path.join(os.path.dirname(cavi_file_path), finished_subdir)
                mkdir_p(new_cavi_dir)
                new_cavi_file_path = os.path.join(new_cavi_dir, os.path.basename(cavi_file_path))

                # move the files to the "finished" sub-directory
                try:
                    shutil.move(comb_file_path, new_comb_file_path)
                    shutil.move(cavi_file_path, new_cavi_file_path)
                except Exception as e:
                    # if the movement of the files failed, reopen them and try to read them further
                    print_error("Unable to move file. Assuming the file is still being used. Exception says: " + str(e))
                    fcomb = open(comb_file_path)
                    fcavi = open(cavi_file_path)

                    # restore the last pointer position
                    fcomb.seek(fcomb_ptr)
                    fcavi.seek(fcavi_ptr)
                    continue

                # break to read the next file
                break


def tail_line(file):        # read last line, closes file and returns, if file is no longer the newest one
    line = file.readline()
    while True:
        where = file.tell()  # get current pointer position
        line = file.readline()
        if not line or line[-1] != '\n':  # if no line is found OR the line doesn't end with \n
            time.sleep(10e-3)
            file.seek(where)
            yield None
        else:
            yield line


def mkdir_p(path):
    """
    Create dir incrementally, and be tolerant if it already exists
    :param path: directory to create
    :return: None
    """
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


class DataCollection:
    """
    A class that takes lines of data, and processes them and writes them to HDF5 files
    """
    def __init__(self):
        self.comb_queue = []
        self.cavi_queue = []

    def append_comb_data(self, data):
        if type(data) == list:
            self.comb_queue.extend(data)

        else:
            self.comb_queue.append(data)

    def append_cavi_data(self, data):
        if type(data) == list:
            self.cavi_queue.extend(data)

        else:
            self.cavi_queue.append(data)

    def _verify_comb_data(self):
        pass

    def _verify_cavi_data(self):
        pass


class LineData:
    """
        A class whose object holds the information of a single line of a data file
        On construction, the regex expression has to be provided
        The class can be used again for parsing multiple lines
    """

    # following are regex keywords for groups
    key_year = "year"
    key_month = "month"
    key_day = "day"
    key_hour = "hour"
    key_minute = "min"
    key_second = "sec"
    key_msecond = "msec"
    key_flags = "flags"

    @staticmethod
    def set_decimal_precision(precision):
        """
        Set the precision of the Decimal class
        :param precision: precision to be set
        :return:
        """
        decimal.getcontext().prec = precision

    def __init__(self, regex_str=""):
        """
        Constructor of the parser function
        :param regex_str: String that represents the regex of the parser
        """
        if regex_str == "":
            self.regex_str = None
            self.regex_comp = None
            self._init_empty()

        else:
            self.set_regex_str(regex_str)
            self._init_empty()

    def set_regex_str(self, regex_str):
        """
        Initializes the regex formula for the class
        :param regex_str: regex expression to set
        :return: None
        """
        self.regex_str = regex_str
        self.regex_comp = re.compile(regex_str)

    def _init_empty(self):
        """
        Initializes empty containers to be set on parsing
        :return:
        """
        self.success = False
        self.time = None
        self.data = None
        self.sync = None  # true or false
        self.parsed_str = None
        self.status_bits = None

    def __deepcopy__(self, memo):
        """
        Reimplementation of the __deepcopy__ method to avoid copying the non-copyable self.match_obj
        :param memo:
        :return:
        """
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k.startswith("match_ob") or k.startswith("regex_"):
                continue
            setattr(result, k, copy.deepcopy(v, memo))
        return result

    def parse_line(self, line, subtract_from_data=1e7):
        """
        Parse a line of data, and subtract a common factor from all elements. The subtraction used Decimal to account
        for precision, so no precision is lost in subtraction
        :param line: String line to be parsed
        :param subtract_from_data: value to be subtracted, or list of values to be subtracted
        :return:
        """
        self.success = False
        if (self.regex_str is None) or (self.regex_comp is None):
            raise Exception("regex expression are not initialized. Use set_regex_str(str) to do it.")
        self._parse_line_regex(line)
        self._parse_date_from_parsed_line()
        self._parse_status_bits()
        self._parse_data_from_parsed_line(subtract_from_data)
        self.success = True

    def _parse_line_regex(self, line):
        self.line_str = line
        # this next line object is non-copyable! be careful when copying this class!
        self.match_obj = self.regex_comp.match(line)
        if self.match_obj == None:
            raise re.error("Failure while parsing line: " + line + ". Apparently it doesn't comply to the regex provided.")
        if self.match_obj.group("sync") == "*":
            self.sync = True
        else:
            self.sync = False

    def _parse_status_bits(self):
        self.status_bits = list(self.match_obj.group(LineData.key_flags))

    def _parse_date_from_parsed_line(self):
        p = self.match_obj
        self.time = datetime.datetime(year=2000 + int(p.group(LineData.key_year)),
                                      month=int(p.group(LineData.key_month)),
                                      day=int(p.group(LineData.key_day)),
                                      hour=int(p.group(LineData.key_hour)),
                                      minute=int(p.group(LineData.key_minute)),
                                      second=int(p.group(LineData.key_second)),
                                      microsecond=int(p.group(LineData.key_msecond))*1000)

    def _parse_data_from_parsed_line(self, common_to_subtract):
        i = int(1)
        line_data = []
        # try to capture every group with name f+number
        while True:
            # if True:
            try:
                line_data.append(decimal.Decimal(self.match_obj.group("f" + str(i))))
            except IndexError:
                break
            i += 1

        # subtract the common factor to reduce required precision
        # the subtraction is done through the Decimal type (arbitrary precision type)
        if type(common_to_subtract) is list:
            line_data = [line_data[i] - decimal.Decimal(common_to_subtract[i]) for i in range(len(common_to_subtract))]
        else:
            line_data = list(map(lambda v: v - decimal.Decimal(common_to_subtract), line_data))

        self.data = list(map(np.double, line_data))

    def __str__(self):
        """
        Convert the data of the object to a string (helps in printing)
        :return:
        """
        return str({"DateTime: ": str(self.time),
                    "Sync: ": self.sync,
                    "Parsed string": str(self.regex_comp.findall(self.line_str)),
                    "Data: ": str(self.data),
                    "Status bits: ": str(self.status_bits),
                    "Regex Str": str(self.regex_str)})
