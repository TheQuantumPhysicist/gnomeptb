import datetime as dt
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
import h5py



__version__ = 0.1

# default decimal class precision
decimal.getcontext().prec = 30

comb_columns_to_include = [1, 2, 3, 4, 5, 6]
cavi_columns_to_include = [0, 1, 2]


def print_error(err):
    sys.stderr.write(str(err) + "\n")
    sys.stderr.flush()


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


def get_data(workdir, cavity_subdir, comb_subdir, finished_subdir, max_queue_size = 250000):
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
        last_time = dt.datetime.now()

        # keep reading the file (and yield in the middle)
        while True:
            cavi_queue = []
            # keep reading until no lines are found or max size is reached (to prevent memory overflow)
            while len(cavi_queue) < max_queue_size:
                cavi_line = next(tail_line(fcavi))
                if cavi_line is None:
                    break
                else:
                    cavi_queue.append(cavi_line.replace("\n", ""))
                    last_time = dt.datetime.now()
            comb_queue = []
            # keep reading until no lines are found or max size is reached (to prevent memory overflow)
            while len(comb_queue) < max_queue_size:
                comb_line = next(tail_line(fcomb))
                if comb_line is None:
                    break
                else:
                    comb_queue.append(comb_line.replace("\n", ""))
                    last_time = dt.datetime.now()

            if len(cavi_queue) == 0 and len(comb_queue) == 0:
                # if no data was found for some time (=timeout_recheck_new_files), close the files, and try to move them
                if dt.datetime.now() - last_time > dt.timedelta(seconds=timeout_recheck_new_files):
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
                        shutil.copy(comb_file_path, new_comb_file_path)
                        shutil.copy(cavi_file_path, new_cavi_file_path)
                    except Exception as e:
                        # if the movement of the files failed, reopen them and try to read them further
                        try:
                            os.remove(new_comb_file_path)
                            os.remove(new_cavi_file_path)
                        except:
                            pass

                        print_error("Unable to copy files after having read them. "
                                    "Assuming the file is still being used. Exception says: " + str(e))
                        fcomb = open(comb_file_path)
                        fcavi = open(cavi_file_path)

                        # restore the last pointer position
                        fcomb.seek(fcomb_ptr)
                        fcavi.seek(fcavi_ptr)
                        continue

                    try:
                        os.remove(comb_file_path)
                        os.remove(cavi_file_path)

                    except Exception as e:
                        print_error(
                            "SEVERE ERROR: Unable to delete files after having read them. This is very dangerous, "
                            "as it may lead to the file being read more than once. Exception says: " + str(e))
                        raise

                    # break to read the next file
                    break

            # return the lines found in queues
            yield {"cavi_queue": cavi_queue, "comb_queue": comb_queue,
                   "empty": True if (len(cavi_queue) == 0 and len(comb_queue) == 0) else False}

            # if returning back from a non-empty submission of data, reset counter
            if not (len(cavi_queue) == 0 and len(comb_queue) == 0):
                last_time = dt.datetime.now()


def tail_line(file):
    """
        read last line, closes file and returns, if file is no longer the newest one
    :param file: file object
    :return:  yields the line that was read from the file
    """
    while True:
        where = file.tell()  # get current pointer position
        line = file.readline()
        if not line or line[-1] != '\n':  # if no line is found OR the line doesn't end with \n (so, incomplete line)
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
    def __init__(self, cavi_regex_str, comb_regex_str, data_output_dir, station_name):
        self.comb_queue = []
        self.cavi_queue = []
        self.comb_processed_queue = []
        self.cavi_processed_queue = []
        self.cavi_line_data = LineData(cavi_regex_str)
        self.comb_line_data = LineData(comb_regex_str)
        self.data_output_dir = data_output_dir
        self.file_writer = SingleFileData(data_output_dir, station_name)
        self.station_name = station_name

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

    def process_data(self):

        # parse lines in the queue
        self.comb_queue = [s.replace("\r", "").replace("\n", "") for s in self.comb_queue]
        self.cavi_queue = [s.replace("\r", "").replace("\n", "") for s in self.cavi_queue]
        self.comb_queue = list(filter(None, self.comb_queue))
        self.cavi_queue = list(filter(None, self.cavi_queue))

        parsed_comb_data = [0]*len(self.comb_queue)
        parsed_cavi_data = [0]*len(self.cavi_queue)
        for i in range(len(self.comb_queue)):
            try:
                self.comb_line_data.parse_line(self.comb_queue[i])
                parsed_comb_data[i] = copy.copy(self.comb_line_data)
            except re.error as e:
                print_error(str(e))
                parsed_comb_data[i] = LineData()

        for i in range(len(self.cavi_queue)):
            try:
                self.cavi_line_data.parse_line(self.cavi_queue[i])
                parsed_cavi_data[i] = copy.copy(self.cavi_line_data)
            except re.error as e:
                print_error(str(e))
                parsed_cavi_data[i] = LineData()

        self.comb_processed_queue.extend(parsed_comb_data)
        self.cavi_processed_queue.extend(parsed_cavi_data)
        self.comb_queue = []
        self.cavi_queue = []

        while True:
            # find the next sync point
            cavi_sync_point_batch_begin = None
            for i in range(len(self.cavi_processed_queue)):
                if self.cavi_processed_queue[i].success:
                    # print(self.cavi_processed_queue[i])
                    if self.cavi_processed_queue[i].sync:
                        cavi_sync_point_batch_begin = i
                        break

            # if no sync point is found, return (to get more data from text files)
            if cavi_sync_point_batch_begin is None:
                return

            # after having found a first sync point, find the next one
            cavi_sync_point_batch_end = None
            for i in range(cavi_sync_point_batch_begin+1, len(self.cavi_processed_queue)):
                if self.cavi_processed_queue[i].success:
                    if self.cavi_processed_queue[i].sync:
                        cavi_sync_point_batch_end = i  # the point past the last point
                        break
            if cavi_sync_point_batch_end is None:
                return
            # print([cavi_sync_point_batch_begin,cavi_sync_point_batch_end])
            # print(self.cavi_processed_queue[cavi_sync_point_batch_begin].time,self.cavi_processed_queue[cavi_sync_point_batch_end].time)
            # print(self.cavi_processed_queue[cavi_sync_point_batch_begin].time)
            # print(self.cavi_processed_queue[cavi_sync_point_batch_end+1].time)
            comb_sync_point_batch_begin = None
            comb_sync_point_batch_end = None
            comb_sync_point_batch_range = []
            for i in range(len(self.comb_processed_queue)):
                if self.comb_processed_queue[i].success:
                    begin = self.cavi_processed_queue[cavi_sync_point_batch_begin].time
                    tm = self.comb_processed_queue[i].time
                    end = self.cavi_processed_queue[cavi_sync_point_batch_end].time
                    if begin <= tm < end:
                        comb_sync_point_batch_range.append(i)
                        # break

            # if no corresponding points in time were found in comb data, return
            # (so that more data can be brought next time)
            if len(comb_sync_point_batch_range) == 0:
                return
            else:
                comb_sync_point_batch_begin = comb_sync_point_batch_range[0]
                comb_sync_point_batch_end = comb_sync_point_batch_range[-1]+1

            # print(cavi_sync_point_batch_begin, cavi_sync_point_batch_end)
            # print(comb_sync_point_batch_begin, comb_sync_point_batch_end)
            # print(self.cavi_processed_queue[cavi_sync_point_batch_begin].time, self.comb_processed_queue[comb_sync_point_batch_begin].time)
            # print(self.cavi_processed_queue[cavi_sync_point_batch_end].time, self.comb_processed_queue[comb_sync_point_batch_end].time)
            self.file_writer.append_batch(self.cavi_processed_queue[cavi_sync_point_batch_begin:
                                                                    cavi_sync_point_batch_end],
                                          self.comb_processed_queue[comb_sync_point_batch_begin:
                                                                    comb_sync_point_batch_end])

            # delete the parts of the queue that are used/skipped
            # the reason for starting from zero is to remove additional data that was not matched before
            # ideally, comb_sync_point_batch_begin = cavi_sync_point_batch_begin = 0
            del self.cavi_processed_queue[0:cavi_sync_point_batch_end]
            del self.comb_processed_queue[0:comb_sync_point_batch_end]


def large_round(num):
    s = str(num)
    if len(s.split(".")) == 1:
        pass
    elif len(s.split(".")) == 0:
        pass
    else:
        pass


class SingleFileData:
    max_batches = 60
    cavi_dataset_name = "CavitiesData"
    comb_dataset_name = "CombData"
    attr_data_format = "AtomicClockData_PTB"
    f_dateFormat = "%Y/%m/%dF"
    f_timeFormat = "%H:%M:%S.%f"
    comb_sample_rate = 1
    cavi_sample_rate = 1000
    Longitude = 10.461654
    Altitude = 78
    Latitude = 52.296052

    @staticmethod
    def create_normalized_list(input_list, to_include_list, prec=4, to_type=np.float64):
        """
        Create a list+offsets from 2d Decimals list
        :param input_list: input 2d list
        :param to_include_list: list of column numbers to include
        :param prec: precision to subtract
        :param to_type: type to convert to after subtracting the mean
        :return: dict with "offsets" and "array"
        """
        data = np.zeros([len(input_list), len(to_include_list)],
                             dtype=np.float64).tolist()


        for i in range(len(input_list)):
            k = 0
            for j in to_include_list:
                data[i][k] = input_list[i].data[j]
                k += 1


        # calculate offsets
        offsets = [decimal.Decimal('0')]*len(data[0])
        for i in range(len(data)):
            for j in range(len(data[i])):
                offsets[j] += decimal.Decimal(data[i][j]) # sum of a column

        offsets = list(map(lambda x: x/len(data), offsets))  # divide by the sum to calculate the mean
        offsets = list(map(lambda x: decimal.Context(prec=decimal.getcontext().prec).create_decimal(
            decimal.Context(prec=prec).create_decimal(x)), offsets))

        # subtract offsets
        for i in range(len(data)):
            for j in range(len(data[i])):
                data[i][j] -= offsets[j]

        # convert to numpy array, after having subtracted the offset
        data = np.array(data, dtype=to_type)
        offsets = np.array(offsets, dtype=to_type)
        return {"array": data, "offsets": offsets}

    def __init__(self, data_output_dir, station_name):
        self.data_output_dir = data_output_dir
        self.station_name = station_name

        self.all_data = {}
        self.num_batches = 0
        self.clear()

    def clear(self):
        self.num_batches = 0
        self.all_data = {"cavi_data": [], "comb_data": []}

    def append_batch(self, cavi_data_list, comb_data_list):
        if self.check_added_data_sanity(cavi_data_list, comb_data_list) is True:
            self.all_data["cavi_data"].extend(cavi_data_list)
            self.all_data["comb_data"].extend(comb_data_list)
            self.num_batches += 1
        else:
            self.clear()

        if self.num_batches >= 60:
            self.write_to_file()
            self.clear()

    def check_added_data_sanity(self, cavi_data_list, comb_data_list):
        if len(cavi_data_list) < SingleFileData.cavi_sample_rate:
            print_error("An error in a batch was found; the number of points for cavity data is < 1000 points")
            print_error("Raising error flag")
            return False
        else:
            return True

    def write_to_file(self):
        year   = self.all_data["cavi_data"][0].time.strftime('%Y')
        month  = self.all_data["cavi_data"][0].time.strftime('%m')
        day    = self.all_data["cavi_data"][0].time.strftime('%d')
        hour   = self.all_data["cavi_data"][0].time.strftime('%H')
        minute = self.all_data["cavi_data"][0].time.strftime('%M')
        second = self.all_data["cavi_data"][0].time.strftime('%S')

        out_dir = os.path.join(self.data_output_dir, year, month, day)
        file_name = self.station_name + "_" + hour + minute + second + ".h5"
        file_path = os.path.join(out_dir, file_name)
        mkdir_p(out_dir)


        #############################################
        # prepare data to write to file, be very careful that the data must remain of type Decimal until the offset is
        # subtracted, which is why no optimized numpy operations are used. NUMPY IS FORBIDDEN BEFORE SUBTRACTING
        #############################################

        cavi_normalized_data = SingleFileData.create_normalized_list(self.all_data["cavi_data"],
                                                                     cavi_columns_to_include)
        cavi_data = cavi_normalized_data["array"]
        cavi_offsets = cavi_normalized_data["offsets"]

        comb_normalized_data = SingleFileData.create_normalized_list(self.all_data["comb_data"],
                                                                     comb_columns_to_include)
        comb_data = comb_normalized_data["array"]
        comb_offsets = comb_normalized_data["offsets"]

        #############################################

        print("Opening file for write: " + file_path)
        try:
            hdf5file_obj = h5py.File(file_path, "w")
            print("File " + file_name + " is open successfully... writing data...")
        except Exception as e:
            print_error("File open error: " + file_path + ". Exception says: " + str(e))
            return

        hdf5file_obj.attrs["WriterVersion"] = __version__
        hdf5file_obj.attrs["LocalFileCreationTime"] = str(dt.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S_UTC"))


        cavi_ds = hdf5file_obj.create_dataset(SingleFileData.cavi_dataset_name, data=cavi_data,
                                              compression="gzip", compression_opts=9)
        cavi_ds.attrs["Date"] = self.all_data["cavi_data"][0].time.strftime(SingleFileData.f_dateFormat)
        cavi_ds.attrs["SamplingRate(Hz)"] = np.float32(SingleFileData.cavi_sample_rate)
        cavi_ds.attrs["Units"] = "Hz"
        cavi_ds.attrs["t0"] = self.all_data["cavi_data"][0].time.strftime(SingleFileData.f_timeFormat)
        cavi_ds.attrs["t1"] = (self.all_data["cavi_data"][0].time +
                               dt.timedelta(seconds=(len(cavi_data)/self.cavi_sample_rate))).\
            strftime(SingleFileData.f_timeFormat)
        cavi_ds.attrs["Longitude"] = np.float64(SingleFileData.Longitude)
        cavi_ds.attrs["Altitude"] = np.float64(SingleFileData.Altitude)
        cavi_ds.attrs["Latitude"] = np.float64(SingleFileData.Latitude)
        cavi_ds.attrs["MissingPoints"] = np.int32(self.max_batches*self.cavi_sample_rate - len(cavi_data))
        for i in range(len(cavi_offsets)):
            cavi_ds.attrs["Offset_column_"+str(i)] = cavi_offsets[i]

        comb_ds = hdf5file_obj.create_dataset(SingleFileData.comb_dataset_name, data=comb_data,
                                              compression="gzip", compression_opts=9)
        comb_ds.attrs["Date"] = self.all_data["comb_data"][0].time.strftime(SingleFileData.f_dateFormat)
        comb_ds.attrs["SamplingRate(Hz)"] = np.float32(SingleFileData.comb_sample_rate)
        comb_ds.attrs["Units"] = "Hz"
        comb_ds.attrs["t0"] = self.all_data["comb_data"][0].time.strftime(SingleFileData.f_timeFormat)
        comb_ds.attrs["t1"] = (self.all_data["comb_data"][0].time +
                               dt.timedelta(seconds=(len(comb_data)/self.comb_sample_rate))).\
            strftime(SingleFileData.f_timeFormat)
        comb_ds.attrs["Longitude"] = np.float64(SingleFileData.Longitude)
        comb_ds.attrs["Altitude"] = np.float64(SingleFileData.Altitude)
        comb_ds.attrs["Latitude"] = np.float64(SingleFileData.Latitude)
        comb_ds.attrs["MissingPoints"] = np.int32(self.max_batches*self.comb_sample_rate - len(comb_data))
        for i in range(len(comb_offsets)):
            comb_ds.attrs["Offset_column_"+str(i)] = comb_offsets[i]

        hdf5file_obj.close()
        print("Done writing file: " + file_path)


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
        self.num_data_points = 0
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

    def parse_line(self, line):
        """
        Parse a line of data, and subtract a common factor from all elements. The subtraction used Decimal to account
        for precision, so no precision is lost in subtraction
        :param line: String line to be parsed
        :return:
        """
        self.success = False
        if (self.regex_str is None) or (self.regex_comp is None):
            raise Exception("regex expression are not initialized. Use set_regex_str(str) to do it.")
        self._parse_line_regex(line)
        self._parse_date_from_parsed_line()
        self._parse_status_bits()
        self._parse_data_from_parsed_line()
        self.success = True

    def _parse_line_regex(self, line):
        self.line_str = line
        # this next line object is non-copyable! be careful when copying this class!
        self.match_obj = self.regex_comp.match(line)
        if self.match_obj == None:
            raise re.error("Failure while parsing line: " + line + " as expression of the form " + self.regex_str + ". Apparently it doesn't comply to the regex provided.")
        if self.match_obj.group("sync") == "*":
            self.sync = True
        else:
            self.sync = False

    def _parse_status_bits(self):
        try:
            self.status_bits = list(self.match_obj.group(LineData.key_flags))
        except IndexError:
            self.status_bits = ""

    def _parse_date_from_parsed_line(self):
        p = self.match_obj
        self.time = dt.datetime(year=2000 + int(p.group(LineData.key_year)),
                                      month=int(p.group(LineData.key_month)),
                                      day=int(p.group(LineData.key_day)),
                                      hour=int(p.group(LineData.key_hour)),
                                      minute=int(p.group(LineData.key_minute)),
                                      second=int(p.group(LineData.key_second)),
                                      microsecond=int(p.group(LineData.key_msecond))*1000)

    def _parse_data_from_parsed_line(self):
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

        self.num_data_points = len(line_data)
        self.data = line_data

    def __str__(self):
        """
        Convert the data of the object to a string (helps in printing)
        :return:
        """
        if self.success:
            return str({"DateTime: ": str(self.time),
                        "Line: ": self.line_str,
                        "Sync: ": self.sync,
                        "Parsed string": str(self.regex_comp.findall(self.line_str)),
                        "Data: ": str(self.data),
                        "Status bits: ": str(self.status_bits),
                        "Regex Str": str(self.regex_str)})
        else:
            return "<No parsed data>"
