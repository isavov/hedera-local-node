# Standard Package Imports

import argparse
import calendar
import hashlib
import logging
import logging.handlers
import os
import os.path as path
import shutil
import sys
import time
import distutils
import distutils.util

from time import time_ns

from boto3.session import Session
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from botocore.handlers import set_list_objects_encoding_type_url
from botocore.exceptions import ClientError

from operator import methodcaller
from threading import Thread

from watchdog.events import RegexMatchingEventHandler
from watchdog.observers import Observer

from appmetrics import metrics, reporter


class Application:
    __version = None
    __mirror_version = None
    __reaper_version = None

    __mirror = None
    __reaper = None

    __parser = None
    __arguments = None
    __file_log_handler = None
    __syslog_log_handler = None
    __stderr_log_handler = None
    __stdout_log_handler = None
    __syslog_log_formatter = None
    __console_log_formatter = None
    __file_log_formatter = None
    __file_log_enabled = False
    __syslog_log_enabled = False

    __loggers = dict()
    __logger = None
    __console_logger = None

    __monitored_path = None

    __gcs_storage = None
    __s3_storage = None

    __running = True
    __initialized = False

    __stats_instances = dict()

    def __init__(self):
        self.__version = Version(1, 2, 0)
        self.__mirror_version = Version(1, 2, 12)
        self.__reaper_version = Version(1, 0, 6)

        self.__parser = argparse.ArgumentParser(description="Mirror a directory to AWS S3, GCS, or both")
        self.__parser.formatter_class = lambda prog: argparse.HelpFormatter(prog,
                                                                            max_help_position=max(120,
                                                                                                  shutil.get_terminal_size().columns),
                                                                            width=max(120,
                                                                                      shutil.get_terminal_size().columns))
        for ln in ("Application", "Mirror", "Reaper", "RemoteStorage"):
            logger = logging.getLogger(ln)
            self.__loggers[ln] = logger

        self.__logger = logging.getLogger("Application")
        self.__console_logger = logging.getLogger("Console")

    def __init_logging(self):
        self.__console_logger.setLevel(logging.INFO)

        self.__stderr_log_handler = logging.StreamHandler(sys.stderr)
        self.__stderr_log_handler.setLevel(logging.ERROR)

        self.__stdout_log_handler = logging.StreamHandler(sys.stdout)
        self.__stdout_log_handler.setLevel(logging.DEBUG)

        self.__file_log_formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)-8s | %(filename)-10s | line %(lineno)03d | %(message)s ")
        self.__file_log_formatter.converter = time.gmtime

        self.__syslog_log_formatter = logging.Formatter(
            "%(name)-8s | %(filename)-10s | line %(lineno)03d | %(message)s ")
        self.__syslog_log_formatter.converter = time.gmtime

        # self.__console_log_formatter = logging.Formatter(
        #     "%(asctime)s: %(levelname)8s --- %(message)s ")
        self.__console_log_formatter = logging.Formatter(
            "%(message)s ")
        self.__console_log_formatter.converter = time.gmtime

        self.__stderr_log_handler.setFormatter(self.__console_log_formatter)
        self.__stdout_log_handler.setFormatter(self.__console_log_formatter)

        def stdout_filter(record):
            if record.levelno < self.__stderr_log_handler.level:
                return True

            return False

        def stderr_filter(record):
            if record.levelno > self.__stdout_log_handler.level:
                return True

            return False

        self.__stdout_log_handler.addFilter(stdout_filter)
        self.__stderr_log_handler.addFilter(stderr_filter)

        self.__console_logger.addHandler(self.__stderr_log_handler)
        self.__console_logger.addHandler(self.__stdout_log_handler)

        for logger in self.__loggers.values():
            logger.setLevel(logging.INFO)
            logger.addHandler(self.__stderr_log_handler)
            logger.addHandler(self.__stdout_log_handler)

    def __init_cli_support(self):
        self.__parser.add_argument("--version", action="version", version=self.__get_version_info())
        self.__parser.add_argument("--debug", action="store_true", required=False,
                                   help="include debug logging in the output", default=bool(distutils.util.strtobool(os.environ.get('DEBUG'))))
        self.__parser.add_argument("--macosx", action="store_true", required=False,
                                   help="enable MacOS X specific supports")
        self.__parser.add_argument("--linux", action="store_true", required=False,
                                   help="enable Linux specific supports")
        self.__parser.add_argument("--enable-reaper", action="store_true", required=False,
                                   help="enable the reaper to collect files already uploaded", default=bool(distutils.util.strtobool(os.environ.get('REAPER_ENABLE'))))
        self.__parser.add_argument("--reaper-min-keep", action="store", required=False, type=int,
                                   help="keep at least this many files (based on newest mtime) in the watched directory", default=os.environ.get('REAPER_MIN_KEEP'))
        self.__parser.add_argument("--reaper-interval", action="store", required=False, type=float,
                                   help="enable the reaper to collect files already uploaded", default=os.environ.get('REAPER_INTERVAL'))#default="60.0")
        self.__parser.add_argument("--s3", action="store_true", required=False, help="enable mirroring to AWS S3", default=bool(distutils.util.strtobool(os.environ.get('S3_ENABLE'))))
        self.__parser.add_argument("--gcs", action="store_true", required=False, help="enable mirroring to GCS", default=bool(distutils.util.strtobool(os.environ.get('GCS_ENABLE'))))
        self.__parser.add_argument("--require-signature", action="store_true", required=False,
                                   help="wait for the signature before mirroring the file", default=bool(distutils.util.strtobool(os.environ.get('SIG_REQUIRE'))))
        self.__parser.add_argument("--signature-extension", action="store", required=False,
                                   help="the file extension of an associated signature", metavar="<ext>", default=os.environ.get('SIG_EXTENSION'))
        self.__parser.add_argument("--prioritize-signatures", action="store_true", required=False,
                                   help="enables prioritization of signature files by ensuring that they are uploaded"
                                        + " before the record files", default=bool(distutils.util.strtobool(os.environ.get('SIG_PRIORITIZE'))))
        self.__parser.add_argument("--log-file", action="store", help="log file to which all output is directed",
                                   metavar="<file>")
        self.__parser.add_argument("--syslog", action="store_true", help="enable syslog output for most logging")
        # self.__parser.add_argument("--rclone-path", action="store", help="path to the rclone executable",
        #                            metavar="<file>", default="/usr/bin/rclone")
        # self.__parser.add_argument("--s3-remote", action="store", help="the rclone remote to use for AWS S3",
        #                            metavar="<name>", default="amazons3")
        # self.__parser.add_argument("--gcs-remote", action="store", help="the rclone remote to use for GCS",
        #                            metavar="<name>", default="gcs")
        self.__parser.add_argument("--s3-access-key", action="store", required=False,
                                   help="the access key to use for AWS s3", metavar="<region>", default=os.environ.get('S3_ACCESS_KEY'))
        self.__parser.add_argument("--s3-secret-key", action="store", required=False,
                                   help="the secret key to use for AWS s3", metavar="<region>", default=os.environ.get('S3_SECRET_KEY'))
        self.__parser.add_argument("--gcs-access-key", action="store", required=False,
                                   help="the access key to use for GCS", metavar="<region>", default=os.environ.get('GCS_ACCESS_KEY'))
        self.__parser.add_argument("--gcs-secret-key", action="store", required=False,
                                   help="the secret key to use for GCS", metavar="<region>", default=os.environ.get('GCS_SECRET_KEY'))
        self.__parser.add_argument("--s3-endpoint", action="store", required=False,
                                   help="the endpoint URL to use for AWS s3",
                                   metavar="<region>", default="https://s3.amazonaws.com")
        self.__parser.add_argument("--gcs-endpoint", action="store", required=False,
                                   help="the endpoint URL to use for GCS",
                                   metavar="<region>", default="https://storage.googleapis.com")
        self.__parser.add_argument("--s3-region", action="store", required=False, help="the region to use for AWS s3",
                                   metavar="<region>", default="us-east-1")
        self.__parser.add_argument("--gcs-region", action="store", required=False, help="the region to use for GCS",
                                   metavar="<region>", default="us-east1")
        self.__parser.add_argument("--pid-file", action="store", help="write my PID to the given file",
                                   metavar="<file>")
        self.__parser.add_argument("--watch-directory", action="store", required=True,
                                   help="local path to monitor for new files", metavar="<path>")
        self.__parser.add_argument("--bucket-name", action="store", required=False,
                                   help="bucket name to use for both S3 and GCS", metavar="<name>", default=os.environ.get('BUCKET_NAME'))
        self.__parser.add_argument("--s3-bucket-name", action="store", required=False,
                                   help="bucket name to use for S3 which overrides the standard bucket name",
                                   metavar="<name>")
        self.__parser.add_argument("--gcs-bucket-name", action="store", required=False,
                                   help="bucket name to use for GCS which overrides the standard bucket name",
                                   metavar="<name>")
        self.__parser.add_argument("--bucket-path", action="store", required=False,
                                   help="bucket path where files should be stored for both S3 and GCS",
                                   metavar="<path>", default=os.environ.get('BUCKET_PATH'))
        self.__parser.add_argument("--s3-bucket-path", action="store", required=False,
                                   help="bucket path where files should be stored for S3 which overrides the standard bucket path",
                                   metavar="<path>")
        self.__parser.add_argument("--gcs-bucket-path", action="store", required=False,
                                   help="bucket path where files should be stored for GCS which overrides the standard bucket path",
                                   metavar="<path>")
        self.__parser.add_argument("--record-extension", action="store", required=False,
                                   help="the file extension of the files to be uploaded", metavar="<ext>", default=os.environ.get('STREAM_EXTENSION'))
        self.__parser.add_argument("--csv-stats-directory", action="store", required=False,
                                   help="write CSV statistics to the specified directory", metavar="<csv_stats_dir>")

        self.__arguments = self.__parser.parse_args()

        if self.__arguments.log_file is not None:
            self.__enable_file_logging(self.__arguments.log_file)

        if self.__arguments.syslog:
            self.__enable_syslog_logging()

        if self.__arguments.debug:
            self.__enable_debug_logging()

        if self.__arguments.pid_file is not None:
            self.__write_pid_file(self.__arguments.pid_file)

        if not path.exists(self.__arguments.watch_directory) or not path.isdir(self.__arguments.watch_directory):
            self.__console_logger.error("Watch directory does not exist: %s", self.__arguments.watch_directory)
            time.sleep(0.1)
            self.__parser.print_help()
            sys.exit(1)

        if not self.__arguments.gcs and not self.__arguments.s3:
            self.__console_logger.error("Must specify at least one mirror destination: S3 or GCS")
            time.sleep(0.1)
            self.__parser.print_help()
            sys.exit(1)

        if self.__arguments.require_signature and self.__arguments.signature_extension is None:
            self.__console_logger.error("Signature extension must be provided when require signatures is enabled")
            time.sleep(0.1)
            self.__parser.print_help()
            sys.exit(1)

        # if not path.exists(self.__arguments.rclone_path) or not path.isfile(self.__arguments.rclone_path):
        #     self.__console_logger.error("RClone executable not found or not functional: %s",
        #                                 self.__arguments.rclone_path)
        #     time.sleep(0.1)
        #     self.__parser.print_help()
        #     sys.exit(1)

        self.__monitored_path = path.realpath(self.__arguments.watch_directory)

    def __enable_syslog_logging(self):
        if self.__arguments.macosx:
            self.__syslog_log_handler = logging.handlers.SysLogHandler(
                facility=logging.handlers.SysLogHandler.LOG_LOCAL1, address='/var/run/syslog')
        else:
            self.__syslog_log_handler = logging.handlers.SysLogHandler(
                facility=logging.handlers.SysLogHandler.LOG_DAEMON)

        self.__syslog_log_handler.setFormatter(self.__syslog_log_formatter)
        self.__syslog_log_handler.setLevel(logging.DEBUG)

        for logger in self.__loggers.values():
            logger.removeHandler(self.__stdout_log_handler)
            logger.removeHandler(self.__stderr_log_handler)
            logger.addHandler(self.__syslog_log_handler)

        self.__syslog_log_enabled = True

    def __enable_debug_logging(self):
        for logger in self.__loggers.values():
            logger.setLevel(logging.DEBUG)

        self.__console_logger.setLevel(logging.DEBUG)

    def __enable_file_logging(self, file):
        self.__file_log_handler = logging.handlers.RotatingFileHandler(file, maxBytes=104857600, backupCount=500)
        self.__file_log_handler.setFormatter(self.__file_log_formatter)
        self.__file_log_handler.setLevel(logging.DEBUG)

        for logger in self.__loggers.values():
            logger.removeHandler(self.__stdout_log_handler)
            logger.removeHandler(self.__stderr_log_handler)
            logger.addHandler(self.__file_log_handler)

        self.__file_log_enabled = True

    def __write_pid_file(self, file):
        fh = None
        try:

            if path.exists(file):
                os.unlink(file)

            pid = os.getpid()
            fh = open(file, mode='w')
            fh.write(str.format("{}", pid))
            fh.flush()
            fh.close()
            return True
        except Exception:
            self.__logger.exception("Process Identifier File Exception [ pid_file = '%s' ]", file)
            return False
        finally:
            if fh is not None:
                try:
                    fh.close()
                except Exception:
                    return True

    def __get_version_info(self):
        return "%(prog)s " + self.__version.to_version_string("") + " (" + self.__version.to_version_string(
            "Application Core") + ", " + self.__reaper_version.to_version_string(
            "Reaper") + ", " + self.__mirror_version.to_version_string("Mirror") + ")"

    def debug(self):
        return self.__arguments.debug

    def pid_file(self):
        return self.__arguments.pid_file

    def csv_stats_directory(self):
        return self.__arguments.csv_stats_directory

    def is_linux_environment(self):
        return self.__arguments.linux

    def is_macosx_environment(self):
        return self.__arguments.macosx

    def file_logging_enabled(self):
        return self.__file_log_enabled

    def syslog_logging_enabled(self):
        return self.__syslog_log_enabled

    # def rclone_path(self):
    #     return self.__arguments.rclone_path

    def monitored_path(self):
        return self.__monitored_path

    def record_extension(self):
        return self.__arguments.record_extension

    def signature_extension(self):
        return self.__arguments.signature_extension

    def require_signature(self):
        return self.__arguments.require_signature

    def prioritize_signatures(self):
        return self.__arguments.prioritize_signatures

    def is_s3_active(self):
        return self.__arguments.s3

    def is_gcs_active(self):
        return self.__arguments.gcs

    def s3_access_key(self):
        return self.__arguments.s3_access_key

    def s3_secret_key(self):
        return self.__arguments.s3_secret_key

    def gcs_access_key(self):
        return self.__arguments.gcs_access_key

    def gcs_secret_key(self):
        return self.__arguments.gcs_secret_key

    # def s3_remote(self):
    #     return self.__arguments.s3_remote
    #
    # def gcs_remote(self):
    #     return self.__arguments.gcs_remote

    def s3_region(self):
        return self.__arguments.s3_region

    def gcs_region(self):
        return self.__arguments.gcs_region

    def s3_endpoint(self):
        return self.__arguments.s3_endpoint

    def gcs_endpoint(self):
        return self.__arguments.gcs_endpoint

    def s3_bucket_name(self):
        name = self.__arguments.s3_bucket_name
        if name is not None and name.strip() != "":
            return name

        return self.__arguments.bucket_name

    def s3_bucket_path(self):
        path = self.__arguments.s3_bucket_path
        if path is not None and path.strip() != "":
            return path

        return self.__arguments.bucket_path

    def gcs_bucket_name(self):
        name = self.__arguments.gcs_bucket_name
        if name is not None and name.strip() != "":
            return name

        return self.__arguments.bucket_name

    def gcs_bucket_path(self):
        path = self.__arguments.gcs_bucket_path
        if path is not None and path.strip() != "":
            return path

        return self.__arguments.bucket_path

    def s3_storage(self):
        return self.__s3_storage

    def gcs_storage(self):
        return self.__gcs_storage

    def enable_reaper(self):
        return self.__arguments.enable_reaper

    def reaper_interval(self):
        return self.__arguments.reaper_interval

    def reaper_min_keep(self):
        return self.__arguments.reaper_min_keep

    def get_proxy_support(self, logger):
        http_proxy = os.getenv('http_proxy', os.getenv('HTTP_PROXY', None))
        https_proxy = os.getenv('https_proxy', os.getenv('HTTPS_PROXY', None))

        logger.info("Network Proxy Information [ http_proxy = '%s', https_proxy = '%s' ]", http_proxy, https_proxy)

        proxies = {}

        if http_proxy is None and https_proxy is None:
            return None

        if http_proxy is not None:
            proxies['http'] = http_proxy

        if https_proxy is not None:
            proxies['https'] = https_proxy

        return proxies

    def local_hash(self, logger, filename):
        try:
            alg = hashlib.md5()
            with open(path.join(self.monitored_path(), filename), 'rb') as fh:
                while True:
                    data = fh.read(65536)
                    if not data:
                        break
                    alg.update(data)

            return alg.hexdigest()
        except Exception:
            logger.exception("Local Hash Computation Error [ filename = '%s' ]", filename)
            return None

    def cloud_copy(self, logger, storage, remote_path, filename):
        start_time = None
        upload_end_time = None
        verify_start_time = None

        if self.debug():
            start_time = time_ns()

        try:
            logger.debug(
                "Cloud Copy Initiated [ service = '%s', bucket = '%s', remote_path = '%s', filename = '%s' ]",
                storage.service_name(), storage.bucket_name(), remote_path, filename)

            local_path = path.join(self.monitored_path(), filename)
            key = str.format("{}/{}", remote_path, filename)

            storage.put(local_path, key)

            if self.debug():
                upload_end_time = time_ns()
                verify_start_time = time_ns()

            remote_hash = storage.hash(key)
            local_hash = self.local_hash(self.__logger, filename)

            if remote_hash is not None:
                if local_hash == remote_hash:
                    # logger.debug(
                    #     "Remote File Exists [ service = '%s', bucket = '%s', remote_path = '%s', filename = '%s', local_hash = '%s', remote_hash = '%s' ]",
                    #     storage.service_name(), storage.bucket_name(), remote_path, filename, local_hash, remote_hash)
                    upload_result = True
                else:
                    logger.debug(
                        "Remote File Mismatch [ service = '%s', bucket = '%s', remote_path = '%s', filename = '%s'," +
                        " local_hash = '%s', remote_hash = '%s' ]",
                        storage.service_name(), storage.bucket_name(), remote_path, filename, local_hash, remote_hash)
                    upload_result = False
            else:
                logger.debug(
                    "Remote File Not Present [ service = '%s', bucket = '%s', remote_path = '%s', filename = '%s'," +
                    " local_hash = '%s' ]",
                    storage.service_name(), storage.bucket_name(), remote_path, filename, local_hash)
                upload_result = False

            if upload_result:
                logger.info(
                    "Cloud Copy Complete [ service = '%s', bucket = '%s', remote_path = '%s', filename = '%s' ]",
                    storage.service_name(), storage.bucket_name(), remote_path, filename)
            else:
                logger.warning(
                    "Cloud Copy Failure [ service = '%s', bucket = '%s', remote_path = '%s', filename = '%s' ]",
                    storage.service_name(), storage.bucket_name(), remote_path, filename)

            return upload_result

        except Exception:
            logger.exception(
                "Cloud Copy Error [ watch_directory = '%s', bucket = '%s', remote_path = '%s', filename = '%s' ]",
                self.monitored_path(), storage.bucket_name(), remote_path, filename)
            return False
        finally:
            if self.debug() and start_time is not None:
                end_time = time_ns()
                duration_ms = (end_time - start_time) / (10 ** 6)
                verify_duration_ms = (end_time - verify_start_time) / (10 ** 6)
                upload_duration_ms = (upload_end_time - start_time) / (10 ** 6)
                logger.debug("Cloud Copy Timing [ duration_ms = '%.3f', upload_duration_ms = '%.3f', service = '%s'," +
                             " bucket = '%s', remote_path = '%s', filename = '%s' ]",
                             duration_ms, upload_duration_ms, storage.service_name(), storage.bucket_name(),
                             remote_path, filename)

                self.report_stat_value(filename, "upload_time", upload_duration_ms)
                self.report_stat_value(filename, "verify_time", verify_duration_ms)
                self.report_stat_value(filename, "upload_freq", 1)

    def cloud_hash(self, logger, storage, path, filename):
        start_time = None

        if self.debug():
            start_time = time_ns()

        try:
            key = str.format("{}/{}", path, filename)
            return storage.hash(key)
        except Exception:
            logger.exception(
                "Cloud Hash Computation Error [ service = '%s', bucket = '%s' remote_path = '%s', filename = '%s' ]",
                storage.service_name(), storage.bucket_name(), path, filename)
            return None
        finally:
            if self.debug() and start_time is not None:
                end_time = time_ns()
                duration_ms = (end_time - start_time) / (10 ** 6)

                logger.debug("Cloud Hash Computation Timing [ duration_ms = '%.3f', service = '%s', bucket = '%s'" +
                             " remote_path = '%s', filename = '%s' ]",
                             duration_ms, storage.service_name(), storage.bucket_name(),
                             path, filename)

    def opposing_filename(self, filename):
        filename_no_ext = path.splitext(filename)[0]

        if filename is not None and filename.endswith(self.signature_extension()):
            return str.format("{}.{}", filename_no_ext, self.record_extension())
        elif filename is not None and filename.endswith(self.record_extension()):
            return str.format("{}.{}", filename_no_ext, self.signature_extension())

        return filename

    def is_signature_file(self, filename):
        if filename is not None and filename.endswith(self.signature_extension()):
            return True

        return False

    def is_record_file(self, filename):
        if filename is not None and filename.endswith(self.record_extension()):
            return True

        return False

    def prepare_counters(self):

        self.__stats_instances["rcd_upload_time"] = metrics.new_histogram("rcd_upload_time",
                                                                             metrics.new_reservoir("exp_decaying"))
        self.__stats_instances["rcd_verify_time"] = metrics.new_histogram("rcd_verify_time",
                                                                             metrics.new_reservoir("exp_decaying"))
        self.__stats_instances["sig_upload_time"] = metrics.new_histogram("sig_upload_time",
                                                                             metrics.new_reservoir("exp_decaying"))
        self.__stats_instances["sig_verify_time"] = metrics.new_histogram("sig_verify_time",
                                                                             metrics.new_reservoir("exp_decaying"))
        self.__stats_instances["rcd_upload_freq"] = metrics.new_meter("rcd_upload_freq")
        self.__stats_instances["sig_upload_freq"] = metrics.new_meter("sig_upload_freq")

        if self.csv_stats_directory() is not None:

            if not path.exists(self.csv_stats_directory()):
                os.mkdir(self.csv_stats_directory(), 0o770)

            reporter.register(
                reporter.CSVReporter(self.csv_stats_directory()),
                reporter.fixed_interval_scheduler(30)
            )

    def counter_name_from_file(self, filename, op):
        fmt = "{}_{}"
        op_type = "unknown"

        if self.is_signature_file(filename):
            op_type = "sig"
        elif self.is_record_file(filename):
            op_type = "rcd"

        return str.format(fmt, op_type, op)

    def report_stat_value(self, filename, op, val):
        name = self.counter_name_from_file(filename, op)
        metric = self.__stats_instances.get(name)

        if metric is not None:
            metric.notify(val)

    def run(self):
        try:
            self.__running = True
            self.__init_logging()
            self.__init_cli_support()

            self.__initialized = True

            self.__logger.info(
                "%s Starting [  pid_file = '%s', csv_stats_directory = '%s' ]",
                self.__version.to_version_string("Application Core"),
                self.pid_file(),
                self.csv_stats_directory())

            self.prepare_counters()

            proxies = self.get_proxy_support(self.__logger)

            if self.is_gcs_active():
                self.__gcs_storage = RemoteStorage(access_key=self.gcs_access_key(), secret_key=self.gcs_secret_key(),
                                                   endpoint=self.gcs_endpoint(), region=self.gcs_region(),
                                                   bucket_name=self.gcs_bucket_name(), gcs_support=True,
                                                   proxies=proxies)

            if self.is_s3_active():
                self.__s3_storage = RemoteStorage(access_key=self.s3_access_key(), secret_key=self.s3_secret_key(),
                                                  endpoint=self.s3_endpoint(), region=self.s3_region(),
                                                  bucket_name=self.s3_bucket_name(), gcs_support=False, proxies=proxies)

            self.__mirror = Mirror(self, self.__mirror_version)
            self.__mirror.start()

            if self.enable_reaper():
                self.__reaper = Reaper(self, self.__reaper_version)
                self.__reaper.start()

            while self.__running:
                if self.__mirror is None:
                    self.__mirror = Mirror(self, self.__mirror_version)
                    self.__mirror.start()

                if not self.__mirror.is_alive():
                    self.__logger.warning("Mirror Death Detected - Restarting [ monitored_path = '%s' ]",
                                          self.monitored_path())
                    self.__mirror = Mirror(self, self.__version)
                    self.__mirror.start()

                self.__mirror.join(0.1)

                if self.enable_reaper() and self.__reaper is None:
                    self.__reaper = Reaper(self, self.__reaper_version)
                    self.__reaper.start()

                if self.__reaper is not None:
                    if not self.__reaper.is_alive():
                        self.__logger.warning("Reaper Death Detected - Restarting [ min_keep = '%s', interval = '%s' ]",
                                              self.reaper_min_keep(),
                                              self.reaper_interval())
                        self.__reaper = Reaper(self, self.__reaper_version)
                        self.__reaper.start()

                    self.__reaper.join(0.1)

                time.sleep(0.001)

            if self.__mirror is not None:
                self.__mirror.stop()
                self.__mirror.join(5)

            if self.__reaper is not None:
                self.__reaper.stop()
                self.__reaper.join(5)

        except (KeyboardInterrupt, SystemExit, InterruptedError):
            if self.__mirror is not None:
                self.__mirror.stop()
                self.__mirror.join(5)

            if self.__reaper is not None:
                self.__reaper.stop()
                self.__reaper.join(5)

            return
        except Exception:
            self.__logger.exception(
                "Application Core Execution Error - Terminating [ watch_directory = '%s', gcs_active = '%s'," +
                " s3_active = '%s' ]",
                self.monitored_path(), self.is_gcs_active(), self.is_s3_active())
            return
        finally:
            try:
                if self.__initialized:
                    self.__logger.info("%s Stopping", self.__version.to_version_string("Application Core"))

                if self.__mirror is not None and self.__mirror.is_alive():
                    self.__mirror.stop()

                if self.__reaper is not None and self.__reaper.is_alive():
                    self.__reaper.stop()

                return
            except Exception:
                return

    def terminate(self):
        self.__running = False

        if self.__mirror is not None and self.__mirror.is_alive():
            self.__mirror.stop()

        if self.__reaper is not None and self.__reaper.is_alive():
            self.__reaper.stop()


class Mirror(Thread):
    __application = None
    __version = None

    __running = True

    __logger = None
    __console_logger = None

    __observer = None
    __file_event_handler = None

    def __init__(self, application, version):
        super().__init__(daemon=False)
        self.__application = application
        self.__version = version
        self.__logger = logging.getLogger("Mirror")
        self.__console_logger = logging.getLogger("Console")

    def __init_observer(self):
        extensions = list()

        if not self.__application.prioritize_signatures() or self.__application.signature_extension() is None:
            extensions.append(self.__application.record_extension())
        elif self.__application.prioritize_signatures():
            extensions.append(self.__application.signature_extension())

        if not self.__application.prioritize_signatures() and self.__application.require_signature() \
                and self.__application.signature_extension() is not None:
            extensions.append(self.__application.signature_extension())

        regexes = list()

        for ext in extensions:
            regexes.append(str.format(".*\.{}", ext))

        self.__file_event_handler = MirrorFileEventHandler(self.__application, regexes=regexes, ignore_directories=True)
        self.__observer = Observer()
        self.__observer.schedule(self.__file_event_handler, self.__application.monitored_path(), recursive=False)

    def start(self):
        self.__running = True
        super().start()

    def stop(self):
        self.__running = False

    def run(self):
        self.__init_observer()

        try:
            self.__observer.start()
        except OSError:
            if self.__observer.is_alive():
                self.__observer.stop()
                self.__observer.join(1)

        try:

            self.__logger.info(
                "%s Starting [ watch_directory = '%s', enable_reaper = '%s', gcs_active = '%s', s3_active = '%s'," +
                " record_extension = '%s', signature_extension = '%s', require_signature = '%s'," +
                " prioritize_signatures = '%s', pid_file = '%s' ]",
                self.__version.to_version_string("Mirror"),
                self.__application.monitored_path(),
                self.__application.enable_reaper(), self.__application.is_gcs_active(),
                self.__application.is_s3_active(), self.__application.record_extension(),
                self.__application.signature_extension(), self.__application.require_signature(),
                self.__application.prioritize_signatures(), self.__application.pid_file())

            # self.__application.log_proxy_support(self.__logger)

            while self.__running:
                try:
                    if not self.__observer.is_alive():
                        self.__logger.warning("Observer Death Detected - Restarting [ monitored_path = '%s' ]",
                                              self.__application.monitored_path())
                        self.__init_observer()
                        self.__observer.start()

                    self.__observer.join(0.1)
                except OSError:
                    if self.__observer.is_alive():
                        self.__observer.stop()
                        self.__observer.join(1)
                        continue

        except (KeyboardInterrupt, SystemExit, InterruptedError):
            self.__observer.stop()
            self.__application.terminate()
            return
        except (Exception, OSError):
            self.__logger.exception(
                "Mirror Execution Error - Terminating [ watch_directory = '%s', gcs_active = '%s', s3_active = '%s' ]",
                self.__application.monitored_path(), self.__application.is_gcs_active(),
                self.__application.is_s3_active())
            return
        finally:
            try:
                self.__logger.info("%s Stopping", self.__version.to_version_string("Mirror"))

                if self.__observer.is_alive():
                    self.__observer.stop()

                return
            except Exception:
                return


class Reaper(Thread):
    __version = None
    __application = None

    __running = True
    __interval = 60.0
    __require_signature = True
    __record_extension = None
    __signature_extension = None

    __min_keep = None

    __current_local_hashes = {}
    __current_s3_hashes = {}
    __current_gcs_hashes = {}

    __stderr_log_handler = None
    __stdout_log_handler = None
    __file_log_handler = None
    __syslog_log_handler = None
    __file_log_formatter = None
    __syslog_log_formatter = None
    __console_log_formatter = None

    __fail_safe = False

    __logger = None
    __console_logger = None

    GCS_HASH = 1
    S3_HASH = 2
    LOCAL_HASH = 3

    def __init__(self, application, version):
        super().__init__(daemon=False)
        self.__version = version
        self.__application = application
        self.__logger = logging.getLogger("Reaper")
        self.__console_logger = logging.getLogger("Console")
        self.__require_signature = self.__application.require_signature()
        self.__record_extension = self.__application.record_extension()
        self.__signature_extension = self.__application.signature_extension()
        self.__interval = self.__application.reaper_interval()
        self.__min_keep = self.__application.reaper_min_keep()

        if self.__interval is None or self.__interval == 0:
            self.__interval = 60.0

    def start(self):
        self.__running = True
        super().start()

    def stop(self):
        self.__running = False

    def cross_check(self, filename):

        if filename not in self.__current_local_hashes:
            return False

        local_hash = self.__current_local_hashes[filename]

        if local_hash is None or local_hash.strip() == "":
            self.__logger.debug("Cross Check - Invalid Local Hash [ filename = '%s' ]", filename)
            return False

        gcs_hash = None
        s3_hash = None

        if self.__application.is_gcs_active():
            if filename not in self.__current_gcs_hashes:
                return False

            gcs_hash = self.__current_gcs_hashes[filename]

            if gcs_hash is None or gcs_hash.strip() == "":
                self.__logger.debug("Cross Check - Invalid GCS Hash [ filename = '%s' ]", filename)
                return False

            if gcs_hash != local_hash:
                self.__logger.debug(
                    "Cross Check - GCS Hash Mismatch [ filename = '%s', local_hash = '%s', gcs_hash = '%s' ]", filename,
                    local_hash, gcs_hash)
                return False

        if self.__application.is_s3_active():
            if filename not in self.__current_s3_hashes:
                return False

            s3_hash = self.__current_s3_hashes[filename]

            if s3_hash is None or s3_hash.strip() == "":
                self.__logger.debug("Cross Check - Invalid S3 Hash [ filename = '%s' ]", filename)
                return False

            if s3_hash != local_hash:
                self.__logger.debug(
                    "Cross Check - S3 Hash Mismatch [ filename = '%s', local_hash = '%s', s3_hash = '%s' ]", filename,
                    local_hash, s3_hash)
                return False

        if self.__application.is_s3_active() and self.__application.is_gcs_active():
            if s3_hash != gcs_hash:
                self.__logger.debug(
                    "Cross Check - Cloud Hash Mismatch [ filename = '%s', gcs_hash = '%s', s3_hash = '%s' ]", filename,
                    gcs_hash, s3_hash)
                return False

        self.__logger.debug("Cross Check Passed [ filename = '%s' ]", filename)
        return True

    def exclude_protected_items(self, work_items, min_keep):
        if work_items is None or len(work_items) <= 0:
            return work_items

        if len(work_items) <= min_keep:
            return None

        sorted_items = sorted(work_items, key=methodcaller('key'))

        return sorted_items[:len(sorted_items) - min_keep]

    def finalize_work_item(self, item):

        if not item.is_populated(self.__application.require_signature()):
            return

        if self.__application.require_signature():
            if len(self.missing_hashes(item.record_name)) > 0:
                return

            if not self.cross_check(item.record_name):
                return

            if len(self.missing_hashes(item.signature_name)) > 0:
                return

            if not self.cross_check(item.signature_name):
                return

            os.unlink(path.join(self.__application.monitored_path(), item.record_name))
            os.unlink(path.join(self.__application.monitored_path(), item.signature_name))

            self.resolve_filename(item.record_name)
            self.resolve_filename(item.signature_name)

            self.__logger.info("Deleted File Pair [ record_file = '%s', signature_file = '%s' ]", item.record_name,
                               item.signature_name)
        else:
            if item.record_name is not None:
                if len(self.missing_hashes(item.record_name)) > 0:
                    return

                if not self.cross_check(item.record_name):
                    return

                os.unlink(path.join(self.__application.monitored_path(), item.record_name))
                self.resolve_filename(item.record_name)
                self.__logger.info("Deleted File [ filename = '%s' ]", item.record_name)

            if item.signature_name is not None:
                if len(self.missing_hashes(item.signature_name)) > 0:
                    return

                if not self.cross_check(item.signature_name):
                    return

                os.unlink(path.join(self.__application.monitored_path(), item.signature_name))
                self.resolve_filename(item.signature_name)
                self.__logger.info("Deleted File [ filename = '%s' ]", item.signature_name)

    def resolve_needs(self, filename, needs):
        for need in needs:
            storage = None
            bucket_path = None

            if need == self.LOCAL_HASH:
                hash = self.__invoke_local_hash(filename)
                self.__current_local_hashes[filename] = hash
            elif need == self.GCS_HASH:
                storage = self.__application.gcs_storage()
                bucket_path = self.__application.gcs_bucket_path()
            elif need == self.S3_HASH:
                storage = self.__application.s3_storage()
                bucket_path = self.__application.s3_bucket_path()

            if need != self.LOCAL_HASH:
                hash = self.__invoke_cloud_hash(storage, bucket_path, filename)

                if hash is None:
                    local_hash = self.__application.local_hash(self.__logger, filename)
                    self.__logger.warning(
                        "Transient File Detected [ service = '%s', bucket = '%s', remote_path = '%s', filename = '%s', local_hash = '%s' ]",
                        storage.service_name(), storage.bucket_name(), bucket_path, filename, local_hash)

                    self.__invoke_cloud_copy(storage, bucket_path, filename)

                if need == self.S3_HASH:
                    self.__current_s3_hashes[filename] = hash
                elif need == self.GCS_HASH:
                    self.__current_gcs_hashes[filename] = hash

    def resolve_filename(self, filename):
        if filename in self.__current_local_hashes:
            self.__current_local_hashes.pop(filename)

        if self.__application.is_s3_active() and filename in self.__current_s3_hashes:
            self.__current_s3_hashes.pop(filename)

        if self.__application.is_gcs_active() and filename in self.__current_gcs_hashes:
            self.__current_gcs_hashes.pop(filename)

    def missing_hashes(self, filename):
        needs = []

        if filename not in self.__current_local_hashes or self.__current_local_hashes[filename] is None:
            needs.append(self.LOCAL_HASH)

        if self.__application.is_s3_active():
            if filename not in self.__current_s3_hashes or self.__current_s3_hashes[filename] is None:
                needs.append(self.S3_HASH)

        if self.__application.is_gcs_active():
            if filename not in self.__current_gcs_hashes or self.__current_gcs_hashes[filename] is None:
                needs.append(self.GCS_HASH)

        if self.__logger.isEnabledFor(logging.DEBUG):
            needs_local = self.LOCAL_HASH in needs
            needs_s3 = self.S3_HASH in needs
            needs_gcs = self.GCS_HASH in needs
            complete = len(needs) == 0
            self.__logger.debug(
                "Work Item Hash Availability [ filename = '%s', complete = '%s', needs_local = '%s', needs_s3 = '%s', needs_gcs = '%s' ]",
                filename, complete, needs_local, needs_s3, needs_gcs)

        return needs

    def __collect_work_items(self):
        work_items = dict()

        try:
            with os.scandir(self.__application.monitored_path()) as file_list:
                for entry in file_list:
                    filename = path.basename(entry.name)
                    key = filename

                    if self.__application.require_signature():
                        key = path.splitext(filename)[0]

                    mtime = path.getmtime(path.join(self.__application.monitored_path(), filename))

                    if entry.is_file() and filename.endswith(self.__record_extension):
                        if key not in work_items:
                            work_items[key] = WorkItem(record_name=filename, record_mtime=mtime)
                        else:
                            work_items[key].record_name = filename
                            work_items[key].record_mtime = mtime

                    elif entry.is_file() and filename.endswith(self.__signature_extension):
                        if key not in work_items:
                            work_items[key] = WorkItem(signature_name=filename, signature_mtime=mtime)
                        else:
                            work_items[key].signature_name = filename
                            work_items[key].signature_mtime = mtime

                    if key in work_items and work_items[key].is_populated(self.__application.require_signature()):
                        self.__logger.debug(
                            "Collected Work Item [ record_name = '%s', record_mtime = '%s', signature_name = '%s', signature_mtime = '%s' ]",
                            work_items[key].record_name, work_items[key].record_mtime,
                            work_items[key].signature_name, work_items[key].signature_mtime)

            if len(work_items) > 0:
                pop_keys = set()
                for key, value in work_items.items():
                    if not value.is_populated(self.__application.require_signature()):
                        pop_keys.add(key)

                if len(pop_keys) > 0:
                    for key in pop_keys:
                        work_items.pop(key)

            return list(work_items.values())
        except Exception:
            self.__logger.exception("Directory List Error [ watch_directory = '%s' ]",
                                    self.__application.monitored_path())
            return None

    def __collect_local_hashes(self, work_items):
        hash_dict = {}

        for item in work_items:
            hash_dict[item.path] = self.__invoke_local_hash(item.name)

        return hash_dict

    def __invoke_local_hash(self, filename):
        return self.__application.local_hash(self.__logger, filename)

    def __invoke_cloud_hash(self, storage, path, filename):
        return self.__application.cloud_hash(self.__logger, storage, path, filename)

    def __invoke_cloud_copy(self, storage, remote_path, filename):
        thread = Thread(target=self.__application.cloud_copy, args=(self.__logger, storage, remote_path, filename))
        thread.daemon = True
        thread.start()

    def run(self):
        try:
            self.__logger.info(
                "%s Starting [ record_extension = '%s', signature_extension = '%s', require_signature = '%s', min_keep = '%s', interval = '%s' ]",
                self.__version.to_version_string("Reaper"), self.__record_extension, self.__signature_extension,
                self.__require_signature, self.__min_keep, self.__interval)

            # self.__application.log_proxy_support(self.__logger)
            work_items = list()

            last_work_time = time_ns()

            while self.__running:

                time.sleep(0.1)
                current_work_time = time_ns()
                current_elapsed = (current_work_time - last_work_time) / (10 ** 9)

                if current_elapsed < self.__interval:
                    continue

                if work_items is None or len(work_items) <= 0:
                    work_items = self.__collect_work_items()

                    if self.__min_keep is not None and self.__min_keep > 0:
                        work_items = self.exclude_protected_items(work_items, self.__min_keep)

                        if self.__logger.isEnabledFor(logging.DEBUG) and work_items is not None and len(work_items) > 0:
                            for item in work_items:
                                self.__logger.debug(
                                    "Deletion Eligible Work Item [ record_name = '%s', record_mtime = '%s'," +
                                    " signature_name = '%s', signature_mtime = '%s' ]",
                                    item.record_name, item.record_mtime, item.signature_name, item.signature_mtime)

                if work_items is not None and len(work_items) > 0:
                    while len(work_items) > 0:
                        item = work_items.pop(0)

                        if item.record_name is not None:
                            item.record_needs = self.missing_hashes(item.record_name)
                            self.resolve_needs(item.record_name, item.record_needs)

                        if item.signature_name is not None:
                            item.signature_needs = self.missing_hashes(item.signature_name)
                            self.resolve_needs(item.signature_name, item.signature_needs)

                        self.finalize_work_item(item)

                        time.sleep(0.5)

                        if not self.__running:
                            break

                last_work_time = time_ns()

        except (KeyboardInterrupt, SystemExit, InterruptedError):
            self.__application.terminate()
            return
        except Exception:
            self.__logger.exception("Reaper Execution Error [ fail_safe = '%s' ]", self.__fail_safe)
            return
        finally:
            self.__logger.info("%s Stopping", self.__version.to_version_string("Reaper"))


class MirrorFileEventHandler(RegexMatchingEventHandler):
    __application = None
    __logger = None
    __console_logger = None

    def __init__(self, application, regexes=(".*"), ignore_regexes=(), ignore_directories=False,
                 case_sensitive=False):
        super().__init__(regexes=regexes, ignore_regexes=ignore_regexes, ignore_directories=ignore_directories,
                         case_sensitive=case_sensitive)
        self.__application = application
        self.__logger = logging.getLogger("Mirror")
        self.__console_logger = logging.getLogger("Console")

    def on_moved(self, event):
        if path.samefile(self.__application.monitored_path(), path.dirname(event.dest_path)):
            self.handle_event(event.event_type, event.src_path, event.dest_path)

    def on_created(self, event):
        if self.__application.is_linux_environment():
            return

        self.handle_event(event.event_type, event.src_path)

    def on_modified(self, event):
        self.handle_event(event.event_type, event.src_path)

    def on_deleted(self, event):
        self.handle_event(event.event_type, event.src_path)

    def handle_event(self, event_type, src_path, dest_path=None):
        epoch = calendar.timegm(time.gmtime())

        self.__logger.debug(
            "File System Event Notify [ timestamp = '%s', event_type = '%s', src_path = '%s', dest_path = '%s' ]",
            int(epoch), event_type, src_path, dest_path)

        if event_type == "deleted":
            return

        if event_type == "moved" and dest_path is not None:
            filename = path.basename(dest_path)
        else:
            filename = path.basename(src_path)

        mtime = path.getmtime(path.join(self.__application.monitored_path(), filename))

        duration_ms = (mtime - epoch) * 1000

        self.__logger.debug("File System Notify Timing [ notify_timestamp_secs = '%.6f', file_mtime_secs = '%.6f'," +
                            " duration_ms = '%.3f', event_type = '%s', filename = '%s' ]",
                            epoch, mtime, duration_ms, event_type, filename)

        if self.__application.is_gcs_active():
            self.__invoke_cloud_copy(self.__application.gcs_storage(), self.__application.gcs_bucket_path(), filename)

            if self.__application.prioritize_signatures():
                self.__invoke_cloud_copy(self.__application.gcs_storage(), self.__application.gcs_bucket_path(),
                                         self.__application.opposing_filename(filename))
            # self.__invoke_cloud_copy(self.__application.gcs_remote(), self.__application.gcs_bucket_name(),
            #                          self.__application.gcs_bucket_path())

        if self.__application.is_s3_active():
            self.__invoke_cloud_copy(self.__application.s3_storage(), self.__application.s3_bucket_path(), filename)

            if self.__application.prioritize_signatures():
                self.__invoke_cloud_copy(self.__application.s3_storage(), self.__application.s3_bucket_path(),
                                         self.__application.opposing_filename(filename))

            # self.__invoke_cloud_copy(self.__application.s3_remote(), self.__application.s3_bucket_name(),
            #                          self.__application.s3_bucket_path())

    def __invoke_cloud_copy(self, storage, remote_path, filename):
        thread = Thread(target=self.__application.cloud_copy, args=(self.__logger, storage, remote_path, filename))
        thread.daemon = True
        thread.start()

        # return self.__application.cloud_copy(self.__logger, storage, remote_path, filename)


# Support Classes

class Version:
    __major = None
    __minor = None
    __release = None

    def __init__(self, major, minor, release):
        self.__major = major
        self.__minor = minor
        self.__release = release

    def major(self):
        return self.__major

    def minor(self):
        return self.__minor

    def release(self):
        return self.__release

    def __str__(self):
        return str.format("{}.{}.{}", self.__major, self.__minor, self.__release)

    def to_version_string(self, program):
        return str.format("{} v{}", program, self)


class WorkItem:
    record_name = None
    record_mtime = None
    record_needs = list()
    signature_name = None
    signature_mtime = None
    signature_needs = list()

    def __init__(self, record_name=None, record_mtime=None, signature_name=None, signature_mtime=None):
        self.record_name = record_name
        self.signature_name = signature_name

        if record_mtime is not None:
            self.record_mtime = int(record_mtime)

        if signature_mtime is not None:
            self.signature_mtime = int(signature_mtime)

    def is_populated(self, require_signature):
        if not require_signature:
            return self.record_name is not None or self.signature_name is not None

        return self.record_name is not None and self.signature_name is not None

    def key(self):
        if self.record_name is not None:
            name = self.record_name
        else:
            name = self.signature_name

        if self.record_mtime is not None:
            mtime = self.record_mtime
        else:
            mtime = self.signature_mtime

        return (mtime, name)


class RemoteStorage:
    __access_key = None
    __secret_key = None
    __endpoint = None
    __region = None
    __bucket_name = None
    __gcs_support = False
    __proxies = {}

    __client = None
    __resource = None
    __session = None
    __bucket = None
    __transfer_config = None

    __logger = None
    __console_logger = None

    __service_name = None

    def __init__(self, access_key, secret_key, endpoint, region, bucket_name, gcs_support=False, proxies=None):
        self.__access_key = access_key
        self.__secret_key = secret_key
        self.__endpoint = endpoint
        self.__region = region
        self.__bucket_name = bucket_name
        self.__gcs_support = gcs_support

        if proxies is None:
            proxies = {}

        self.__proxies = proxies

        self.__logger = logging.getLogger("RemoteStorage")
        self.__console_logger = logging.getLogger("Console")

        self.__init_remote()

        if self.__gcs_support:
            self.__init_gcs()

    def __init_remote(self):
        self.__service_name = "S3"

        self.__session = Session(aws_access_key_id=self.__access_key,
                                 aws_secret_access_key=self.__secret_key,
                                 region_name=self.__region)

        self.__resource = self.__session.resource('s3', endpoint_url=self.__endpoint,
                                                  config=Config(signature_version='s3v4', proxies=self.__proxies))

        self.__client = self.__session.client('s3', endpoint_url=self.__endpoint,
                                              config=Config(signature_version='s3v4', proxies=self.__proxies))

        self.__bucket = self.__resource.Bucket(self.__bucket_name)

        self.__transfer_config = TransferConfig(multipart_threshold=9999999999999999)

    def __init_gcs(self):
        self.__service_name = "GCS"

        self.__session.events.unregister('before-parameter-build.s3.ListObjects',
                                         set_list_objects_encoding_type_url)

    def exists(self, key):
        try:
            resp = self.__client.head_object(Bucket=self.__bucket_name, Key=key)
            return resp
        except ClientError as ex:
            if ex.response['Error']['Code'] != '404':
                self.__logger.exception(
                    "Remote Exists Error [ endpoint = '%s', region = '%s', bucket_name = '%s', key = '%s' ]",
                    self.__endpoint, self.__region, self.__bucket_name, key, exc_info=ex)
                raise
            else:
                return None

    def hash(self, key):
        try:
            resp = self.exists(key)

            if resp is None:
                return None

            return str.replace(resp['ETag'], '"', '')
        except ClientError as ex:
            self.__logger.exception(
                "Remote Hash Error [ endpoint = '%s', region = '%s', bucket_name = '%s', key = '%s' ]",
                self.__endpoint, self.__region, self.__bucket_name, key, exc_info=ex)
            return None

    def put(self, local_path, key):
        try:
            self.__bucket.upload_file(local_path, key, Config=self.__transfer_config)
            return True
        except ClientError as ex:
            self.__logger.exception(
                "Remote Put Error [ endpoint = '%s', region = '%s', bucket_name = '%s', key = '%s', local_path = '%s' ]",
                self.__endpoint, self.__region, self.__bucket_name, key, local_path, exc_info=ex)
            return False

    def service_name(self):
        return self.__service_name

    def bucket_name(self):
        return self.__bucket_name


# Application Entry Point

app = Application()
app.run()
