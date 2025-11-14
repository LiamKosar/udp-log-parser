from __future__ import annotations
import socket
import os
from datetime import datetime
import time
from multiprocessing import Process, Queue
import logging
from queue import Empty
from contextlib import contextmanager
import psycopg2
from functools import wraps
import random
import yaml
from typing import Dict
import signal
import sys


# makes a log that looks like this:
# 2025-10-31 14:11:25 - INFO - worker 1 is processing 5 items
logging.basicConfig(
    filename='server.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class PGConnection(object):
    pg_url_env_variable_name: str 
    conn: psycopg2.extensions.connection

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(PGConnection, cls).__new__(cls)
        return cls.instance
    
    def initialize(self, pg_url_env_variable_name: str):
        self.pg_url_env_variable_name = pg_url_env_variable_name
        self.conn = None
    
    def attempt_connection_refresh(self):
        try:
            if self.conn:
                self.conn.close()
            self.conn = psycopg2.connect(os.environ.get(self.pg_url_env_variable_name))
        except psycopg2.OperationalError:
            logging.error(f'Failed connection refresh for pg at {self.pg_url_env_variable_name}')
            raise
    
    @contextmanager
    def get_connection(self):
        if self.conn is None:
            self.attempt_connection_refresh()
        yield self.conn
        self.conn.commit()

def start_log_daemon(lds: LogDaemonSettings):
    log_shortcut_to_queue_map: Dict[str, Queue[tuple]] = {}
    for log_schema in lds.log_schema_list:
        log_shortcut_to_queue_map[log_schema.log_shortcut] = Queue()

    processes = []
    log_parser_process = Process(target=log_parser, args=(lds, log_shortcut_to_queue_map, ))
    processes.append(log_parser_process)

    for log_shortcut in log_shortcut_to_queue_map.keys():
        log_schema: LogSchema = lds.shortcut_to_log_schema_map[log_shortcut]
        log_queue: Queue[tuple] = log_shortcut_to_queue_map[log_shortcut]

        for i in range(log_schema.num_workers):
            processes.append(Process(target=log_pusher, args=(log_schema, log_queue, i,)))


    # Set up signal handler in the parent process
    def signal_handler(signum, frame):
        logging.info("Shutdown signal received, stopping all processes...")
        
        # Give processes a chance to shutdown gracefully
        for p in processes:
            p.join(timeout=5)
        
        # Force terminate any remaining processes
        for p in processes:
            if p.is_alive():
                logging.warning(f"Force terminating process {p.pid}")
                p.terminate()
                p.join(timeout=1)
                if p.is_alive():
                    p.kill()
        
        logging.info("All processes stopped")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    for process in processes:
        process.start()

    for process in processes:
        process.join()



def log_parser(lds: LogDaemonSettings, log_shortcut_to_queue_map: Dict[str, Queue]):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", lds.port))
    while True:
        data, _ = sock.recvfrom(lds.datagram_max_size)
        msg = data.strip()
        metric_string = msg.decode('utf-8')
        log_shortcut, log = metric_string.split('#')

        log_schema: LogSchema = lds.shortcut_to_log_schema_map.get(log_shortcut)
        if log_schema is None:
            continue

        parameters = log.split(log_schema.delimiter)
        parameter_schemas: list[ParameterSchema] = log_schema.parameter_schema_list

        # drop log if doesn't match schema
        if len(parameters) != len(parameter_schemas):
            continue

        all_parameters_converted = True
        for i in range(len(parameters)):
            parameter = parameters[i]
            try:
                parameters[i] = parameter_schemas[i].decode(parameter)
            except DecodingException:
                all_parameters_converted = False
                break
        
        # drop log if doesn't match schema
        if not all_parameters_converted:
            continue     

        parameter_tuple = tuple(parameters) 
        log_shortcut_to_queue_map[log_shortcut].put(parameter_tuple)

def log_pusher(log_schema: LogSchema, log_queue: Queue[tuple], worker_num: int):
    logging.info(f'Starting worker {worker_num} for {log_schema.log_shortcut}')
    
    PGConnection().initialize(log_schema.pg_url_env_variable_name)
    batch_size = log_schema.batch_size
    batch_timeout = log_schema.batch_timeout

    batch: list[tuple] = []
    last_batch_flush = time.time()

    while True:
        try:   
            
            item = log_queue.get(timeout=log_schema.batch_timeout)
            batch.append(item)
            
            # Check if we should flush the batch 
            if len(batch) >= batch_size or (time.time() - last_batch_flush) > batch_timeout:
                if batch:
                    process_batch(log_schema, batch)
                    batch = []
                    last_batch_flush = time.time()              
        except Empty:
            # Queue is empty, check if we need to flush based on timeout
            if batch and (time.time() - last_batch_flush) > batch_timeout:
                process_batch(log_schema, batch)
                batch = []
                last_batch_flush = time.time()

def inject_connection_with_retries(retries: int = 3):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            conn_manager = PGConnection()
            for _ in range(retries):
                try:
                    with conn_manager.get_connection() as conn:
                        kwargs['conn'] = conn
                        result = func(*args, **kwargs)
                        return result
                except psycopg2.DatabaseError as e:
                    logging.error(f'Error connecting to pg: {e}')
                    conn_manager.conn = None
                    time.sleep(random.uniform(.5, 3))
            
            logging.info('Dropped logs')

        return wrapper
    return decorator

@inject_connection_with_retries(retries=3)
def process_batch(log_schema: LogSchema, batch: list[tuple], **kwargs):
    conn = kwargs['conn']

    with conn.cursor() as cursor:
        cursor.executemany(
            f"INSERT INTO {log_schema.pg_table_name} ({log_schema.parameter_insert_string}) VALUES ({log_schema.parameter_placeholder_str})",
            batch
        )

class LogDaemonSettings:

    TYPE_CONVERTERS = {
    'int': int,
    'float': float,
    'bool': lambda x: x.lower() in ('true', '1', 'yes'),
    'str': str,
    # 'datetime': lambda x: datetime.fromisoformat(x),
    'timestamp': lambda x: datetime.fromtimestamp(float(x)),
    }

    port: int
    datagram_max_size: int
    log_schema_list: list[LogSchema]
    shortcut_to_log_schema_map: Dict[str, LogSchema]

    def __init__(self, schema_config: Dict[str, any]):
        network_settings = schema_config['network']
        self.port = network_settings.get('port', 8125)
        self.datagram_max_size = network_settings.get('datagram_max_size', 65535)

        self.log_schema_list = []
        self.shortcut_to_log_schema_map = {}
        log_schemas = schema_config['log_schemas']

        for ls in log_schemas.values():
            log_schema = LogSchema(ls)
            self.log_schema_list.append(log_schema)
            self.shortcut_to_log_schema_map[log_schema.log_shortcut] = log_schema

class LogSchema:
    """
    Structure of a specific log type.
    Logs can be denoted as this in utf-8:
    'log_shortcut#<parameter_1><delimiter><parameter_2><delimiter><parameter_n>'
    """

    log_shortcut: str # log prefix, will be used to route the parser to the correct schema
    pg_url_env_variable_name: str # the name of the environment variable storing pg url
    pg_table_name: str
    num_workers: str # number of workers pushing logs to pg
    batch_size: int # how many logs get pushed with each query
    batch_timeout: int # overrides batch size in case of minimal log throughput
    delimiter: str # separates parameters within a log string 
    parameter_schema_list: list[ParameterSchema]

    # these are generated strings, storing them to make queries faster
    parameter_insert_string: str # ex: 'parameter_1, parameter_2, ..., parameter_n'
    parameter_placeholder_str: str # ex: '%s, %s, ..., %s'

    def __init__(self, log_schema: Dict[str, any]):
        self.log_shortcut = log_schema['log_shortcut']
        self.pg_url_env_variable_name = log_schema['pg_url_env_variable_name']
        self.pg_table_name = log_schema['pg_table_name']
        self.num_workers = log_schema.get('num_workers', 1)
        self.batch_size = log_schema.get('batch_size', 5)
        self.batch_timeout = log_schema.get('batch_timeout', 5)
        self.delimiter = log_schema['delimiter']

        self.parameter_schema_list = []
        self.parameter_string_format = ''
        parameter_schemas = log_schema['parameter_schemas']

        for parameter_schema in parameter_schemas:
            self.parameter_schema_list.append(ParameterSchema(parameter_schema))
          
        self.parameter_insert_string = ', '.join([param.name for param in self.parameter_schema_list])
        self.parameter_placeholder_str = LogSchema.repeat_placeholder(len(self.parameter_schema_list))

    @classmethod
    def repeat_placeholder(cls, n: int):
        """Create a string with n %s placeholders separated by commas."""
        return ', '.join(['%s'] * n)

class ParameterSchema:
    name: str
    type: str

    def __init__(self, parameter_schema: Dict[str, any]):
        if LogDaemonSettings.TYPE_CONVERTERS.get(parameter_schema['type']):
            self.name = parameter_schema['name']
            self.type = parameter_schema['type']
        else:
            raise ValueError(f"Parameter type {parameter_schema['type']} has not been implemented")
        
    def decode(self, parameter: str):
        """ Try to cast the parameter to its type """
        converter = LogDaemonSettings.TYPE_CONVERTERS[self.type]
        try:
            return converter(parameter) 

        except (ValueError, TypeError) as e:
            logging.error(f"Error converting {parameter=}: {e}")
            raise DecodingException(f"Couldn't decode {parameter=} into {self.type=}")
    

class DecodingException(Exception):
    def __init__(self, *args):
        super().__init__(*args)       


if __name__ == "__main__":
    with open('config.yaml', 'r') as file:
            schema_config = yaml.safe_load(file)
    lds = LogDaemonSettings(schema_config=schema_config)
    start_log_daemon(lds)