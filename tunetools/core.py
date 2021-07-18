import argparse
import itertools
import json
import os
import socket
import sqlite3
import time
from typing import Iterable

from . import db_utils
from . import config
from .search_space import *


def _construct_config(
        parameters: Iterable,
        values: Iterable,
        force_values: dict
):
    result = dict(zip([x.name for x in parameters], values))
    result.update(force_values)
    return result


def _construct_where(
        parameters: Iterable,
        values: Iterable
):
    return dict(zip([x.db_name for x in parameters], values))


def _print_config(
        config: dict
):
    print("=" * 20)
    for x in config.items():
        print(str(x[0]) + ": " + str(x[1]))
    print("=" * 20)


def _prepare_db(
        conn: sqlite3.Connection,
        num_sample: int,
        parameters: Iterable,
        filter_function
):
    with conn:
        db_utils.create_table(conn, "RESULT", {
            "ID": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "HOST": "TEXT NOT NULL",
            "PID": "INTEGER NOT NULL",
            "RUN_AT": "INTEGER",
            "DURATION_MIN ": "REAL",
            "STATUS": "TEXT DEFAULT PENDING"
        })

        db_utils.ensure_column(conn, "RESULT",
                               [(x.db_name, x.base_type.db_type, x.default) for x in parameters])

        # check parameter compatibility
        old_param_files = os.path.join(".tune", "parameter.json")
        if not os.path.isfile(old_param_files):
            old_params = {}
        else:
            old_params = json.load(open(old_param_files))
        for x in parameters:
            if x.name in old_params:
                if (x.base_type.db_type, str(x.default)) != tuple(old_params[x.name]):
                    raise ValueError("Parameters compatibility check failed! Parameter %s "
                                     "(type: %s, default: %s), but found: (type: %s, default: %s). "
                                     "Please remove tune/parameter.json if you think it doesn't matter." %
                                     (x.name, old_params[x.name][0], old_params[x.name][1],
                                      x.base_type.db_type, str(x.default)))
            old_params[x.name] = (x.base_type.db_type, str(x.default))
        with open(old_param_files, "w") as f:
            json.dump(old_params, f, indent=2)

        param_space_sampled = [x.sample() for x in parameters]
        insert_param = []

        for param_tuple in itertools.product(*param_space_sampled):
            if filter_function is not None:
                config = _construct_config(parameters, param_tuple, {})
                if not filter_function(config):
                    continue
            current_count = db_utils.count(conn, "RESULT",
                                           where=_construct_where(parameters, param_tuple))
            if current_count < num_sample:
                model = {
                    "HOST": socket.gethostname(),
                    "PID": os.getpid()
                }

                for p, v in zip(parameters, param_tuple):
                    model[p.db_name] = v

                insert_param.extend([model] * (num_sample - current_count))

        db_utils.insert(conn, "RESULT", insert_param)


def test(
        obj_function,
        parameters: list = None,
        force_values: dict = None
):
    if parameters is None:
        parameters = []
    if force_values is None:
        force_values = {}
    config = _construct_config(parameters, [x.default for x in parameters], force_values)
    _print_config(config)
    result = obj_function(config)
    print("=" * 20)
    print("result: " + str(result))


def run(
        obj_function,
        filter_function=None,
        num_sample=1,
        parameters: list = None,
        force_values: dict = None
):
    if parameters is None:
        parameters = []
    if force_values is None:
        force_values = {}

    if not os.path.isdir(".tune"):
        os.makedirs(".tune")
    conn = sqlite3.connect(os.path.join(".tune", "tune.db"))
    _prepare_db(conn, num_sample, parameters, filter_function)

    while True:
        with conn:
            x = db_utils.select(conn, "RESULT",
                                project=["ID"] + [x.db_name for x in parameters],
                                where={"STATUS": "PENDING"},
                                order_by="RANDOM()",
                                limit=1, return_first=True)
            values = None
            if x is not None:
                db_id = x[0]
                db_utils.update(conn, "RESULT",
                                put={
                                    "HOST": socket.gethostname(),
                                    "PID": os.getpid(),
                                    "STATUS": "RUNNING",
                                    "RUN_AT": int(time.time())
                                },
                                where={
                                    "ID": db_id
                                })
                values = x[1:]

        if values is None:
            break

        config = _construct_config(parameters, values, force_values)
        start = time.time()
        _print_config(config)

        results = None
        try:
            results = obj_function(config)
        finally:
            if results is None:
                print("No result returned!! Set %s.STATUS = PENDING" % str(db_id))
                with conn:
                    db_utils.update(conn, "RESULT", put={"STATUS": "PENDING"}, where={"ID": db_id})

        duration = (time.time() - start) / 60
        with conn:
            db_utils.ensure_column(conn, "RESULT",
                                   [("ret_" + k, TypeMap[type(v)].db_type, None) for k, v in
                                    results.items()])
            replace_dict = {
                "STATUS": "TERMINATED",
                "DURATION_MIN": duration
            }
            for k, v in results.items():
                replace_dict['ret_' + k] = v
            db_utils.update(conn, "RESULT",
                            put=replace_dict,
                            where={
                                "ID": db_id
                            })

    conn.close()


def test_or_run(
        obj_function,
        filter_function=None,
        num_sample=1,
        parameters: list = None
):
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true")
    for x in parameters:
        parser.add_argument("--" + x.name, type=x.base_type.python_type, default=None)
    args = parser.parse_args()

    force_values = {}
    for arg in vars(args):
        value = getattr(args, arg)
        if value is not None:
            force_values[arg] = value
    if args.test:
        print("Test!")
        test(obj_function, parameters=parameters, force_values=force_values)
    else:
        print("Run!")
        run(obj_function, filter_function=filter_function, num_sample=num_sample, parameters=parameters,
            force_values=force_values)
