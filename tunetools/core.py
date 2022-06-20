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
from . import session_logger


def _construct_config(
        parameters: Iterable,
        values: Iterable,
        force_values: dict
):
    result = dict(
        map(lambda x: (x[0].name, x[0].base_type.python_type(x[1])), zip(parameters, values))
    )
    for force_name, force_value in force_values.items():
        result[force_name] = type(result[force_name])(force_value)
    return result


def _construct_where(
        parameters: Iterable,
        values: Iterable
):
    result = {}
    for p, v in zip(parameters, values):
        if not p.ignore:
            result[p.db_name] = v
    return result


def _print_config(
        config: dict,
        extra: str
):
    promt = "=" * int((20 - len(extra)) / 2)
    promt = promt + extra + promt
    print(promt)
    for x in config.items():
        print(str(x[0]) + ": " + str(x[1]))
    print("=" * len(promt))


def _prepare_db(
        conn: sqlite3.Connection,
        num_sample: int,
        parameters: Iterable,
        filter_function,
        save_db=True
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
            old_type, old_value = tuple(old_params[x.name])
            try:
                old_value = x.base_type.python_type(old_value)
            except ValueError:
                pass
            if (x.base_type.db_type, x.base_type.python_type(x.default)) != (old_type, old_value):
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
    plan = []  # (parameters, number_to_execute, number_to_insert)

    for param_tuple in itertools.product(*param_space_sampled):
        if filter_function is not None:
            config = _construct_config(parameters, param_tuple, {})
            if not filter_function(**config):
                continue
        where = _construct_where(parameters, param_tuple)
        current_count = db_utils.count(conn, "RESULT",
                                       where=where)
        where['STATUS'] = 'TERMINATED'
        current_done_count = db_utils.count(conn, "RESULT",
                                       where=where)
        if current_done_count < num_sample:
            model = {}
            model_db = {
                "HOST": socket.gethostname(),
                "PID": os.getpid()
            }

            for p, v in zip(parameters, param_tuple):
                model[p.name] = v
                model_db[p.db_name] = v

            plan.append((model, num_sample - current_done_count, num_sample - current_count))
            if current_count < num_sample:
                insert_param.extend([model_db] * (num_sample - current_count))

    if save_db:
        db_utils.insert(conn, "RESULT", insert_param)

    return plan


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
    _print_config(config, "TEST")
    result = obj_function(**config)
    print("result: " + str(result))


def plan(
        filter_function=None,
        num_sample=1,
        parameters: list = None,
        force_values: dict = None,
):
    if parameters is None:
        parameters = []
    if force_values is None:
        force_values = {}

    if not os.path.isdir(".tune"):
        os.makedirs(".tune")
    conn = sqlite3.connect(os.path.join(".tune", "tune.db"))
    plan = _prepare_db(conn, num_sample, parameters, filter_function, False)
    if len(plan) == 0:
        print("No task will be executed!")
        return
    samples = 0
    samples_to_insert = 0
    param_ranges = {}
    for p, n, _ in plan:
        p.update(force_values)
        for k, v in p.items():
            param_set = param_ranges.get(k, set())
            param_set.add(v)
            param_ranges[k] = param_set
    ignore_params = []
    for k, v in param_ranges.items():
        if len(v) == 1:
            print("common parameter: %s, %s" % (k, v))
            ignore_params.append(k)
    for p, n, n_insert in plan:
        for ignore_p in ignore_params:
            del p[ignore_p]
        samples += n
        samples_to_insert += n_insert
        print("(%d/%d) %s" % (n, n_insert, p))
    print()
    print("%d task%s / %d sample%s will be executed." % (len(plan), '' if len(plan) <= 1 else 's',
                                                          samples, '' if samples <= 1 else 's'))
    print("%d sample%s will be inserted." % (samples_to_insert, '' if samples_to_insert <= 1 else 's'))


def run(
        obj_function,
        filter_function=None,
        num_sample=1,
        parameters: list = None,
        force_values: dict = None,
        on_finish_function=None,
        worker_id=0
):
    if parameters is None:
        parameters = []
    if force_values is None:
        force_values = {}

    os.makedirs(".tune", exist_ok=True)
    conn = sqlite3.connect(os.path.join(".tune", "tune.db"))
    _prepare_db(conn, num_sample, parameters, filter_function)
    run_count = 0

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
        session_logger._start(db_id)
        start = time.time()
        _print_config(config, "RUN #%d IN #%d" % (run_count, worker_id))

        results = None
        try:
            results = obj_function(**config)
            run_count += 1
        except BaseException as e:
            import traceback
            traceback.print_exc()
            raise e
        finally:
            if results is None:
                print("No result returned!! Set %s.STATUS = PENDING" % str(db_id))
                with conn:
                    db_utils.update(conn, "RESULT", put={"STATUS": "PENDING"}, where={"ID": db_id})
            session_logger._end(results is not None)

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
            for k, v in force_values.items():
                replace_dict['param_' + k] = v
            db_utils.update(conn, "RESULT",
                            put=replace_dict,
                            where={
                                "ID": db_id
                            })

    # check if finish
    if on_finish_function is not None:
        with conn:
            total_count = db_utils.count(conn, "RESULT")
            terminated_count = db_utils.count(conn, "RESULT", where={"STATUS": "TERMINATED"})
            if total_count == terminated_count:
                on_finish_function(run_count)

    conn.close()


def test_or_run(
        obj_function,
        filter_function=None,
        num_sample=1,
        parameters: list = None,
        on_finish_function=None,
):
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--plan", action="store_true")
    for x in parameters:
        parser.add_argument("--" + x.name, type=x.base_type.python_type, default=None)
    args = parser.parse_args()

    force_values = {}
    keywords = ['test', 'plan', 'run']
    for arg in vars(args):
        value = getattr(args, arg)
        if value is not None and arg not in keywords:
            force_values[arg] = value
    if args.test:
        print("Test!")
        test(obj_function, parameters=parameters, force_values=force_values)
    elif args.plan:
        print("Plan!")
        plan(filter_function=filter_function, num_sample=num_sample,
             parameters=parameters, force_values=force_values)
    else:
        print("Run!")
        run(obj_function, filter_function=filter_function, num_sample=num_sample,
            parameters=parameters,
            force_values=force_values, on_finish_function=on_finish_function)
