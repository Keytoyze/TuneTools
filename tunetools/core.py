import sqlite3
from .search_space import *
import itertools
import socket
import os
import time
from . import db_utils
from typing import Iterable


def _construct_config(
        parameters: Iterable,
        values: Iterable
):
    return dict(zip([x.name for x in parameters], values))


def _construct_where(
        parameters: Iterable,
        values: Iterable
):
    return dict(zip([x.db_name for x in parameters], values))


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

        param_space_sampled = [x.sample() for x in parameters]
        insert_param = []

        for param_tuple in itertools.product(*param_space_sampled):
            if filter_function is not None:
                config = _construct_config(parameters, param_tuple)
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


def run(
        obj_function,
        filter_function=None,
        num_sample=1,
        database_file="tune.db",
        parameters: list = None
):
    if parameters is None:
        parameters = []

    conn = sqlite3.connect(database_file)
    _prepare_db(conn, num_sample, parameters, filter_function)

    while True:
        with conn:
            x = db_utils.select(conn, "RESULT",
                                project=["ID"] + [x.db_name for x in parameters],
                                where={"STATUS": "PENDING"},
                                order_by="RANDOM()",
                                limit=1, return_first=True)
            parameter_tuple_to_run = None
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
                parameter_tuple_to_run = x[1:]

        if parameter_tuple_to_run is None:
            break

        config = _construct_config(parameters, parameter_tuple_to_run)
        start = time.time()

        results = obj_function(config)

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


if __name__ == "__main__":
    def test(x):
        print(">>> start")
        time.sleep(10)
        print(">>> end")
        import random
        return {
            "result": x['alpha'] + x['beta'] + random.random() / 100,
            "result2": "this is result2"
        }


    def filter(x):
        return x['alpha'] != 0 and x['beta'] != 0


    run(obj_function=test, filter_function=filter, num_sample=20, parameters=[
        GridSearchSpace("alpha", base_type=Float, default=0.5, domain=[0.0, 0.3, 0.5]),
        GridSearchSpace("beta", base_type=Float, default=0.5, domain=[0.0, 0.3, 0.5]),
        # GridSearchSpace("gamma", base_type=String, default="xxx", domain=["ff", "ddd"])
    ])
