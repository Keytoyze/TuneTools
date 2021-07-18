import argparse
import os
import socket
import sqlite3
import time

from tunetools import db_utils
from tunetools import statistics_utils

def _get_db_conn(args):
    db_path = os.path.join(os.getcwd(), ".tune", "tune.db")
    if not os.path.isfile(db_path):
        print("Error: cannot find .tune directory in the current working dir!")
        exit(-1)
    return sqlite3.connect(db_path)


def status(args):
    conn = _get_db_conn(args)

    with conn:
        aver_duration = db_utils.execute_sql_return_first(conn, 'SELECT AVG(DURATION_MIN) FROM '
                                                                '(SELECT DURATION_MIN FROM RESULT '
                                                                'WHERE STATUS = "TERMINATED" '
                                                                'ORDER BY RUN_AT DESC '
                                                                'LIMIT 20)')[0]
        count_running = db_utils.count(conn, "RESULT", where={"STATUS": "RUNNING"})
        count_pending = db_utils.count(conn, "RESULT", where={"STATUS": "PENDING"})
    print("Running: %d, Pending: %d" % (count_running, count_pending))
    if count_running != 0:
        print("Speed: %lf/h" % (60 / aver_duration * count_running))
        total_left_min = int(
            (count_pending + count_running / 2) * aver_duration / count_running)

        print("Left Time: %dh%dmin (%s)" % (total_left_min / 60, total_left_min % 60,
                                            time.ctime(time.time() + total_left_min * 60)))

    conn.close()


def terminate(args):
    conn = _get_db_conn(args)
    with conn:
        cursor = db_utils.select(conn, "RESULT", ["ID", "HOST", "PID"],
                                 where={"STATUS": "RUNNING"})
        for id, host, pid in cursor:
            if host == socket.gethostname():
                try:
                    os.kill(pid, 9)
                    print("Terminate: id = %d, host = %s, pid = %d" % (id, host, pid))
                except ProcessLookupError:
                    print("Terminate: Not such process, id = %d, host = %s, pid = %d" % (id, host, pid))
                db_utils.update(conn, "RESULT", put={"STATUS": "PENDING"}, where={"ID": id})
            else:
                print("Ignore: id = %d, host = %s, pid = %d" % (id, host, pid))
    conn.close()


def clean(args):
    conn = _get_db_conn(args)
    with conn:
        result = db_utils.delete(conn, "RESULT", where={"STATUS": "PENDING"})
        print("Remove %d pending tasks!" % result.rowcount)
    conn.close()

def statistics(args):
    conn = _get_db_conn(args)
    with conn:
        statistics_utils.parse_pandas(conn, args.config)

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    
    func_parser = subparsers.add_parser('terminate')
    func_parser.set_defaults(func=terminate)

    func_parser = subparsers.add_parser('status')
    func_parser.set_defaults(func=status)

    func_parser = subparsers.add_parser('clean')
    func_parser.set_defaults(func=clean)

    func_parser = subparsers.add_parser('statistics')
    func_parser.set_defaults(func=statistics)
    func_parser.add_argument("--config", type=str, default=None, required=True, help="path to the config yml file")

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
