import argparse
import os
import socket
import sqlite3
import time

from tunetools import db_utils


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
                os.kill(pid, 9)
                db_utils.update(conn, "RESULT", put={"STATUS": "PENDING"}, where={"ID": id})
                print("Terminate: id = %d, host = %s, pid = %d" % (id, host, pid))
            else:
                print("Ignore: id = %d, host = %s, pid = %d" % (id, host, pid))
    conn.close()


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    subparser_list = [
        ('terminate', terminate),
        ('status', status),
    ]
    for name, func in subparser_list:
        argparser = subparsers.add_parser(name)
        argparser.set_defaults(func=func)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
