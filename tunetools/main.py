import argparse
import os
import socket
import sqlite3
import time
import shutil
import json

from tunetools import db_utils
from tunetools import decorator


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
                                                                'LIMIT %d)' % args.limit)[0]
        if aver_duration is None:
            aver_duration = 60
            print("WARNING: I don't know the average duration. Use 60min for now.")
        running_task = db_utils.select(conn, "RESULT", project=['ID', 'HOST', 'PID', 'RUN_AT'],
                                       where={"STATUS": "RUNNING"})
        running_task = list(running_task)
        count_pending = db_utils.count(conn, "RESULT", where={"STATUS": "PENDING"})
        count_running = len(running_task)
    print("Running: %d, Pending: %d" % (count_running, count_pending))
    if count_running != 0:
        print("Speed: %lf/h" % (60 / aver_duration * count_running))
        total_left_sec = (count_pending + count_running / 2) * aver_duration / count_running * 60

        def format_time(left_sec):
            left_min = int(left_sec / 60)
            if left_min < 60:
                return "%02d:%02d" % (left_min, left_sec % 60)
            return "%02d:%02d:%02d" % (left_min / 60, left_min % 60, left_sec % 60)

        print("Left Time: %s (%s)" % (format_time(total_left_sec),
                                      time.ctime(time.time() + total_left_sec)))
        print("========================")
        current = time.time()
        for id, host, pid, run_at in running_task:
            this_duration = current - run_at
            print("[%s-%s] %.2lf%%, duration = %s, left = %s\t\t%s" % (
                host, pid, this_duration / aver_duration / 60 * 100,
                format_time(this_duration), format_time(aver_duration * 60 - this_duration),
                _get_last_line(os.path.join('.tune', 'logs', str(id) + '.log')).strip()
            ))

    conn.close()

def _get_last_line(name):
    if not os.path.isfile(name):
        return ''
    with open(name, 'rb') as f:
        file_size = os.path.getsize(name)
        offset = -100
        if file_size == 0:
            return ''
        while True:
            if (abs(offset) >= file_size):
                f.seek(-file_size, 2)
                data = f.readlines()
                return str(data[-1])
            f.seek(offset, 2)
            data = f.readlines()
            if (len(data) > 1):
                return str(data[-1])
            else:
                offset *= 2

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
                    print("Terminate: Not such process, id = %d, host = %s, pid = %d" % (
                        id, host, pid))
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
    from tunetools import statistics
    conn = _get_db_conn(args)
    with conn:
        statistics._parse(conn, args.config, args.formatter, args.csv)

def draw(args):
    from tunetools import statistics
    statistics.draw_with_json(json.load(open(args.config)))


def db(args):
    conn = _get_db_conn(args)
    import pandas, traceback
    while True:
        sql = input(">>> ")
        if sql == 'commit':
            conn.commit()
            conn.close()
            break
        try:
            re = conn.execute(sql)
            try:
                descriptions = list([description[0] for description in re.description])
                content = list(re)
                pd = pandas.DataFrame(content, columns=descriptions)
                print(pd.to_string())
            except:
                print(re.rowcount)
        except:
            traceback.print_exc()


def get_tune_dir_and_backup_dir(args):
    path = os.path.join(os.getcwd(), ".tune")
    if not os.path.isdir(path):
        print("Error: cannot find .tune directory in the current working dir!")
        exit(-1)
    return path, os.path.join(path, args.name)


def copy_file_in_dir(src, dst):
    for n in os.listdir(src):
        source = os.path.join(src, n)
        if os.path.isfile(source):
            target = os.path.join(dst, n)
            shutil.copyfile(source, target)
            print("copy: %s -> %s" % (source, target))


def store(args):
    tune_dir, backup_dir = get_tune_dir_and_backup_dir(args)
    if os.path.isdir(backup_dir):
        response = input(
            "Warning: backup name '%s' exists! Do you want to overwrite it? [y|n]" % args.name)
        if response == 'y':
            shutil.rmtree(backup_dir)
        else:
            exit(0)
    os.mkdir(backup_dir)
    copy_file_in_dir(tune_dir, backup_dir)
    print("store success!")


def restore(args):
    tune_dir, backup_dir = get_tune_dir_and_backup_dir(args)
    if not os.path.isdir(backup_dir):
        print("Error: cannot find backup name: %s" % args.name)
        exit(-1)
    copy_file_in_dir(backup_dir, tune_dir)
    print("restore success!")


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    run_parser = subparsers.add_parser('run',
                                       help='run tasks and record results for each parameter combination')
    run_parser.set_defaults(func=decorator._run)
    run_parser.add_argument('--worker', type=int, default=1, metavar='<worker>',
                            help='number of workers to run in parallel')

    test_parser = subparsers.add_parser('test',
                                        help='run the task by default values for once, but not record')
    test_parser.set_defaults(func=decorator._test)

    plan_parser = subparsers.add_parser('plan',
                                        help='list all the parameter combinations that will be executed')
    plan_parser.set_defaults(func=decorator._plan)

    for subparser in [run_parser, test_parser, plan_parser]:
        subparser.add_argument('python_file', type=str, default=None, metavar='<py_file>',
                               help='a python file recording the experiment tuning configuarions')
        subparser.add_argument('--inject', nargs='*', type=str, default=[], metavar='param:value',
                               help='k-v pairs that injected into parameters')

    terminate_parser = subparsers.add_parser('terminate',
                                             help='terminate all the running processes')
    terminate_parser.set_defaults(func=terminate)

    status_parser = subparsers.add_parser('status',
                                          help='show the running status')
    status_parser.set_defaults(func=status)
    status_parser.add_argument('--limit', type=int, default=20, metavar='<limit>')

    store_parser = subparsers.add_parser('store',
                                         help='backup records')
    store_parser.set_defaults(func=store)
    store_parser.add_argument('name', type=str, default=None, metavar='<name>')

    restore_parser = subparsers.add_parser('restore',
                                           help='restore backups')
    restore_parser.set_defaults(func=restore)
    restore_parser.add_argument('name', type=str, default=None, metavar='<name>')

    clean_parser = subparsers.add_parser('clean',
                                         help='remove all non-terminated tasks')
    clean_parser.set_defaults(func=clean)

    db_parser = subparsers.add_parser('db',
                                      help='execute sql on the record database')
    db_parser.set_defaults(func=db)

    statistics_parser = subparsers.add_parser('statistics',
                                              help='do statistics')
    statistics_parser.set_defaults(func=statistics)
    statistics_parser.add_argument("config", type=str, default=None, metavar='<config_file>',
                                   help="path to the config yml file")
    statistics_parser.add_argument("--formatter", type=str, default="[{count}] {mean:.4f}±{std:.4f}",
                                   help="number formatter. Default: [{count}] {mean:.4f}±{std:.4f}")
    statistics_parser.add_argument("--csv", action='store_true',
                                   help="print as csv format")

    statistics_parser = subparsers.add_parser('draw',
                                              help='draw with the json from statistics')
    statistics_parser.set_defaults(func=draw)
    statistics_parser.add_argument("config", type=str, default=None, metavar='<json_file>',
                                   help="path to the json file")

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
