import sys
import sqlite3
import socket
import os
import time
import utils

if __name__ == "__main__":
    command = sys.argv[1]

    if command == "terminate":
        file = sys.argv[2]

        conn = sqlite3.connect(file)
        with conn:
            cursor = utils.select(conn, "RESULT", ["ID", "HOST", "PID"],
                                  where={"STATUS": "RUNNING"})
            for id, host, pid in cursor:
                if host == socket.gethostname():
                    os.kill(pid, 9)
                    utils.update(conn, "RESULT", put={"STATUS": "PENDING"}, where={"ID": id})
                    print("Terminate: id = %d, host = %s, pid = %d" % (id, host, pid))
                else:
                    print("Ignore: id = %d, host = %s, pid = %d" % (id, host, pid))
        conn.close()

    elif command == "status":
        file = sys.argv[2]

        conn = sqlite3.connect(file)
        with conn:
            aver_duration = utils.execute_sql_return_first(conn, 'SELECT AVG(DURATION_MIN) FROM '
                                                                 '(SELECT DURATION_MIN FROM RESULT '
                                                                 'WHERE STATUS = "TERMINATED" '
                                                                 'ORDER BY RUN_AT DESC '
                                                                 'LIMIT 20)')[0]
            count_running = utils.count(conn, "RESULT", where={"STATUS": "RUNNING"})
            count_pending = utils.count(conn, "RESULT", where={"STATUS": "PENDING"})
        print("Running: %d, Pending: %d" % (count_running, count_pending))
        if count_running != 0:
            print("Speed: %lf/h" % (60 / aver_duration * count_running))
            total_left_min = (int)(
                (count_pending + count_running / 2) * aver_duration / count_running)

            print("Left Time: %dh%dmin (%s)" % (total_left_min / 60, total_left_min % 60,
                                                time.ctime(time.time() + total_left_min * 60)))

        conn.close()
