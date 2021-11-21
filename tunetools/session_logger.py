import sys, os, shutil


class RedirectLogger:

    def __init__(self, path, std_out):
        self.path = path
        self.stdout = std_out

    def write(self, string):
        self.stdout.write(string)
        with open(self.path, 'a') as f:
            f.write(string)

    def flush(self):
        self.stdout.flush()


def _start(run_id):
    dir_path = os.path.join(".tune", "logs")
    path = os.path.join(dir_path, str(run_id) + ".log")
    if not os.path.isdir(dir_path):
        os.makedirs(dir_path)
    if not os.path.isfile(path):
        open(path, mode="w").close()
    sys.stdout = RedirectLogger(path, sys.stdout)
    sys.stderr = RedirectLogger(path, sys.stderr)


def _end(should_remove_file):
    if type(sys.stdout) == RedirectLogger:
        if should_remove_file:
            os.remove(sys.stdout.path)
        sys.stdout = sys.stdout.stdout
        sys.stderr = sys.stderr.stdout
