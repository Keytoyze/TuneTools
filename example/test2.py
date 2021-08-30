
class Data(int):
    def __init__(self, t: int):
        super().__init__()
        self.t = t

def t(a: Data(6), b, c):
    pass


if __name__ == '__main__':
    import inspect
    inspect.signature(t)
    pass