class BaseType:

    def __init__(self, db_type: str, python_type: type):
        self.db_type = db_type
        self.python_type = python_type


Float = BaseType("REAL", float)
Int = BaseType("INTEGER", int)
String = BaseType("TEXT", str)

TypeMap = {
    float: Float,
    int: Int,
    str: String
}
