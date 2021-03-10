from .search_types import *


class BaseSearchSpace:

    def __init__(self, name: str, base_type: BaseType, default):
        self.name = name
        self.db_name = "param_" + name
        self.base_type = base_type
        self.default = default

    def sample(self) -> list:
        pass


class GridSearchSpace(BaseSearchSpace):

    def __init__(self, name: str, base_type: BaseType, default, domain: list):
        super().__init__(name, base_type, default)
        self.domain = domain
        for x in domain:
            if type(x) != base_type.python_type:
                raise ValueError("type doesn't match: require " + str(
                    base_type.python_type) + ", but find " + str(x))

    def sample(self) -> list:
        return self.domain
