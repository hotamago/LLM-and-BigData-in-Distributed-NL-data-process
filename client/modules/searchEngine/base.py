from modules.managerKey import ManagerKey
from typing import Dict


class BaseConfigSearchEngine:
    def __init__(self):
        pass

    def _getConfig(self, *arg, **kwarg) -> Dict:
        raise NotImplementedError

    def getConfig(self, *arg, **kwarg) -> Dict:
        return self._getConfig(arg, kwarg)


class BaseSearchEngine:
    def __init__(self, keyManager: ManagerKey):
        self.keyManager = keyManager

    def _query(self, config: BaseConfigSearchEngine):
        raise NotImplementedError

    def query(self, config: BaseConfigSearchEngine):
        return self._query(config)
