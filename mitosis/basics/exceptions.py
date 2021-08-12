class MitosisException(Exception):
    pass


class FlowValidationException(MitosisException):
    pass


class PersistentCellValidationException(MitosisException):
    pass


class FlowIntegrationException(MitosisException):
    pass


class KeyNotUniqueException(MitosisException):
    def __init__(self, key):
        self.key = key

# NOTE: Is it better to throw a standard keyerror?
class KeyNotPresentException(MitosisException, KeyError):
    pass

class AppException(MitosisException):
    pass
