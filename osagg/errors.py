"""PEP 249 exception hierarchy."""


class Warning(Exception):  # noqa: A001  (PEP 249 name)
    pass


class Error(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class PushdownError(NotSupportedError):
    """Raised when a query cannot be pushed down and would need a full scan
    larger than the configured safety cap."""
