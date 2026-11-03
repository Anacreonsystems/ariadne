"""Shared refusal type for native stream server authorities."""


class NativeStreamError(RuntimeError):
    """A named protocol refusal; HTTP mapping belongs to the transport layer."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)
