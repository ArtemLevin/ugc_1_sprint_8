from flask import jsonify

class AppError(Exception):
    status_code = 500
    message = "Internal error"

    def __init__(self, message=None, status_code=None):
        if message:
            self.message = message
        if status_code:
            self.status_code = status_code

    def to_response(self):
        return jsonify({"error": self.message}), self.status_code


class RateLimitExceeded(AppError):
    status_code = 429
    message = "Too many requests"


class DuplicateEvent(AppError):
    status_code = 409
    message = "Event is duplicate"