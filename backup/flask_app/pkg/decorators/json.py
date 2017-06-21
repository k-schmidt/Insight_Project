"""
Insight Data Engineering
Kyle Schmidt

Decorator to turn Python dictionaries into json
"""
import functools

from flask import jsonify  # type: ignore


def json(func):
    """
    Generate a JSON response from a database model or a Python dictionary
    Attributes:
        f: function to wrap
    """
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        """
        Wrapped function - turns response into valid json
        """
        response = func(*args, **kwargs)
        status = None
        headers = None

        if isinstance(response, tuple):
            response, status, headers = (response + (None,)
                                         *
                                         (3 - len(response)))
        if not isinstance(response, dict):
            response = response.export_data()

        response = jsonify(response)
        if status is not None:
            response.status_code = status
        if headers is not None:
            response.headers.extend(headers)
        return response
    return wrapped
# def json(func):
#     """
#     Generate a JSON response from a database model or a Python dictionary
#     Attributes:
#         f: function to wrap
#     """
#     @functools.wraps(func)
#     def wrapped(*args, **kwargs):
#         """
#         Wrapped function - turns response into valid json
#         """
#         response = func(*args, **kwargs)
#         response = jsonify(response)
#         if status is not None:
#             response.status_code = status
#         if headers is not None:
#             response.headers.extend(headers)
#         return response
