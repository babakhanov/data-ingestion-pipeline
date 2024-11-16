import re


def camel_to_snake(camel_case_str):
    return re.sub(r'([a-z])([A-Z])', r'\1_\2', camel_case_str).lower()


def no_cache_key(*args, **kwargs):
    """Custom cache key function that disables caching"""
    return None
