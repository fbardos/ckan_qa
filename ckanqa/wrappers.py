import functools

from ckanqa.context import CkanContext


# TODO: This should be obsolete now...
def save_ckan_context_in_redis(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        self.ckan_context = CkanContext.load_object_from_redis(kwargs['context'])
        try:
            return func(self, *args, **kwargs)
        finally:
            self.ckan_context.save_object_to_redis(kwargs['context'])
    return wrapper
