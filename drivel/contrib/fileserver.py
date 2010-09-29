import mimetypes
import os

__all__ = ['StaticFileServer']


class StaticFileServer(object):
    """For testing purposes only. Use a real static file server.
    """
    def __init__(self, directory_list, wrapped_app, host):
        self.host = host
        self.host.log("httpd", "info", "serving static files: %s" %
            directory_list)
        self.directory_list = [os.path.realpath(x) for x in directory_list]
        self.wrapped_app = wrapped_app

    def __call__(self, env, start_response):
        for directory in self.directory_list:
            path = os.path.realpath(directory + env['PATH_INFO'])
            if not path.startswith(directory):
                start_response("403 Forbidden",
                    [('Content-Type', 'text/plain')])
                return ['Forbidden']
            if os.path.isdir(path):
                path = os.path.join(path, 'index.html')
            if os.path.exists(path):
                content_type, encoding = mimetypes.guess_type(path)
                if content_type is None:
                    content_type = 'text/plain'
                start_response("200 OK", [('Content-Type', content_type)])
                return file(path).read()
        return self.wrapped_app(env, start_response)
