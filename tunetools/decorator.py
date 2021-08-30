import os

import tunetools as tt
import inspect


def _on_error(msg):
    print(msg)
    exit(-1)


def _check_env():
    if 'tunetools_env' not in globals():
        _on_error("usage: tunetools {run,test,plan} python_file ...")


def main(num_sample: int = 1):
    def decorate(function):
        _check_env()

        globals()['__num_sample'] = num_sample
        globals()['__main'] = function
        signature = inspect.signature(function)
        search_spaces = []
        for parameter_name, p in signature.parameters.items():
            annotation = p.annotation
            search_spaces.append(
                tt.GridSearchSpace(parameter_name, annotation.default, annotation.domain))
        globals()['__parameters'] = search_spaces

        return function

    return decorate


def filtering(function):
    _check_env()

    globals()['__filtering'] = function

    return function


def onfinish(function):
    _check_env()

    globals()['__onfinish'] = function

    return function


def _check_parameter(signature, params, name):
    for func_param in signature.parameters:
        if func_param not in params:
            _on_error(
                "Function parameter '%s' has not been registered in @parameters menifest." %
                func_param)
    for deco_param in params:
        if deco_param not in signature.parameters:
            _on_error(
                "Decorator parameter '%s' should also be included in the parameters "
                "of the %s function." % (deco_param, name))


def _prepare_env(args):
    globals()['tunetools_env'] = 0
    exec(open(os.path.abspath(args.python_file)).read())

    if '__main' not in globals():
        _on_error("You should specify the main function, and decorate with @main.")
    params = globals().get('__parameters', [])
    params = dict((x.name, x) for x in params)

    import inspect
    _check_parameter(inspect.signature(globals()['__main']), params, "@main")
    if '__filtering' in globals():
        _check_parameter(inspect.signature(globals()['__filtering']), params, "@filtering")

    inject_strings = args.inject
    inject_dict = dict([x.split(':') for x in inject_strings])
    for inject_param, inject_value in inject_dict.items():
        if inject_param not in params:
            _on_error("Injected parameter '%s' has not been registered in @parameters menifest."
                      % inject_param)
        if not params[inject_param].check_type(inject_value):
            _on_error("Injected parameter '%s:%s' doesn't match the type defined in "
                      "@parameters menifest." % (
                          inject_param, inject_value
                      ))

    return inject_dict


def _run(args):
    injects = _prepare_env(args)
    tt.run(globals()['__main'],
           filter_function=globals().get('__filtering', None),
           num_sample=globals().get('__num_sample', 1),
           parameters=globals().get('__parameters', []),
           force_values=injects,
           on_finish_function=globals().get('__onfinish', None))


def _test(args):
    injects = _prepare_env(args)
    tt.test(globals()['__main'],
            parameters=globals().get('__parameters', []),
            force_values=injects)


def _plan(args):
    injects = _prepare_env(args)
    tt.plan(filter_function=globals().get('__filtering', None),
            num_sample=globals().get('__num_sample', 1),
            parameters=globals().get('__parameters', []),
            force_values=injects)
