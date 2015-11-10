'''
Created on Apr 21, 2015

@author: martin.melchior
'''


class RunConfigurationError(StandardError):
    pass


class ConfigurationError(StandardError):
    pass


class PipelineFrameworkError(StandardError):
    pass


class PipelineSpecificationError(StandardError):
    pass


class PipelineGraphError(StandardError):
    pass


class ProcessingError(StandardError):
    pass


class DrmError(StandardError):
    pass
