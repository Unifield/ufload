import cloud
import db

__version__ = '0.4'

# null progress, can be overridden by importers
def _progress(p):
    pass

progress = _progress
