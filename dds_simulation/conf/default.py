#from backports import configparser
import configparser
import os

PROJECT_ROOT = os.path.join(os.getenv('HOME'), 'projects', 'DDS-Simulation')

CONFIGFILE = os.path.join(PROJECT_ROOT, 'etc', 'datastore.ini')
CONF = None


def getconfig():
    CONF = configparser.RawConfigParser()
    return CONF


def parameter(section, param):
    config = CONF or getconfig()
    config.read(CONFIGFILE)
    return config.get(section, param)
