import os.path
from configparser import ConfigParser

path_cur_dir = os.path.dirname(__file__)
file = os.path.join(path_cur_dir, 'database.ini')


def config(filename=file, section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} not found in the {1} file'.format(section, filename)
        )

    return db
