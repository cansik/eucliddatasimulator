import os, errno
import stat

from jinja2 import Template


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def read_template(template_name):
    template_path = os.path.join(os.path.dirname(__file__), template_name)
    with open(template_path, 'r') as template_file:
        data = template_file.read()
        return Template(data)


def write_all_text(file_path, content):
    with open(file_path, 'w') as outputfile:
        outputfile.write(content)
    os.chmod(file_path, stat.S_IRWXU)
