import pickle

__author__ = 'cansik'

command = ''
input_files = []
output_files = []
resources = None
executable = None


def read_data():
    global command, input_files, output_files, resources, executable
    command = eval('{{command}}')
    input_files = eval("{{input_files}}")
    output_files = eval("{{input_files}}")
    # resources = eval('{{resources}}')
    executable = pickle.loads("""{{executable}}""")


if __name__ == '__main__':
    read_data()
    print('Name: %s' % command)
    print('In: %s' % ', '.join(input_files))
    print('Out: %s' % ', '.join(output_files))
    print('Resources: %s' % executable.resources)
