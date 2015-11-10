__author__ = 'cansik'

command = ''
input_files = []
output_files = []


def read_data():
    global command, input_files, output_files
    command = eval('{{command}}')
    input_files = eval("{{input_files}}")
    output_files = eval("{{input_files}}")


if __name__ == '__main__':
    read_data()
    print('Name: %s' % command)
    print('In: %s' % ', '.join(input_files))
    print('Out: %s' % ', '.join(output_files))
