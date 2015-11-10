def _append_to_file(outfile, inputname, infile):
    outfile.write("    -------------------------------------\n")
    outfile.write("    Input (port: %s):\n" % inputname)
    outfile.write("    -------------------------------------\n")
    outfile.write("    -------------------------------------\n")
    for line in infile:
        outfile.write("        %s" % line)
