from euclidwf.framework.taskdefs import Executable, Input, Output, ComputingResources

extract=Executable(command="extract",
                   inputs=[Input("image"), Input("spectra")], 
                   outputs=[Output("catalog"), Output("quality")],
                   resources=ComputingResources(cores=2, ram=2.0, walltime=4.0)
                   )

