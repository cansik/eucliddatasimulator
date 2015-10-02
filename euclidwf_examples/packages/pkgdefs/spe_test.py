'''
Created on April 25, 2015

@author: martin.melchior
'''
from euclidwf.framework.taskdefs import Executable, Input, Output, ComputingResources

simulate_image=Executable(command="simulate_image",
                          inputs=[Input("catalog_in"), Input("spectra_templates"), 
                                  Input("dispersion_relation"), Input("sensitivity_fct"),
                                  Input("exposure_time"),Input("bkgr_value")], 
                          outputs=[Output("image")],
                          resources=ComputingResources(cores=2, ram=2.0, walltime=4.0)
                          )

extract_spectra=Executable(command="extract_spectra",
                           inputs=["image", "catalog_in", "dispersion_relation", 
                                   "sensitivity_fct","exposure_time"], 
                           outputs=["spectra", "spectra_properties"],
                           )

redshift=Executable(command="redshift",
                    inputs=[Input("spectra"), Input("galaxy_templates"), Input("redshift_corr")], 
                    outputs=[Output("redshifts")],
                    )

redshift_reliability=Executable(command="redshift_reliability",
                                inputs=[Input("spectra"), Input("galaxy_templates"), Input("redshifts")], 
                                outputs=[Output("reliabilities")],
                                )

redshift_catalog=Executable(command="redshift_catalog",
                            inputs=[Input("redshifts"), Input("redshift_reliabilities")], 
                            outputs=[Output("redshift_catalog")],
                            )                                        
