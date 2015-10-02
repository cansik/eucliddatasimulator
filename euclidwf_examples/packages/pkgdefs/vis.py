'''
Created on Mar 6, 2015

@author: martin.melchior
'''
from euclidwf.framework.taskdefs import Executable, Input, Output, \
                TYPE_LISTFILE, MIME_TXT, ComputingResources

vis_split_quadrants=Executable(command="vis_split_quadrants",
                               inputs=[Input("exposures")], 
                               outputs=[Output("quadrants_list", mime_type=MIME_TXT, content_type=TYPE_LISTFILE)]
                               )

vis_correct_bias=Executable(command="vis_correct_bias",
                            inputs=[Input("quadrant"), Input("master_bias"), Input("control_params")], 
                            outputs=[Output("corrected_frame")],
                            resources=ComputingResources(cores=2, ram=2.0, walltime=4.0)
                            )

vis_correct_nonlin=Executable(command="vis_correct_nonlin",
                              inputs=[Input("quadrant"), Input("nonlin_model"), Input("control_params")], 
                              outputs=[Output("corrected_frame")]
                              )

vis_correct_cti=Executable(command="vis_correct_cti",
                           inputs=[Input("quadrant"), Input("cti_model"), Input("control_params")], 
                           outputs=[Output("corrected_frame")]
                           )

vis_correct_dark=Executable(command="vis_correct_dark",
                            inputs=[Input("quadrant"), Input("master_dark"), Input("control_params")], 
                            outputs=[Output("corrected_frame"), Output("hot_pixels_map")]
                            )

vis_correct_flat=Executable(command="vis_correct_flat",
                            inputs=[Input("quadrant"), Input("master_flat"), Input("control_params")], 
                            outputs=[Output("corrected_frame"), Output("dead_pixels_map")],
                            )

vis_correct_illum=Executable(command="vis_correct_illum",
                             inputs=[Input("quadrant"), Input("master_illum"), Input("control_params")], 
                             outputs=[Output("corrected_frame"), Output("dead_pixels_map")],
                             )

vis_combine_quadrants=Executable(command="vis_combine_quadrants",
                                 inputs=[Input("inputlist", content_type=TYPE_LISTFILE)], 
                                 outputs=[Output("illum_corrected"), Output("hot_pixels_map"), Output("dead_pixels_map")],
                                 )

vis_combine_exposures=Executable(command="vis_combine_exposures",
                                 inputs=[Input("frames", content_type=TYPE_LISTFILE)], 
                                 outputs=[Output("exposures_list")],
                                 )

vis_combine_detectors=Executable(command="vis_combine_detectors",
                                 inputs=[Input("inputlist", content_type=TYPE_LISTFILE)], 
                                 outputs=[Output("frame"), Output("bkgr_map"), Output("noise_map"), Output("cosmic_rays_map"), Output("weights_map")],
                                 )

vis_flag_ghosts=Executable(command="vis_flag_ghosts",
                           inputs=[Input("exposure"), Input("ghost_model"), Input("control_params")], 
                           outputs=[Output("exposure"), Output("ghost_flag_map")],
                           )

vis_split_detectors=Executable(command="vis_split_detectors",
                               inputs=[Input("exposures")], 
                               outputs=[Output("detector_frames", mime_type=MIME_TXT, content_type=TYPE_LISTFILE)],
                               )

vis_calib_background=Executable(command="vis_calib_background",
                                inputs=[Input("frame"), Input("scattered_light_model"), Input("control_params")], 
                                outputs=[Output("output_frame"), Output("bkgr_map"), Output("noise_map")],
                                )

vis_flag_scattered=Executable(command="vis_flag_scattered",
                              inputs=[Input("frame"), Input("scattered_light_model"), Input("control_params")], 
                              outputs=[Output("output_frame"), Output("cosmic_rays_map"), Output("weights_map")],
                              )

vis_psf=Executable(command="vis_psf",
                   inputs=[Input("exposure"), Input("control_params")], 
                   outputs=[Output("psf_model")],
                   )

