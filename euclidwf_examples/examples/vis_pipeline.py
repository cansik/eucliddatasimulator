'''
Created on Mar 26, 2015
@author: martin.melchior
'''
from euclidwf.framework.workflow_dsl import parallel, pipeline

from vis import vis_correct_bias, vis_correct_dark, vis_correct_cti, vis_correct_nonlin, \
                vis_correct_flat, vis_correct_illum, vis_calib_background, vis_flag_scattered, \
                vis_combine_quadrants, vis_combine_detectors, vis_split_detectors,\
                vis_split_quadrants


@parallel(iterable='quadrant')
def vis_correct_quadrant(quadrant, master_bias, nonlin_model, cti_model, master_dark, 
                         master_flat, master_illum, illum_model, control_params):
    
    quadrant_bias_corrected = vis_correct_bias(quadrant=quadrant, 
                                                master_bias=master_bias, 
                                                control_params=control_params)
    
    quadrant_nonlin_corrected = vis_correct_nonlin(quadrant=quadrant_bias_corrected, 
                                                    nonlin_model=nonlin_model, 
                                                    control_params=control_params)

    quadrant_cti_corrected = vis_correct_cti(quadrant=quadrant_nonlin_corrected, 
                                              cti_model=cti_model, 
                                              control_params=control_params)

    quadrant_dark_corrected, hot_pixels_map = vis_correct_dark(quadrant=quadrant_cti_corrected, 
                                                               master_dark=master_dark, 
                                                               control_params=control_params)

    quadrant_flat_corrected, dead_pixels_map1 = vis_correct_flat(quadrant=quadrant_dark_corrected, 
                                                                master_flat=master_flat, 
                                                                control_params=control_params)
    
    quadrant_illum_corrected, dead_pixels_map2 = vis_correct_illum(quadrant=quadrant_flat_corrected, 
                                                  master_illum=master_illum, 
                                                  illum_model=illum_model, 
                                                  control_params=control_params)

    return quadrant_illum_corrected, hot_pixels_map, dead_pixels_map1

@parallel(iterable='frame')
def vis_correct_detectors(frame, scattered_light_model, control_params):
    
    frame, bkgr_map, noise_map = vis_calib_background(frame=frame, 
                                                      scattered_light_model=scattered_light_model, 
                                                      control_params=control_params)

    frame, cosmic_rays_map, weights_map = vis_flag_scattered(frame=frame, 
                                                             scattered_light_model=scattered_light_model, 
                                                             control_params=control_params)
    
    return frame, bkgr_map, noise_map, cosmic_rays_map, weights_map
    

@pipeline(outputs=('frame', 'bkgr_map', 'noise_map', 'cosmic_rays_map', 'weights_map', 'hot_pixels_map', 'dead_pixels_map'))
def vis_pipeline(exposures, master_bias, nonlin_model, cti_model, master_dark, master_flat, 
                            master_illum, illum_model, scattered_light_model,control_params):
        
    quadrants = vis_split_quadrants(exposures=exposures)
    
    quadrants_correction_output = vis_correct_quadrant(quadrant=quadrants, master_bias=master_bias, 
                                                        nonlin_model=nonlin_model, cti_model=cti_model, 
                                                        master_dark=master_dark, master_flat=master_flat, 
                                                        master_illum=master_illum, illum_model=illum_model, 
                                                        control_params=control_params)
    
    corrected_exposures, hot_pixels_map, dead_pixels_map = vis_combine_quadrants(inputlist=quadrants_correction_output)
    
    detector_frames = vis_split_detectors(exposures=corrected_exposures)
    
    vis_correct_detectors_output = vis_correct_detectors(frame=detector_frames, 
                                                          scattered_light_model=scattered_light_model, 
                                                          control_params=control_params)
    
    frame, bkgr_map, noise_map, cosmic_rays_map, weights_map = vis_combine_detectors(inputlist=vis_correct_detectors_output)
    return frame, bkgr_map, noise_map, cosmic_rays_map, weights_map, hot_pixels_map, dead_pixels_map
    



if __name__ == '__main__':
    #for pkg,execs in workflow_dsl.get_invoked_execs(vis_pipeline).iteritems():
    #    print str(pkg)
    #    for ex in execs:
    #        print "    "+ex
    
    from euclidwf.framework.graph_builder import build_graph
    from euclidwf.utilities import visualizer
    pydron_graph=build_graph(vis_pipeline)
    print str(pydron_graph)    
    visualizer.visualize_graph(pydron_graph)
    
