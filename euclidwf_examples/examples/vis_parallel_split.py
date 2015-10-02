'''
Created on Mar 26, 2015
@author: martin.melchior
'''


from euclidwf.framework.workflow_dsl import pipeline, parallel
from vis import vis_correct_dark, vis_correct_flat, vis_split_quadrants, vis_combine_quadrants

@parallel(iterable='quadrant')
def correct(quadrant, master_dark, master_flat, control_params):

    quadrant_dark_corr, hot_pixels_map = vis_correct_dark(quadrant=quadrant, 
                                                          master_dark=master_dark, 
                                                          control_params=control_params)
    quadrant_flat_corr, dead_pixels_map = vis_correct_flat(quadrant=quadrant_dark_corr, 
                                                           master_flat=master_flat, 
                                                           control_params=control_params)
    
    return quadrant_flat_corr, hot_pixels_map, dead_pixels_map

@pipeline(outputs=('corrected_exposures', 'hot_pixels_map', 'dead_pixels_map'))
def vis_two_steps_nested(exposures, master_dark, master_flat, control_params):
    quadrants = vis_split_quadrants(exposures=exposures)
    quadrants_correction_output = correct(quadrant=quadrants, master_dark=master_dark, 
                                          master_flat=master_flat, control_params=control_params)    
    corrected_exposures, hot_pixels_map, dead_pixels_map = vis_combine_quadrants(inputlist=quadrants_correction_output)
    return corrected_exposures, hot_pixels_map, dead_pixels_map


if __name__ == '__main__':
    from euclidwf.framework.graph_builder import build_graph
    print build_graph(vis_two_steps_nested)
