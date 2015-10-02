'''
Created on Mar 26, 2015
@author: martin.melchior
'''


from euclidwf.framework.workflow_dsl import pipeline
from vis import vis_correct_dark, vis_correct_flat

@pipeline(outputs=('quadrant_flat_corr', 'hot_pixels_map', 'dead_pixels_map'))
def vis_two_steps(quadrant, master_dark, master_flat, control_params):

    quadrant_dark_corr, hot_pixels_map = vis_correct_dark(quadrant=quadrant, 
                                                          master_dark=master_dark, 
                                                          control_params=control_params)
    quadrant_flat_corr, dead_pixels_map = vis_correct_flat(quadrant=quadrant_dark_corr, 
                                                           master_flat=master_flat, 
                                                           control_params=control_params)
    
    return quadrant_flat_corr, hot_pixels_map, dead_pixels_map



if __name__ == '__main__':
    from euclidwf.framework.graph_builder import build_graph
    print build_graph(vis_two_steps)
