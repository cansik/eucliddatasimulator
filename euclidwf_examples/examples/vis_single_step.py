'''
Created on Mar 26, 2015
@author: martin.melchior
'''


from euclidwf.framework.workflow_dsl import pipeline
from vis import vis_correct_dark

@pipeline(outputs=('quadrant_dark_corr', 'hot_pixels_map'))
def vis_single_step(quadrant, master_dark, control_params):
    quadrant_dark_corr, hot_pixels_map = vis_correct_dark(quadrant=quadrant, 
                                                          master_dark=master_dark, 
                                                          control_params=control_params)    
    return quadrant_dark_corr, hot_pixels_map



if __name__ == '__main__':
    from euclidwf.framework.graph_builder import build_graph
    print build_graph(vis_single_step)
