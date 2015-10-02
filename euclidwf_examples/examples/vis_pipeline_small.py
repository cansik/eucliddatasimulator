'''
Created on Mar 26, 2015
@author: martin.melchior
'''


from euclidwf.framework.workflow_dsl import parallel, pipeline

from vis import vis_correct_bias, vis_split_quadrants, vis_combine_quadrants

@parallel(iterable='quadrant')
def vis_correct_quadrant(quadrant, master_bias, control_params):
    
    quadrant_bias_corrected = vis_correct_bias(quadrant=quadrant, 
                                                master_bias=master_bias, 
                                                control_params=control_params)
    
    return quadrant_bias_corrected 
    

@pipeline(outputs=('corrected_exposures', 'hot_pixels_map', 'dead_pixels_map'))
def vis_pipeline(exposures, master_bias, control_params):
        
    quadrants = vis_split_quadrants(exposures=exposures)
    
    quadrants_correction_output = vis_correct_quadrant(quadrant=quadrants, 
                                                        master_bias=master_bias, 
                                                        control_params=control_params)
    
    corrected_exposures, hot_pixels_map, dead_pixels_map = vis_combine_quadrants(inputlist=quadrants_correction_output)
    
    return corrected_exposures, hot_pixels_map, dead_pixels_map
    



if __name__ == '__main__':
    from euclidwf.framework.graph_builder import build_graph
    pydron_graph=build_graph(vis_pipeline)
    print str(pydron_graph)
    