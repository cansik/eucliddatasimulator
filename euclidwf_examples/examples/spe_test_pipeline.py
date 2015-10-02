'''
Created on Mar 25, 2015
@author: martin.melchior
'''
from euclidwf.framework.workflow_dsl import nested, pipeline

from spe_test import simulate_image, extract_spectra, redshift, \
                                        redshift_reliability, redshift_catalog
from euclidwf.utilities import visualizer

@nested()
def simulate(catalog_in, spectra_templates, dispersion_relation, sensitivity_fct, 
                              exposure_time, bkgr_value):
    sim_image = simulate_image(catalog_in=catalog_in, 
                                spectra_templates=spectra_templates,
                                dispersion_relation=dispersion_relation, 
                                sensitivity_fct=sensitivity_fct,
                                exposure_time=exposure_time, 
                                bkgr_value=bkgr_value)

    sim_image2 = simulate_image(catalog_in=catalog_in, 
                                spectra_templates=spectra_templates,
                                dispersion_relation=dispersion_relation, 
                                sensitivity_fct=sensitivity_fct,
                                exposure_time=exposure_time, 
                                bkgr_value=bkgr_value)
    return sim_image, sim_image2

@nested()
def spectrum_redshift(spectra, galaxy_templates, redshift_corr):

    redshifts = redshift(spectra=spectra, 
                          galaxy_templates=galaxy_templates, 
                          redshift_corr=redshift_corr)
    
    reliabilities = redshift_reliability(spectra=spectra, 
                                          galaxy_templates=galaxy_templates, 
                                          redshifts=redshifts)
    
    return redshifts, reliabilities

@pipeline(outputs=('image1','image2','spectra','spectral_properties','catalog'))
def simulate_and_reduce_image(catalog_in, spectra_templates, dispersion_relation, sensitivity_fct, 
                              exposure_time, bkgr_value, galaxy_templates, redshift_corr):

    sim_image, sim_image2 = simulate(catalog_in=catalog_in, 
                                spectra_templates=spectra_templates,
                                dispersion_relation=dispersion_relation, 
                                sensitivity_fct=sensitivity_fct,
                                exposure_time=exposure_time, 
                                bkgr_value=bkgr_value)

    spectra, spectra_props = extract_spectra(image=sim_image, 
                                             catalog_in=catalog_in,
                                             dispersion_relation=dispersion_relation, 
                                             sensitivity_fct=sensitivity_fct,
                                             exposure_time=exposure_time)

    redshifts, reliabilities = spectrum_redshift(spectra=spectra, 
                                                 galaxy_templates=galaxy_templates, 
                                                 redshift_corr=redshift_corr)

    catalog = redshift_catalog(redshifts=redshifts,
                                redshift_reliabilities=reliabilities)

    return sim_image, sim_image2, spectra, spectra_props, catalog 


if __name__ == '__main__':
    from euclidwf.framework.graph_builder import build_graph
    pydron_graph=build_graph(simulate_and_reduce_image)
    print str(pydron_graph)    
    visualizer.visualize_graph(pydron_graph)
    
            
    
