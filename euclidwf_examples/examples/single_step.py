from euclidwf.framework.workflow_dsl import pipeline
from test_package import extract

@pipeline(outputs=('catalog', 'quality'))
def single_step(image, spectra):
    catalog, quality = extract(image=image, spectra=spectra)    
    return catalog, quality
