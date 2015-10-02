Euclid Pipeline Framework
==========================

Installation Instructions for the Euclid Pipeline Runner (EPR) and Test Examples 
----------------------------------------------------------------------------------

We describe the steps to setup the pipeline runner (as it can be used from the command line to test pipeline specifications) and how the example pipelines can be made executed. 
In general, we distinguish two different configurations: 
- Configuration with EPR and the IAL.DRM where the jobs are executed are on the same machine. This is suited for test runs without access or without making use of a HPC infrastructure. This configuration is referred to as 'LOCAL' configuration. 
- Configuration with EPR on the local machine and the IAL.DRM on a remote host (or within a VM on the local machine) that is accessible by ssh. This configuration could be used to execute pipelines with longer running pipeline steps executed in a HPC infrastructure. This configuration is referred to as 'SSH' configuration.  

The following recipe was tested on a pristine LODEEN-1.1-1 with a user named 'user' - by using a 'LOCAL' configuration.


(1) Prepare Filesystem
Define the EUCLID_HOME variable - in the example below the directory /home/user/epr:
export EUCLID_HOME=/home/user/epr
mkdir -p $EUCLID_HOME

Installation Directory : $EUCLID_HOME/install (when checking out from SVN)
mkdir -p $EUCLID_HOME/install
Config Directory	 : $EUCLID_HOME/config
mkdir -p $EUCLID_HOME/config
Workspace Directory: $EUCLID_HOME/workspace (when running the HPC jobs on the same machine) 
mkdir -p $EUCLID_HOME/workspace
Cache Directory	 : $EUCLID_HOME/cache  
mkdir -p $EUCLID_HOME/cache


(2) Prepare euclid-repository access
Make sure that access to the euclid-repo is properly configured. See http://euclid.roe.ac.uk/projects/codeen-users/wiki/Yum-codeen-repo for details.
When starting with LODEEN it should already be configured.
Furthermore, configure the sgsServices-repo:
- Put the sgsservices.repo-file in /etc/yum.repos.d/ --> WHERE DO WE GET THAT FROM ?
- Execute: yum --disablerepo="*" --enablerepo="sgsServices" list available
    

(3) IAL.DRM Installation
Make sure that ial.drm is properly installed on the submission host ('LOCAL' or 'SSH'). See http://euclid.roe.ac.uk/projects/ousdcf/wiki/12092014_IAL_DRM for details.
Currently, we use the trunk version (https://euclid.esac.esa.int/svn/EC/SGS/ST/4-2-03-IAL/drm/trunk/) which is consistent with the RPM in the euclid-repo. 
For the 'LOCAL' configuration, we use the following settings in the IAL.DRM config file (~/.ial_drm/conf.ini):
[SYSTEM]
work_dir_host=<EUCLID_HOME>/workspace
work_dir_node=<EUCLID_HOME>/workspace
drm=LOCAL
log_level=INFO

IMPORTANT: Expand the EUCLID_HOME variable!!

TODO: For challenge 6, we will use a new version of IAL.DRM. 

        
(4) Install EPR and Examples
export PATH=/usr/bin:/usr/sbin:/bin:/sbin

(a) EPR 
Installation from yum-repository:
yum install euclid_wfm

This will install EPR and all its dependencies.

(b) For the most revet version of EPR, you can install EPR from svn:
cd $EUCLID_HOME/install
svn co https://euclid.esac.esa.int/svn/EC/SGS/ST/4-2-03-IAL/wfm/trunk/euclidwf/
cd euclidwf
python setup.py install
export PATH=$EUCLID_HOME/install/euclidwf/bin:$PATH
export PYTHONPATH=$EUCLID_HOME/install/euclidwf/lib/python2.7/site-packages:$PYTHONPATH

Note that this will not install the dependencies of EPR - the dependencies should be installed by performing (a).
 
(c) EPR Examples:
cd $EUCLID_HOME/install
svn co https://euclid.esac.esa.int/svn/EC/SGS/ST/4-2-03-IAL/wfm/trunk/euclidwf_examples/
export PYTHONPATH=$EUCLID_HOME/install/euclidwf_examples/packages/pkgdefs:$PYTHONPATH

    
(5) Prepare EPR Configuration File
Use the example config file included in euclidwf_examples:
cp $EUCLID_HOME/install/euclidwf_examples/resources/test_configuration.cfg $EUCLID_HOME/config/epr_config.cfg
Now customize the contents so that it fits your setup:

For the 'LOCAL' DRM configuration:
drm.protocol=local
drm.hostname=localhost
drm.username=
drm.password=    
ws.protocol=file
ws.hostname=
ws.root=$EUCLID_HOME/epr/workspace
ws.username=
ws.password=
wfm.pkgdefs=$EUCLID_HOME/install/euclidwf_examples/packages/pkgdefs
wfm.scripts=$EUCLID_HOME/install/euclidwf_examples/examples

For the 'SSH' DRM configuration:
drm.protocol=ssh
drm.hostname=<hostname>
drm.username=<user>
drm.password=<password>
ws.protocol=sftp
ws.hostname=<hostname>
ws.root=$EUCLID_HOME/workspace
ws.username=<user>
ws.password=<password>
wfm.pkgdefs=$EUCLID_HOME/install/euclidwf_examples/packages/pkgdefs
wfm.scripts=$EUCLID_HOME/install/euclidwf_examples/examples

Note that the last two properties (wfm.pkgdefs, wfm.scripts) are set for running the examples included in the examples project. If you run your own examples, modify this path accordingly.


(6) Prepare the example for execution
(a) Generate the stub executables by executing:
python exec_stubs_generator.py --pkgdefs="$EUCLID_HOME/install/euclidwf_examples/packages/pkgdefs/" --destdir="$EUCLID_HOME/testexecs"
Now, make sure that exec stubs become executables in the execution environment:
For the 'LOCAL' DRM configuration: 
export PATH=$EUCLID_HOME/testexecs:$PATH

For the 'SSH' DRM configuration:
copy the directory with the test execs ($EUCLID_HOME/testexecs) to the execution host, e.g. to $HOME/testexecs
add the location to the PATH: e.g. export PATH=$HOME/testexecs:$PATH

(b) Generate mock test data:
Use the mock data generator - for the example vis_pipeline.py this looks as follows:
python mock_data_generator.py --pipelinefile=$EUCLID_HOME/install/euclidwf_examples/examples/vis_pipeline.py --destdir=$EUCLID_HOME/workspace/vis --workdir=vis --pkgdefs=$EUCLID_HOME/install/euclidwf_examples/packages/pkgdefs

For the 'LOCAL' configuration no further steps are needed. 
For the 'SSH' configuration, the destination folder needs to be copied to the execution host into the 'workspace' configured for the IAL.DRM (work_dir_host in conf.ini).

    
(7) Run the Example (from the command line)
python pipeline_runner.py --pipeline="vis_pipeline.py" --config="$EUCLID_HOME/config/epr_config.cfg" --data="$EUCLID_HOME/workspace/vis/vis_pipeline.data"
    

(8) Setup and Run from within Eclipse
Check out euclidwf and eucldwf_examples from svn to $EUCLID_HOME/epr/install. See (4b) above.

Create a new PyDev project and set $EUCLID_HOME/epr/install/euclidwf as its source.
Make sure that 'packages' (and optionally 'bin') are specified as source folders. To achieve that, right-click on the project and select "Properties"; then, select 'PyDev - PYTHONPATH' and use 'Add source folder'.

Create a new PyDev project and set $EUCLID_HOME/epr/install/euclidwf_examples as its source.
Make sure that 'examples' and 'packages/pkgdefs' are specified as source folders .

Specify a run configuration with 
main module: bin/pipeline_runner.py (select it by browsing to its location)
arguments: --pipeline="vis_pipeline.py" --config="$EUCLID_HOME/config/epr_config.cfg" --data="$EUCLID_HOME/workspace/vis/vis_pipeline.data"
environment: add variable EUCLID_HOME
