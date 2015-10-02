#!/bin/sh

# [Cyril Halmo]

if [ -z "$1" ] || ( [ $2 ] && [ $2 != "--yum" ] )
then
    echo "Usage: $0 euclid_home_directory [ --yum ]
            Default is svn checkout."
    exit 0
fi

## Prepare Filesystem

if [[ $1 == /* ]]
then
    export EUCLID_HOME=$1
else
    export EUCLID_HOME="$(pwd)/$1"
fi

exit 0

mkdir -p $EUCLID_HOME
mkdir -p $EUCLID_HOME/install/ial $EUCLID_HOME/install/epr $EUCLID_HOME/config $EUCLID_HOME/workspace $EUCLID_HOME/cache
mkdir ~/.ial_drm
touch ~/.ial_drm/conf.ini


if [ ! $2 ]
then
    # Install dependecies if sources are from svn

    sudo yum -y install python-imaging-tk
    sudo yum -y install graphviz-devel
    sudo easy_install --upgrade six
    sudo easy_install pygraphviz
    svn co https://euclid.esac.esa.int/svn/EC/SGS/ST/4-2-03-IAL/external/pydron/tags/SNAPSHOT_JUNE15/ $EUCLID_HOME/install/epr/pydron
    cd $EUCLID_HOME/install/epr/pydron/
    sudo python setup.py install
fi
    


if [ $2 ]
then
    ## Prepare euclid-repository access
    sudo echo "[sgsServices]
    name=sgsServices
    baseurl=https://userread:password@apceuclidrepo.in2p3.fr/nexus/content/repositories/el7.sgsservices
    enabled=1
    protect=0
    gpgcheck=0
    metadata_expire=10s
    autorefresh=1
    type=rpm-md
    sslcacert=/etc/pki/tls/certs/CNRS2-Standard.pem" >> /etc/yum.repos.d/sgsservices.repo

    yum --disablerepo="*" --enablerepo="sgsServices" list available
else
    cd $EUCLID_HOME/install/epr
    svn co https://euclid.esac.esa.int/svn/EC/SGS/ST/4-2-03-IAL/drm/trunk/ $EUCLID_HOME/install/epr/drm
    cd $EUCLID_HOME/install/epr/drm/
    sudo python setup.py install
fi


## IAL.DRM Installation

echo "[SYSTEM]
work_dir_host=$EUCLID_HOME/workspace
work_dir_node=$EUCLID_HOME/workspace
drm=LOCAL
log_level=INFO" >> ~/.ial_drm/conf.ini


## Install EPR and Examples

export PATH=/usr/bin:/usr/sbin:/bin:/sbin

# EPR

if [ $2 ]
then
    sudo yum install euclid_wfm
else
    cd $EUCLID_HOME/install/epr
    svn co https://euclid.esac.esa.int/svn/EC/SGS/ST/4-2-03-IAL/wfm/trunk/euclidwf/
    cd $EUCLID_HOME/install/epr/euclidwf/
    sudo python setup.py install
    export PATH=$EUCLID_HOME/install/epr/euclidwf/bin:$PATH
    export PYTHONPATH=$EUCLID_HOME/install/epr/euclidwf/lib/python2.7/site-packages:$PYTHONPATH
fi


# EPR Examples

cd $EUCLID_HOME/install/epr
svn co https://euclid.esac.esa.int/svn/EC/SGS/ST/4-2-03-IAL/wfm/trunk/euclidwf_examples/
export PYTHONPATH=$EUCLID_HOME/install/epr/euclidwf_examples/packages/pkgdefs:$PYTHONPATH


## Prepare EPR Configuration File

rm $EUCLID_HOME/config/epr_config.cfg

echo "drmConfig.protocol=local
drmConfig.host=localhost
drmConfig.port=
drmConfig.statusCheckPollTime=5.0
drmConfig.statusCheckTimeout=7200

credentials.drmUsername=
credentials.drmPassword=
credentials.wsUsername=
credentials.wsPassword=

wsConfig.protocol=file
wsConfig.host=mo
wsConfig.port=
wsConfig.workspaceRoot=$EUCLID_HOME/workspace

localcache=$EUCLID_HOME/cache
pkgRepository=$EUCLID_HOME/install/epr/euclidwf_examples/packages/pkgdefs
pipelineDir=$EUCLID_HOME/install/epr/euclidwf_examples/examples" >> $EUCLID_HOME/config/epr_config.cfg


## Prepare the example for execution

chmod +x $EUCLID_HOME/install/epr/euclidwf/bin/*
export PATH=$EUCLID_HOME/testexecs:$EUCLID_HOME/install/epr/euclidwf/bin:$PATH

# Generate the stub executables

exec_stubs_generator.py --pkgdefs="$EUCLID_HOME/install/epr/euclidwf_examples/packages/pkgdefs/" --destdir="$EUCLID_HOME/testexecs" --xml

# Generate mock test data

mock_data_generator.py --pipelinefile=$EUCLID_HOME/install/epr/euclidwf_examples/examples/vis_pipeline.py --destdir=$EUCLID_HOME/workspace/vis --workdir=vis --pkgdefs=$EUCLID_HOME/install/euclidwf_examples/packages/pkgdefs


## Run the Example

pipeline_runner.py --pipeline="vis_pipeline.py" --config="$EUCLID_HOME/config/epr_config.cfg" --data="$EUCLID_HOME/workspace/vis/vis_pipeline.dat"
