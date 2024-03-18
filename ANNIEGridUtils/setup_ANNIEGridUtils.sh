#!/bin/bash 

function setup_fnal_security() {
    # Remove your old certificate for good measure
    if [ -f /tmp/x509up_u$(id -u) ]; then
        rm /tmp/x509up_u$(id -u)
    fi

    # Get a new certificate
    RETRY=0
    while ! kx509 ; do
        let RETRY=RETRY+1
        if [ $RETRY -gt 3 ]; then
            echo "Failed 3 times. Aborting." 
            exit 1
        fi

        echo "Failed to get a certificate. You probably need to kinit." 
    done

    # Check the VOMS proxy
    if [ -z "`voms-proxy-info -all|grep "^attribute"`" ]; then
        echo "No valid VOMS proxy found, getting one" 
        voms-proxy-init -rfc -noregen -voms=fermilab:/fermilab/annie/Role=Analysis -valid 120:00 
    fi
}


source /cvmfs/fermilab.opensciencegrid.org/products/common/etc/setup
export PRODUCTS=${PRODUCTS}:/cvmfs/larsoft.opensciencegrid.org/products/
setup fife_utils

export GROUP="annie"
export GRID_USER=$USER
export EXPERIMENT="annie"
export SAM_EXPERIMENT=$EXPERIMENT
export SAM_STATION=$EXPERIMENT
#export IFDH_BASE_URI="https://samwebgpvm05.fnal.gov:8483/sam/annie/api"
export IFDH_BASE_URI="https://samweb.fnal.gov:8483/sam/annie/api"
export IFDH_FORCE="gsiftp"

fullpath="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
export ANNIEGRIDUTILSDIR="$(dirname "${fullpath}")"
export PATH=$PATH:"${ANNIEGRIDUTILSDIR}"

setup_fnal_security
