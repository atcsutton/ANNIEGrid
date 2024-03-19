#!/bin/sh

#
# This job assumes it's being passed:
#  $SAM_PROJECT_NAME
#  $EXPERIMENT
#  $GRID_USER

if [ "x$ART_SAM_DEBUG" = "xtrue" ]
then
    set -x
fi

hostname
uname -a
ls /lib*/libc-*.so

# initialize options with blank
# tarball=""
conf=""
limit=""
nevts=""
ifconf=""
ivar=""
cpsc=""
topDir=""
rename_outputs=false
job_dirs=false
self_destruct_timeout=""
earlysources=""
earlyscripts=""
sources=""
prescripts=""
postscripts=""

n_max_files_skipped=1

#
# parse options we know, collect rest in $args
#
usage() {
    cat <<EOF
Usage:
    $0 [Options] [cmd_options]

       find ifdh_art and dependencies in CVMFS or in /nusoft/app/externals,
       register a consumer process, and run an ART executable,
       fetching input from a SAM Project specified by $SAM_PROJECT
       in the environment.

    Options are:

    -h|--help
        Print this message and exit

    -c|--config directory
        toolchain config to use
	must be a directory in ToolAnalysis/config and have a symlink in the top directory

    -L|--limit
        Pass a number of files limit to establishProcess.

    -n|--nevents N
        how many events to run over per file

    -i|--input_file_config configfile
        name of the configuration file that defines what input files to run over
	eg. for LoadWCSim this would be "LoadWCSimConfig" while for DataDecoder this would be "my_files.txt" 

    -v|--input_config_var varname
        variable name in the input_file_config that defines what the input file is
	'eg. for LoadWCSim this would be required and set to "InputFile" while for DataDecoder this argument is not required.

    -o|--copy_out_script script
        optional script to copy outputs to the final destination. 

    -r|--rename_outputs
        rename the output files by prepending the input file name or job and file number

    -j|--job_dirs
        make destination directories based on the job numbers.

    --self_destruct_timer seconds
        suicide if the executable runs more than seconds seconds;
        usually only use this if you have jobs that hang and you
        get no output back
	
    --earlysource file:arg:arg:...
    --earlyscript file:arg:arg:...
    --source file:arg:arg:...
    --prescript file:arg:arg:...
    --postscript file:arg:arg:...
        source/execute the file. These are performed in the order above. 
	prescript and earlier are before ups. postscript is after processing all files, but before copying out.



EOF
}

VALID_ARGS=$(getopt -o hrjc:L:n:i:v:o: --long help,rename_outputs,job_dirs,config:,limit:,nevents:,input_file_config:,input_config_var:,copy_out_script:,self_destruct_timer:,earlysource:,earlyscript:,source:,prescript: -- "$@")
if [[ $? -ne 0 ]]; then
    exit 1;
fi

eval set -- "$VALID_ARGS"
while [ : ]; do
    echo "debug: \$1 '$1' \$2 '$2'"
    case "$1" in
	-h|--help)              usage; exit 0;;
	-r|--rename_outputs)    rename_outputs=true;  shift; continue;;
	-j|--job_dirs)          job_dirs=true;        shift; continue;;
	-c|--config)            conf="$2";    shift;  shift; continue;;
	-L|--limit)             limit="$2";   shift;  shift; continue;;
	-n|--nevents)           nevts="$2";   shift;  shift; continue;;
	-i|--input_file_config) ifconf="$2";  shift;  shift; continue;;
	-v|--input_config_var)  ivar="$2";    shift;  shift; continue;;
	-o|--copy_out_script)   cpsc="$2";    shift;  shift; continue;;
	--self_destruct_timer) self_destruct_timeout=$2; shift; shift; continue;;
	--earlysource)   earlysources="$earlysources \"$2\"":; shift; shift; continue;;
	--earlyscript)   earlyscripts="$earlyscripts \"$2\"":; shift; shift; continue;;
	--source)        sources="$sources \"$2\"":;           shift; shift; continue;;
	--prescript)     prescripts="$prescripts \"$2\"":;     shift; shift; continue;;
	--postscript)    postscripts="$postscripts \"$2\"":;   shift; shift; continue;;
	--)                                           shift; break;;
    esac
    break
done

################################################################################
# HOUSEKEEPING
################################################################################
clean_dir() {
    oldwd=`pwd`

    if [ -n "${1}" ]; then
	echo ""
	echo "${1} before cleaning:"
	ls ${1}
	rm -rf ${1}/*
	echo ""
	echo "${1} after cleaning:"
	ls ${1}
    fi
    
    cd ${oldwd}
}

clean_it_up() {
    echo ""
    echo "Cleaning up"

    clean_dir "${outputDir}"
    clean_dir "${tempConfigDir}"
    clean_dir "${CONDOR_DIR_INPUT}"

    if [ -n "${consumer_id}" ]; then
	echo ""
	echo "Ending this process"
	echo "ifdh endProcess ${projurl} ${consumer_id}"
	ifdh endProcess "${projurl}" "${consumer_id}"
    fi

    ifdh cleanup -x

    #dump=`ifdh dumpProject ${projurl}`
    dump=`curl ${IFDH_BASE_URI}/projects/name/${SAM_PROJECT_NAME}/summary?format=json`
    num_active=`echo ${dump} | jq -r '.process_counts.active'`
    status=`echo ${dump} | jq -r '.project_status'`

    if [[ ${status} == *"running"* ]]; then
	echo "The project is running and there are are ${num_active} active processes remaining"
	if [ "${num_active}" = "null" ]; then
	    echo "Ending the whole project"
	    ifdh endProject "${projurl}"
	fi
    fi

    echo ""
    echo "Done Cleaning"
}

################################################################################
# Function to get the next file in the SAM project
################################################################################
get_next_file() {
    fname=""
    uri=`IFDH_DEBUG= ifdh getNextFile $projurl $consumer_id | tail -1`
    [ -z "${uri}" ] && return 0

    IFDH_DEBUG= ifdh fetchInput "${uri}" > fetch.log 2>&1
    res=$?
    if [ ${res} -ne 0 ]; then
        echo "Failed to fetch from ${uri}"
        cat fetch.log
        rm fetch.log
    else
        fname=`tail -1 fetch.log`
	echo "Got file: ${fname}"
    fi

    return ${res}
}

################################################################################
# Check space on this grid node
################################################################################
check_space() {
    set : `df -P . | tail -1`
    avail_blocks=${5}
    if [ ${avail_blocks} -lt 1024 ]
    then
	echo "Not enough space (only ${avail_blocks}k) on this node in `pwd`."
	df -H .
	return 1
    fi
    return 0
}

################################################################################
# Check job lifetime
################################################################################
check_lifetime() {
    echo "-------------------------------"
    echo "| Checking dynamic lifetime"
    # Is dynamic lifetime in use
    if [ X${dynamic_lifetime} == X ]; then
        echo "|  dynamic_lifetime = ${dynamic_lifetime}"
        echo "-------------------------------"
        # No? Then don't bother with this test
        return 0
    fi

    tnow=`date +%s` # time since Epoch
    tleft=$(( ${FIFE_GLIDEIN_ToDie} - ${tnow} ))
    echo "|   tnow = $tnow "
    echo "|   FIFE_GLIDEIN_ToDie = ${FIFE_GLIDEIN_ToDie}"
    echo "|   tleft = ${tleft}"
    echo "|   dynamic_lifetime = ${dynamic_lifetime}"
    if [ ${tleft} -le ${dynamic_lifetime} ]; then
        # There is not enough time left to process the job,
        # send back the kill command
        echo "|  Killing"
        echo "-------------------------------"
        return 1
    fi
    echo "|  Continuing"
    echo "-------------------------------"
    return 0
}

################################################################################
# The typical ANNIE configfile setup forces us to run from the top directory
# this means output files will be placed in that directory, 
################################################################################
modify_config_fullpath() {
    oldwd=`pwd`
    
    # copy the toolchain configs to a new directory so we can edit them
    cd ${topDir}
    tempConfigDir="${topDir}/TempConfig"
    mkdir ${tempConfigDir}
    cd ${tempConfigDir}

    cp -r ${toolAnaDir}/configfiles/${conf}/* .
    
    # remove any cases of ./ before the configfiles directory
    sed -i 's/\.\/configfiles/configfiles/g' *

    # insert the full path for this config 
    # Have to use an alternate sed delimiter (~)
    sed -i 's~configfiles/'${conf}/'~'${tempConfigDir}'/~g' *

    # insert the full path for other configs that are referenced
    sed -i 's~configfiles~/MyToolAnalysis/configfiles~g' *

    cd ${oldwd}
}

################################################################################
# Update the input file for the toolchain
################################################################################
update_input_file() {
    oldwd=`pwd`

    echo "Updating input file"
    # make sure we're in the tool analysis directory
    cd ${tempConfigDir}

    if [ -z "${ivar}" ]; then
	# no input variable defined, assume that we can just overwrite the ifconf
	echo "${fname}" > ${ifconf}
    else
	# we'll have to overwrite the specific line
	sed -i 's/^'${ivar}' .*$/'${ivar}' '${fname}'/g' ${ifconf}
    fi

    
    ls ${tempConfigDir}/${ifconf}
    cat ${tempConfigDir}/${ifconf}
    echo "Done updating input file"

    cd ${oldwd}
}

################################################################################
# Rename outputs by prepending the input file name or job and file number
################################################################################
rename_output_files() {
    oldwd=`pwd`

    echo ""
    echo "Renaming output files"
    cd ${outputDir}

    if [ ! -f "renamed_files.txt" ]; then
	touch renamed_files.txt
	echo "renamed_files.txt" >> renamed_files.txt
    fi

    # If using job dirs then prepend job and file number
    # otherwise prepend the input file name
    if ${job_dirs}; then
	if [ -z "${filenum}" ]; then
	    filenum=0
	else
	    filenum=$((filenum + 1))
	fi
	
	if [ -n "${JOBSUBJOBSECTION}" ]; then
	    jobnum=${JOBSUBJOBSECTION}
	else
	    jobnum=${PROCESS}
	fi

	prefix="job${jobnum}_file${filenum}"
	# just looking for files that have not been renamed
	for outfile in `ls | grep -vFf renamed_files.txt`; do
	    mv ${outfile} ${prefix}.${outfile}
	    echo ${prefix}.${outfile} >> renamed_files.txt
	done
	
    else
	prefix=`basename ${fname}`	
	# just looking for files that have not been renamed
	for outfile in `ls | grep -vFf renamed_files.txt`; do
	    mv ${outfile} ${prefix}.${outfile}
	    echo ${prefix}.${outfile} >> renamed_files.txt
	done
    fi

    cd ${oldpwd}
}

################################################################################
# Self destruct after n seconds
################################################################################
kill_proc_kids_after_n() {
    watchpid=$1
    after_secs=$2
    rate=10
    sofar=0

    start=`date +%s`
    echo "Starting self-destruct timer of $after_secs at $start"

    while kill -0 $watchpid 2> /dev/null && [ $sofar -lt $after_secs ]
    do
        sleep $rate
        now=`date +%s`
        sofar=$((now - start))
        printf "."
    done
    printf "\n"

    if kill -0 $watchpid
    then
       pslist=`ps -ef | grep " $watchpid " | grep -v grep`
       printf "Timed out after $sofar seconds...\n"
       for signal in 15 9
       do
           echo "$pslist" |
              while read uid pid ppid rest
              do
                 if [ $ppid = $watchpid ]
                 then
                     echo "killing -$signal $uid $pid $ppid $rest"
                     kill -$signal $pid
                 fi
              done
           echo "killing -$signal $watchpid"
           kill -$signal $watchpid
       done
    fi
}


################################################################################
# The meat and potatoes
################################################################################
#-------------------------------------------------------------------------------
# check that necessary inputs were set
#-------------------------------------------------------------------------------
if [ -n "${conf}" ]; then
    echo "Configuration is ${conf}"
else
    echo "ToolChain was not specified"
    clean_it_up
    exit 1
fi

if [ -n "${DEST}" ]; then
    echo "Destination is ${DEST}"
else
    echo "Destination was not specified"
    clean_it_up
    exit 1
fi

if [ -n "${nevts}" ]; then
    echo "Number of events per file is ${nevts}"
else
    echo "Number of events per file was not specified"
    clean_it_up
    exit 1
fi

if [ -n "${ifconf}" ]; then
    echo "Input file config is ${ifconf}"
else
    echo "Input file config not specified"
    clean_it_up
    exit 1
fi

if [ -n "${ivar}" ]; then
    echo "Input file config variable is ${ivar}"
fi


# Set the self destruct
if [ x"$self_destruct_timeout" != x ]
then
   kill_proc_kids_after_n $$ $self_destruct_timeout &
fi


#-------------------------------------------------------------------------------
# if we don't enough space, try again for a bit before giving up
#-------------------------------------------------------------------------------
hostname
echo ""

count=0
until check_space
do
    count=$((count + 1))
    if [ $count -gt 6 ]
    then
	echo "Timed out waiting for space"
	clean_it_up
	exit 1
    fi
    sleep 600
done

#-------------------------------------------------------------------------------
# Run the early stuff
#-------------------------------------------------------------------------------
for blat in $earlysources; do
    blat=${CONDOR_DIR_INPUT}/${blat}
    blat=`echo ${blat} | sed -e 's/:/ /g'`
    eval blat=${blat}
    echo "doing source: ${blat}"
    eval "source ${blat}"
done

for blat in $earlyscripts; do
    blat=${CONDOR_DIR_INPUT}/${blat}
    blat=`echo $blat | sed -e 's/:/ /g'`
    eval blat=$blat
    echo "doing: $blat"
    eval "$blat"
done

for blat in $sources; do
    blat=${CONDOR_DIR_INPUT}/${blat}
    blat=`echo $blat | sed -e 's/:/ /g'`
    eval blat=$blat
    echo "doing: source $blat"
    eval "source $blat"
done

for blat in $prescripts; do
    blat=${CONDOR_DIR_INPUT}/${blat}
    blat=`echo $blat | sed -e 's/:/ /g'`
    eval blat=$blat
    echo "doing: $blat"
    eval "$blat"
done

#-------------------------------------------------------------------------------
# Setup ups and ifdhc
#-------------------------------------------------------------------------------
source /cvmfs/fermilab.opensciencegrid.org/products/common/etc/setup
setup ifdhc


#-------------------------------------------------------------------------------
# print some things for debugging
#-------------------------------------------------------------------------------
echo ""
ups active
echo ""
printenv
echo""
pwd
echo ""
ls
echo ""
echo "$INPUT_TAR_DIR_LOCAL"
ls $INPUT_TAR_DIR_LOCAL
echo ""

topDir=$_CONDOR_JOB_IWD
toolAnaDir=$INPUT_TAR_DIR_LOCAL

#-------------------------------------------------------------------------------
# grab project information
#-------------------------------------------------------------------------------
echo "ifdh findProject $SAM_PROJECT_NAME ${SAM_STATION:-$EXPERIMENT}"
projurl=`curl ${IFDH_BASE_URI}/findProject?name=${SAM_PROJECT_NAME}&station=${SAM_STATION}`
#projurl=`ifdh findProject ${SAM_PROJECT_NAME} ${SAM_STATION:-$EXPERIMENT}`
if [ -n "${projurl}" ]; then
   echo "Project URL: ${projurl}"
else
    echo "Project URL not found. Aborting!"
    clean_it_up
    exit 1
fi

appname="Analyse"
appversion="latest"
hostname=`hostname --fqdn`
appfamily="ToolAnalysis"

if [ -n "${JOBSUBJOBID}" ]
then
   description="${JOBSUBJOBID}"
elif [ -n "${CLUSTER}"]
then
   description="${CLUSTER}.${PROCESS}"
else
   description=""
fi

consumer_id=''
count=0
while [ "$consumer_id" = "" ]
do
    echo " Attempt $((count+1))"
    sleep 5
    consumer_id=`IFDH_DEBUG= ifdh establishProcess "$projurl" "$appname" "$appversion" "$hostname" "$GRID_USER" "$appfamily" "$description" "$limit"`
    count=$((count + 1))
    if [ $count -gt 10 ]; then
        echo "Unable to establish consumer id!"
        echo "Unable to establish consumer id!" >&2
	clean_it_up
        exit 1	
    fi
done

echo "Consumer id: ${consumer_id}"

#-------------------------------------------------------------------------------
# Setup the output directory, adjust config path, and set nevents
#-------------------------------------------------------------------------------
cd $topDir
outputDir="${topDir}/Outputs"
mkdir Outputs
modify_config_fullpath || break
sed -i 's/^Inline .*$/Inline '${nevts}'/g' ${tempConfigDir}/ToolChainConfig

#-------------------------------------------------------------------------------
# This is what we'll run inside of the container
# Since we're calling Analyse from a different directory we also need to update the ROOT_INCLUDE_PATH
# the first ROOT_INCLUDE_PATH addition doesn't need the colon, but the second one does...
#-------------------------------------------------------------------------------
containercmd="\"cd /MyToolAnalysis && "
containercmd+="source Setup.sh && "
containercmd+="export ROOT_INCLUDE_PATH=\"'"'${ROOT_INCLUDE_PATH}'"'\"/MyToolAnalysis/DataModel && "
containercmd+="export ROOT_INCLUDE_PATH=\"'"'${ROOT_INCLUDE_PATH}'"'\":/MyToolAnalysis/ToolDAQ/boost_1_66_0/boost/serialization/ && "
containercmd+="export ROOT_INCLUDE_PATH=\"'"'${ROOT_INCLUDE_PATH}'"'\":/MyToolAnalysis/ToolDAQ/boost_1_66_0/install/include/ && "
containercmd+="cd ${outputDir} && "
containercmd+="echo \"'"'${ROOT_INCLUDE_PATH}'"'\" && "
containercmd+="/MyToolAnalysis/Analyse ${tempConfigDir}/ToolChainConfig\""

command="singularity exec -B${topDir}:${topDir},${toolAnaDir}:/MyToolAnalysis /cvmfs/singularity.opensciencegrid.org/anniesoft/toolanalysis\:latest/ bash -c ${containercmd}"

#-------------------------------------------------------------------------------
# the loop to grab files from SAM and run over them
#-------------------------------------------------------------------------------
res=0
n_skipped_in_a_row=0
while [ "$res" = 0 ]; do
    check_lifetime || break
    get_next_file || break
    if [ -z "${fname}" ]; then
        echo "No files returned by SAM project.  Most likely all files in the project have already been seen."
        break
    fi
    ifdh updateFileStatus ${projurl}  ${consumer_id} ${fname} transferred

    update_input_file || break

    echo ""
    echo "Running: ${command}"    
    if eval "${command}"; then
        ifdh updateFileStatus ${projurl}  ${consumer_id} ${fname} consumed
        n_skipped_in_a_row=0
    else
        command_exit_code=$?
	((n_skipped_in_a_row++))
        if [[ ${n_skipped_in_a_row} -ge ${n_max_files_skipped} ]]; then
	    echo "Reached limit of ${n_skipped_in_a_row} failed jobs in a row. Returning error code of ${command_exit_code} and ending the multi-file loop."
            res=${command_exit_code}
	else
	    echo "Command returned an error code of ${command_exit_code}, marking file as skipped. This is skip ${n_skipped_in_a_row out} of a maximum of ${n_max_files_skipped}."
	fi
        ifdh updateFileStatus ${projurl}  ${consumer_id} ${fname} skipped
    fi

    if ${rename_outputs}; then
	rename_output_files
    fi 
done

#-------------------------------------------------------------------------------
# the post execution scripts
#-------------------------------------------------------------------------------
for blat in $postscripts; do
    blat=${CONDOR_DIR_INPUT}/${blat}
    blat=`echo $blat | sed -e 's/:/ /g'`
    eval blat=$blat
    echo "doing: $blat"
    eval "$blat"
    postres=$?
    if [ "$res" = "0" -a "$postres" != "0" ]; then
	res=$postres
    fi
done


#-------------------------------------------------------------------------------
# Now to copy things out to $DEST
#-------------------------------------------------------------------------------
if ${job_dirs}; then
    if [ -n "${JOBSUBJOBSECTION}" ]; then
	newdest=${DEST}/job_${JOBSUBJOBSECTION}
    else
	newdest=${DEST}/job_${PROCESS}
    fi
    echo "new destination will be ${newdest}"
    ifdh mkdir_p ${newdest}
    DEST=${newdest}
fi

if [ -n "${cpsc}" ]; then
    # use custom script
    echo ""
    echo "Using custom copyout script"
    eval ${CONDOR_DIR_INPUT}/${cpsc}
else
    # otherwise just copy everything
    echo ""
    echo "Copying back everything from ${outputDir} to ${DEST}"
        
    cd ${outputDir}
    ls
    for file in `ls`; do
	if [ "${file}" = "renamed_files.txt" ]; then
	    continue
	fi
	
	ifdh addOutputFile ${file}
    done

    ifdh copyBackOutput "${DEST}"
fi

#-------------------------------------------------------------------------------
# And clean it all up
#-------------------------------------------------------------------------------
clean_it_up
