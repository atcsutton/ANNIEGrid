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
dest=""
limit=""
nevts=""
ifconf=""
ivar=""
cpsc=""
topDir=""
rename_outputs=false

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

    # -t|--tarball
    # 	tarball containing the user's ToolAnalysis
    # 	if this is not present, the standard ToolAnalysis container is used

    -c|--config directory
        toolchain config to use
	must be a directory in ToolAnalysis/config and have a symlink in the top directory

    -D|--dest path
        specify destination path or url for copying back output
        default is to not copy back files

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
        rename the output files by prepending the input file name
	


EOF
}

while [ $# -gt 0 ]
do
    echo "debug: \$1 '$1' \$2 '$2'"
    case "x$1" in
	x-h|x--help)    usage; exit 0;;
	# x-t|x--tarball)           tarball="$2"; shift;  shift; continue;;
	x-c|x--config)            conf="$2";    shift;  shift; continue;;
	x-D|x--dest)              dest="$2";    shift;  shift; continue;;
	x-L|x--limit)             limit="$2";   shift;  shift; continue;;
	x-n|x--nevents)           nevts="$2";   shift;  shift; continue;;
	x-i|x--input_file_config) ifconf="$2";  shift;  shift; continue;;
	x-v|x--input_config_var)  ivar="$2";    shift;  shift; continue;;
	x-o|x--copy_out_script)   cpsc="$2";    shift;  shift; continue;;
	x-r|x--rename_outputs)    rename_outputs=true;  shift; continue;;
	*)                        args="$args \"$1\"";  shift; continue;;
    esac
    break
done

################################################################################
# Function to get the next file in the SAM project
################################################################################
get_next_file() {
    fname=""
    uri=`IFDH_DEBUG= ifdh getNextFile $projurl $consumer_id | tail -1`
    [ -z "${uri}" ] && return 0

    IFDH_DEBUG= ifdh fetchInput "$uri" > fetch.log 2>&1
    res=$?
    if [ $res -ne 0 ]; then
        echo "Failed to fetch from $uri"
        cat fetch.log
        rm fetch.log
    else
        fname=`tail -1 fetch.log`
    fi

    return $res
}

################################################################################
# Check space on this grid node
################################################################################
check_space() {
    set : `df -P . | tail -1`
    avail_blocks=$5
    if [ $avail_blocks -lt 1024 ]
    then
	echo "Not enough space (only ${avail_blocks}k) on this node in `pwd`."
	df -H .
	return 1
    fi
    return 0
}

################################################################################
# The typical ANNIE configfile setup forces us to run from the top directory
# this means output files will be placed in that directory, 
################################################################################
modify_config_fullpath() {
    oldwd=`pwd`
    
    # copy the toolchain configs to a new directory so we can edit them
    cd $topDir
    tempConfigDir="${topDir}/tempConfig"
    mkdir $tempConfigDir
    cd $tempConfigDir

    
    cp -r ${topDir}/configfiles/${config}/* .
    
    # remove any cases of ./ before the configfiles directory
    sed -i 's/\.\/configfiles/configfiles/g' *

    # insert the full path for this config. Have to use an alternate sed delimiter (~)
    sed -i 's~configfiles/'${config}/'~'`pwd`'/~g' *

    # insert the full path for other configs that are referenced
    sed -i 's~configfiles~'${topDir}'/configfiles~g' *

    cd $oldwd
}

################################################################################
# Update the input file for the toolchain
################################################################################
update_input_file() {
    oldwd=`pwd`
    
    # make sure we're in the tool analysis directory
    cd $tempConfigDir

    if [ -z "$ivar" ]; then
	# no input variable defined, assume that we can just overwrite the ifconf
	echo "$fname" > ${ifconf}
    else
	# we'll have to overwrite the specific line
	sed -i 's/^'${ivar}' .*$/'${ivar}' '${fname}'/g' ${ifconf}
    fi

    cd $oldwd
}

################################################################################
# Rename outputs by prepending the input file name
################################################################################
rename_output_files() {
    oldwd=`pwd`
    
    if [ ! -f "renamed_files.txt" ]; then
	touch renamed_files.txt
	echo "renamed_files.txt" >> renamed_files.txt
    fi
    
    # just looking for files that have not been renamed
    for file in `ls | grep -vFf renamed_files.txt`; do
	mv ${file} ${fname}.${file}
	echo ${fname}.${file} >> renamed_files.txt
    done

    cd $oldpwd
}

################################################################################
# The meat and potatoes
################################################################################

# Setup ups and ifdhc
source /cvmfs/fermilab.opensciencegrid.org/products/common/etc/setup
setup ifdhc

# print some things for debugging
echo ""
ups active
printenv
pwd
ls
echo ""

topDir=$_CONDOR_JOB_IWD

#-------------------------------------------------------------------------------
# if we don't have ups or enough space, try again for a bit
# before giving up
#-------------------------------------------------------------------------------
hostname
count=0
until check_space
do
    count=$((count + 1))
    if [ $count -gt 6 ]
    then
	echo "Timed out waiting for space and"
	exit 1
    fi
    sleep 600
done

#-------------------------------------------------------------------------------
# grab project information
#-------------------------------------------------------------------------------
projurl=`ifdh findProject $SAM_PROJECT_NAME ${SAM_STATION:-$EXPERIMENT}`
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
    if [ $count -gt 10 ]
    then
        echo "Unable to establish consumer id!"
        echo "Unable to establish consumer id!" >&2
        exit 1
    fi
done

echo project url: $projurl
echo consumer id: $consumer_id

#-------------------------------------------------------------------------------
# untar if necessary then make an output dir and modify the configs for full paths
# untar happens automatically with the use of --tar_file_name for jobsub
#-------------------------------------------------------------------------------
cd $topDir
# if [ -n "$tarball" ]; then
#     toolanaDir="${topDir}/ToolAnalysis"
#     mkdir "${toolanaDir}"
#     tar -xzf "${CONDOR_DIR_INPUT}/${tarball}" -C "${toolanaDir}"
# fi
outputDir="${topDir}/Outputs"
mkdir Outputs

# Adjust the config path to allow running from anywhere
# then set the number of events to run over according to the input options
modify_config_fullpath || break
sed -i 's/^Inline .*$/Inline '${nevts}'/g' ${tempConfigDir}/ToolChainConfig

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
    echo "got file: $fname"
    ifdh updateFileStatus $projurl  $consumer_id $fname transferred

    update_input_file || break

    containercmd="\"cd ${topDir} && ls && source Setup.sh && cd ${outputDir} && ${topDir}/Analyse ${tempConfigDir}/ToolChainConfig\""
    command="singularity exec -B/srv:/srv,${topDir}:${topDir} /cvmfs/singularity.opensciencegrid.org/anniesoft/toolanalysis\:latest/ bash -c ${containercmd}"

    echo "Running: $command"
    if eval "$command"; then
        ifdh updateFileStatus $projurl  $consumer_id $fname consumed
        n_skipped_in_a_row=0
    else
        command_exit_code=$?
	((n_skipped_in_a_row++))
        if [[ $n_skipped_in_a_row -ge $n_max_files_skipped ]]; then
	    echo "Reached limit of $n_skipped_in_a_row failed jobs in a row. Returning error code of $command_exit_code and ending the multi-file loop."
            res=$command_exit_code
	else
	    echo "Command returned an error code of $command_exit_code, marking file as skipped. This is skip $n_skipped_in_a_row out of a maximum of $n_max_files_skipped."
	fi
        ifdh updateFileStatus $projurl  $consumer_id $fname skipped
    fi

    if [ $rename_outputs ]; then
	rename_output_files
    fi
	
done

#-------------------------------------------------------------------------------
# Now to copy things out to dest
#-------------------------------------------------------------------------------
if [ -n "${cpsc}" ]; then
    # use custom script
    eval $CONDOR_DIR_INPUT/${cpsc}
else
    # otherwise just copy everything
    cd $outputDir
    for file in `ls`; do
	ifdh addOutputFile $file
    done

    ifdh copyBackOutput "${dest}"
fi

#-------------------------------------------------------------------------------
# And clean it all up
#-------------------------------------------------------------------------------
ifdh endProcess "$projurl" "$consumer_id"
cd ${topDir}
rm -rf *
rm ${CONDOR_DIR_INPUT}/*
ifdh cleanup -x
