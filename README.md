# ANNIEGrid
Tools for the ANNIE experiment to work on the grid

Before using any of the tools you must setup SAM on the gpvm:
```source setup_sam.sh```

You should only ever need to directly use `submit_annie_jobs.py`. Here are all the inputs:
```usage: submit_annie_jobs.py --jobname JOBNAME --dest DEST --config CONFIG --input_file_config INPUT_FILE_CONFIG --defname DEFNAME [--input_file INPUT_FILE]
                            [--tarball TARBALL] [--input_config_var INPUT_CONFIG_VAR] [--copy_out_script COPY_OUT_SCRIPT] [--no_rename] [--njobs NJOBS]
                            [--files_per_job FILES_PER_JOB] [--nevents NEVENTS] [--disk DISK] [--memory MEMORY] [--cpu CPU] [--expected_lifetime EXPECTED_LIFETIME]
                            [--grace_memory GRACE_MEMORY] [--grace_lifetime GRACE_LIFETIME] [--continue_project PROJECT_NAME] [--exclude_site SITE] [--print_jobsub]
                            [--test_submission] [-h]

Submit ANNIE jobs

Required arguments:
  These arguments must be supplied

  --jobname JOBNAME     Job name
  --dest DEST           Destination for outputs
  --config CONFIG, -c CONFIG
                        Tool chain config to use. The config should be a directory in ToolAnalysis/config/
  --input_file_config INPUT_FILE_CONFIG
                        File in the toolchain config directory that defines the input file that is run over. eg. for LoadWCSim this would be "LoadWCSimConfig", while
                        for DataDecoder this would be "my_files.txt"
  --defname DEFNAME     SAM dataset definition to run over

Other optional arguments:
  These arguments are optional

  --input_file INPUT_FILE
                        Copy an extra file to the grid node. You can use this multiple times.
  --tarball TARBALL     If you want to use your local ToolAnalysis then pass in a tarball here.Otherwise the base build from the singularity container will be
                        used.Note: you DO NOT have to move your tarball to your pnfs scratch area.
  --input_config_var INPUT_CONFIG_VAR
                        Variable name in the input_file_config that defines what the input is. eg. for LoadWCSim this would be required and set to "InputFile", while
                        for DataDecoder this argument is not required.
  --copy_out_script COPY_OUT_SCRIPT
                        Use the supplied COPY_OUT_SCRIPT (located on pnfs). Otherwise all files will be copied as is to the DEST.
  --no_rename           By default, output file we be renamed by prepending the input file name for uniqueness. Using this flag will turn off that "feature"

Job control args:
  Optional arguments for additional job control

  --njobs NJOBS         Number of jobs to submit
  --files_per_job FILES_PER_JOB
                        Number of files per job. If zero, calculate from number of jobs
  --nevents NEVENTS     Number of events per file to process
  --disk DISK           Local disk space requirement for worker node in MB.
  --memory MEMORY       Local memory requirement for worker node in MB.
  --cpu CPU             Request worker nodes that have at least NUMBER cpus
  --expected_lifetime EXPECTED_LIFETIME
                        Expected job lifetime (default is 10800s=3h). Valid values are an integer number of seconds or one of "short" (6h), "medium" (12h) or "long"
                        (24h, jobsub default)
  --grace_memory GRACE_MEMORY
                        Auto-release jobs which become held due to insufficient memory and resubmit with this additional memory (in MB)
  --grace_lifetime GRACE_LIFETIME
                        Auto-release jobs which become held due to insufficient lifetime and resubmit with this additional lifetime (in seconds)
  --continue_project PROJECT_NAME
                        Do not start a new samweb project, instead continue the specified one.
  --exclude_site SITE   Specify an offsite location to exclude.

Debugging options:
  These are optional arguments that are useful for debugging or testing

  --print_jobsub        Print jobsub command
  --test_submission     Override other arguments given to submit a test to the grid.It will run 1 job with 3 events and write the output to
                        /pnfs/nova/scratch/users/<user>/test_jobs/<date>_<time>

HELP!:

  -h, --help            Show this help message and exit
  ```
