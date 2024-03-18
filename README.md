# ANNIEGrid
Tools for the ANNIE experiment to work on the grid

Before using any of the tools you must setup them up on the gpvm:
```
source setup_ANNIEGridUtils.sh
```


You should only ever need to directly use `submit_annie_jobs.py`. 

The basic workflow would look like this:
0. Identify/create a SAM dataset to run over
1. Set up your tool chain configuration
	a. Use `config/Create_run_config.sh <ToolChainName>`, then modify/copy things as necessary
	b. Make note of which configuration file defines the input file to use (ie. my_files.txt for the DataDecoder tool chain or LoadWCSimConfig for the LoadWCSim tool chain)
	c. Make note of the input file variable name (there would be no variable name if you're doing a my_files.txt style input, while for LoadWCSimConfig this would be "InputFile")
2. Tar up your local ToolAnalysis directory
3. Set up a destination directory on PNFS. This directory should be group writable (`chmod g+w <dir>`)
4. Run the `submit_annie_jobs.py` command as specified below. Along with the required arguments, here are some additional ones to consider
   - `--file` instead of writing the full command in the teminal you can write a config file for reusability 
	- `--input_config_var` depends on your tool chain setup as mentioned above
	- `--njobs` technically optional, but should almost always use it
	- `--disk` and `--memory` sets the minium reuired disk space and memory available for worker nodes. Sometimes you need more memory.
	- `--expected_lifetime` how much time should your job be allowed to run (default is 3 hrs)
	- `--exclude_site` some sites don't work
	- `--print_jobsub` shows you the full jobsub command that was generated
	- `--test_submission` will run a small grid job over one input file and only three events 
	
Here is the full usage:
```
usage: submit_annie_jobs.py --jobname JOBNAME --dest DEST --config CONFIG
                            --input_file_config INPUT_FILE_CONFIG --defname
                            DEFNAME --tarball TARBALL
                            [--input_config_var INPUT_CONFIG_VAR]
                            [--no_job_dirs] [--no_rename]
                            [--copy_out_script COPY_OUT_SCRIPT]
                            [--input_file INPUT_FILE] [--export EXPORT]
                            [--earlysource EARLYSOURCE]
                            [--earlyscript EARLYSCRIPT] [--source SOURCE]
                            [--prescript PRESCRIPT] [--postscript POSTSCRIPT]
                            [--njobs NJOBS] [--maxConcurrent MAXCONCURRENT]
                            [--files_per_job FILES_PER_JOB]
                            [--nevents NEVENTS] [--disk DISK]
                            [--memory MEMORY] [--cpu CPU]
                            [--expected_lifetime EXPECTED_LIFETIME]
                            [--grace_memory GRACE_MEMORY]
                            [--grace_lifetime GRACE_LIFETIME]
                            [--continue_project PROJECT_NAME]
                            [--exclude_site SITE] [--onsite_only]
                            [--offsite_only] [--print_jobsub] [--test]
                            [--test_submission] [--kill_after SEC] [-h]
                            [-f FILE]

Submit ANNIE grid job

Required arguments:
  These arguments must be supplied

  --jobname JOBNAME     Job name
  --dest DEST           Destination for outputs
  --config CONFIG, -c CONFIG
                        Tool chain config to use. The config should be a
                        directory in ToolAnalysis/config/
  --input_file_config INPUT_FILE_CONFIG
                        File in the toolchain config directory that defines
                        the input file that is run over. eg. for LoadWCSim
                        this would be "LoadWCSimConfig", while for DataDecoder
                        this would be "my_files.txt"
  --defname DEFNAME     SAM dataset definition to run over
  --tarball TARBALL     If you want to use your local ToolAnalysis then pass
                        in a tarball here.Otherwise the base build from the
                        singularity container will be used.Note: you DO NOT
                        have to move your tarball to your pnfs scratch area.

Other optional arguments:
  These arguments are optional

  --input_config_var INPUT_CONFIG_VAR
                        Variable name in the input_file_config that defines
                        what the input is. eg. for LoadWCSim this would be
                        required and set to "InputFile", while for DataDecoder
                        this argument is not required.
  --no_job_dirs         By default, directories will be created in DEST for
                        each job number in order to prevent overpopulating
                        pnfs directories. Using this flag will turn off that
                        feature.
  --no_rename           By default, output file we be renamed by prepending
                        the input file name for uniqueness. Using this flag
                        will turn off that feature
  --copy_out_script COPY_OUT_SCRIPT
                        Use the supplied COPY_OUT_SCRIPT (located on pnfs).
                        Otherwise all files will be copied as is to the DEST.
  --input_file INPUT_FILE
                        Copy an extra file to the grid node. You can use this
                        multiple times.
  --export EXPORT       Export environment variable to the grid. It must be
                        already set in your current environment. You can pass
                        in multiple variables.
  --earlysource EARLYSOURCE
                        Source this script before doing anything else in the
                        job. You can use this multiple times. Syntax is colon
                        separated arguments (ie. script:arg:arg...)
  --earlyscript EARLYSCRIPT
                        Execute this script before doing anything else in the
                        job (other than any earlysources you've requested. You
                        can use this multiple times. Syntax is colon separated
                        arguments (ie. script:arg:arg...)
  --source SOURCE       Source this script after any specified "early" scripts
                        or sources. You can use this multiple times. Syntax is
                        colon separated arguments (ie. script:arg:arg...)
  --prescript PRESCRIPT
                        Execute this script before running the ToolChain. You
                        can use this multiple times. Syntax is colon separated
                        arguments (ie. script:arg:arg...)
  --postscript POSTSCRIPT
                        Execute this script after running the ToolChain on all
                        files but before copying them out. You can use this
                        multiple times. Syntax is colon separated arguments
                        (ie. script:arg:arg...)

Job control args:
  Optional arguments for additional job control

  --njobs NJOBS         Number of jobs to submit
  --maxConcurrent MAXCONCURRENT
                        Run a maximum of N jobs simultaneously
  --files_per_job FILES_PER_JOB
                        Number of files per job. If zero, calculate from
                        number of jobs
  --nevents NEVENTS     Number of events per file to process
  --disk DISK           Local disk space requirement for worker node in MB.
                        (default 10000MB (10GB))
  --memory MEMORY       Local memory requirement for worker node in MB.
                        (default 1900MB (1.9GB))
  --cpu CPU             Request worker nodes that have at least NUMBER cpus
  --expected_lifetime EXPECTED_LIFETIME
                        Expected job lifetime (default is 10800s=3h). Valid
                        values are an integer number of seconds or one of
                        "short" (6h), "medium" (12h) or "long" (24h, jobsub
                        default)
  --grace_memory GRACE_MEMORY
                        Auto-releese jobs which become held due to
                        insufficient memory and resubmit with this additional
                        memory (in MB)
  --grace_lifetime GRACE_LIFETIME
                        Auto-release jobs which become held due to
                        insufficient lifetime and resubmit with this
                        additional lifetime (in seconds)
  --continue_project PROJECT_NAME
                        Do not start a new samweb project, instead continue
                        the specified one.
  --exclude_site SITE   Specify an offsite location to exclude.
  --onsite_only         Allow to run solely on onsite resources.
  --offsite_only        Allow to run solely on offsite resources.

Debugging options:
  These are optional arguments that are useful for debugging or testing

  --print_jobsub        Print jobsub command
  --test                Do not actually do anything, just run tests and print
                        jobsub cmd
  --test_submission     Override other arguments given to submit a test to the
                        grid.It will run 1 job with 3 events and write the
                        output to /pnfs/nova/scratch/users/<user>/test_jobs/<d
                        ate>_<time>
  --kill_after SEC      If job is still running after this many seconds, kill
                        in such a way that a log will be returned

HELP!:

  -h, --help            Show this help message and exit
  -f FILE, --file FILE  Text file containing any arguments to this utility.
                        Multiple allowed. Arguments should look just like they
                        would on the command line, but the parsing of this
                        file is whitespace insenstive. Comments will be
                        identified with the # character and removed.
```
