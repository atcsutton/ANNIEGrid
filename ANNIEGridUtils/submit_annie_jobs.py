#!/usr/local/bin/python3
#/bin/env python3

from __future__ import print_function
from __future__ import division
from future import standard_library
standard_library.install_aliases()
from builtins import str
from past.utils import old_div
import os, sys, stat, pwd, re
import argparse
import datetime
import samweb_client
import string
import tokenize
import io
import subprocess

jobsub_opts = []

annie_sam_wrap_cmd = 'annie_sam_wrap.sh'
annie_sam_wrap_opts = []

export_to_annie_sam_wrap = ['GRID_USER', 'EXPERIMENT', 'SAM_EXPERIMENT', 'SAM_STATION', 'SAM_PROJECT_NAME', 'IFDH_BASE_URI', 'IFDH_FORCE']

timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
user=os.getenv("USER")


def build_jobsub_cmd(jobsub_opts):
    jobsub_cmd = 'jobsub_submit'
    jobsub_cmd += ' ' + ' '.join([arg for arg in jobsub_opts])

    # Add exported environment variables
    for export in export_to_art_sam_wrap:
        jobsub_opts += ['    -e ' + export]
        
    # Add tarball and SAM wrapper script with its options
    if args.tarball:
        jobsub_opts += ['--tar_file_name dropbox://' + args.user_tarball]

    jobsub_opts += ['file://' + annie_sam_wrap_cmd]
    jobsub_opts += [' ' + ' '.join(annie_sam_wrap_opts)]

    jobsub_cmd += ' ' + ' '.join([opt for opt in jobsub_opts])

    return jobsub_cmd

    
#######################################################################################
if __name__=='__main__':

    while "-f" in sys.argv or "--file" in sys.argv:
        ### Allow args to be passed in as a plain text file.
        ### We make a preliminary parser get these arguments out for two reasons:
        ###    1)  Maintain standard -h, --help functionality
        ###    2)  Avoid necessity required arguments in initial parsing,
        ###        allow them to be missing, but find them in the file.
        preliminary_parser = argparse.ArgumentParser(prog=prog, description='Submit nova art job')

        preliminary_parser.add_argument('-f', '--file',
                        help='''Text file containing any arguments to this utility.  Multiple allowed.
                        Arguments should look just like they would on the command line,
                        but the parsing of this file is whitespace insenstive.
                        Commented lines will be identified with the # character and removed. ''', type=str, action='append')
        pre_args, unknown = preliminary_parser.parse_known_args()

        # Remove pre_args from sys.argv so they are not processed again
        sys.argv = [x for x in sys.argv if x not in [ "-f", "--file"]]

        if pre_args.file:
            for filepath in pre_args.file:
                index = sys.argv.index(filepath)
                sys.argv.remove(filepath)
                if os.path.isfile(filepath):
                    fullpath = filepath
                else:
                    fullpath = find_file(["$NOVAGRIDUTILS_DIR/configs/"],filepath)
                text = open(fullpath, 'r').read()
                text = remove_comments(text) # Strip out commented lines
                newargs = []
                for line in text.splitlines():
                    # Insert arguments into list in order
                    # where the -f appeared
                    newargs += line.split()
                sys.argv[index:index] = newargs

    
    parser = argparse.ArgumentParser(description="Submit ANNIE jobs", add_help=False)

    required_args = parser.add_argument_group('Required arguments', 'These arguments must be supplied')
    required_args.add_argument('--jobname',           required=True, help='Job name');
    required_args.add_argument('--dest',              required=True, help='Destination for outputs')
    required_args.add_argument('--config', '-c',      required=True, help='Tool chain config to use. The config should be a directory in ToolAnalysis/config/')
    required_args.add_argument('--input_file_config', required=True, help='File in the toolchain config directory that defines the input file that is run over. '\
                                                                          'eg. for LoadWCSim this would be \"LoadWCSimConfig\", '\
                                                                          'while for DataDecoder this would be \"my_files.txt\"')
    required_args.add_argument('--defname',           required=True, help='SAM dataset definition to run over')

    
    optional_args = parser.add_argument_group('Other optional arguments', 'These arguments are optional')
    optional_args.add_argument('--input_file', action='append',    help='Copy an extra file to the grid node. You can use this multiple times.')
    optional_args.add_argument('--tarball',                        help='If you want to use your local ToolAnalysis then pass in a tarball here.'\
                                                                        'Otherwise the base build from the singularity container will be used.'\
                                                                        'Note: you DO NOT have to move your tarball to your pnfs scratch area.')
    optional_args.add_argument('--input_config_var',               help='Variable name in the input_file_config that defines what the input is. '\
                                                                        'eg. for LoadWCSim this would be required and set to \"InputFile\", '\
                                                                        'while for DataDecoder this argument is not required.')
    optional_args.add_argument('--copy_out_script',                help='Use the supplied COPY_OUT_SCRIPT (located on pnfs). Otherwise all files will be copied as is to the DEST.')
    optional_args.add_argument('--no_rename', action='store_true', help='By default, output file we be renamed by prepending the input file name for uniqueness. '\
                                                                        'Using this flag will turn off that "feature"')
    optional_args.add_argument('--export',                         help='Export environment variable to the grid. It must be already set in your current environment. You can pass in multiple variables. ')

    job_control_args = parser.add_argument_group('Job control args', 'Optional arguments for additional job control')
    job_control_args.add_argument('--njobs',             type=int, default=0,       help='Number of jobs to submit')
    job_control_args.add_argument('--files_per_job',     type=int, default=0,       help='Number of files per job. If zero, calculate from number of jobs')
    job_control_args.add_argument('--nevents',           type=int, default=-1,      help='Number of events per file to process')
    job_control_args.add_argument('--disk',              type=int, default=10000,   help='Local disk space requirement for worker node in MB. (default 10000MB (10GB))')
    job_control_args.add_argument('--memory',            type=int, default=1900,    help='Local memory requirement for worker node in MB. (default 1900MB (1.9GB)')
    job_control_args.add_argument('--cpu',               type=int, default=1,       help='Request worker nodes that have at least NUMBER cpus')    
    job_control_args.add_argument('--expected_lifetime',           default="10800", help='Expected job lifetime (default is 10800s=3h). '\
                                                                                         'Valid values are an integer number of seconds or one of '\
                                                                                         '\"short\" (6h), \"medium\" (12h) or \"long\" (24h, jobsub default)')
    job_control_args.add_argument('--grace_memory',                default='1024',  help='Auto-releese jobs which become held due to insufficient memory '\
                                                                                         'and resubmit with this additional memory (in MB)')
    job_control_args.add_argument('--grace_lifetime',              default='10800', help='Auto-release jobs which become held due to insufficient lifetime '\
                                                                                         'and resubmit with this additional lifetime (in seconds)')

    job_control_args.add_argument('--continue_project',  metavar='PROJECT_NAME', default="",      help='Do not start a new samweb project, '\
                                                                                                       'instead continue the specified one.')
    job_control_args.add_argument('--exclude_site',      metavar='SITE',         action='append', help='Specify an offsite location to exclude.')

    debug_args = parser.add_argument_group('Debugging options', 'These are optional arguments that are useful for debugging or testing')
    debug_args.add_argument('--print_jobsub',    action='store_true', help='Print jobsub command')
    debug_args.add_argument('--test_submission', action='store_true', help='Override other arguments given to submit a test to the grid.'\
                                                                           'It will run 1 job with 3 events and write the output to '\
                                                                           '/pnfs/nova/scratch/users/<user>/test_jobs/<date>_<time>')

    support_args = parser.add_argument_group("HELP!", "")
    support_args.add_argument("-h", "--help", action="help",   help='Show this help message and exit')
    support_args.add_argument('-f', '--file', action='append', help='''Text file containing any arguments to this utility.  Multiple allowed.
                                                                       Arguments should look just like they would on the command line,
                                                                       but the parsing of this file is whitespace insenstive.
                                                                       Comments will be identified with the # character and removed. ''')

    args = parser.parse_args()

    #---------------------------------------------------------------------------
    # Do the work
    #---------------------------------------------------------------------------

    # Check for test submission. Has to be first to override other arguments
    if args.test_submission:
        test_njobs = 1
        test_nevts = 3
        test_dest = "/pnfs/annie/scratch/users/%s/test_jobs/%s" % (os.environ["USER"], timestamp)
        if not os.path.exists(test_dest):
            os.makedirs(test_dest)
            mode = os.stat(test_dest).st_mode | stat.S_IXGRP | stat.S_IWGRP
            os.chmod(test_dest, mode)
        test_expected_lifetime = "0"
        test_dynamic_lifetime = "500"
        test_files_per_job = 1

        print("Running a test submission. Overwriting:")

        print("  njobs", args.njobs, "-->", test_njobs)
        args.njobs = test_njobs
        print("  nevts", args.nevts, "-->", test_nevts)
        args.nevts = test_nevts
        print("  dest", args.dest, "-->", test_dest)
        args.dest = test_dest
        print("  expected_lifetime", args.expected_lifetime, "-->", test_expected_lifetime)
        args.expected_lifetime = test_expected_lifetime
        print("  dynamic_lifetime", args.dynamic_lifetime, "-->", test_dynamic_lifetime)
        args.dynamic_lifetime = test_dynamic_lifetime
        print("  files_per_job", args.files_per_job, "-->", test_files_per_job)
        args.files_per_job = test_files_per_job

        
    ##########################
    # Setup jobsub stuff
    ##########################
    njobs = args.njobs
    files_per_job = args.files_per_job
    #samweb = samweb_client.SAMWebClient(experiment='annie')
        
    if files_per_job > 0 and njobs > 0:
        jobsub_opts += ['-N %d' %njobs]
        annie_sam_wrap_opts += ['--nevents %d' %files_per_job]
            
    elif files_per_job > 0:
        # Files per job defined, but njobs not. Calculate on the fly
        num_files = 10 #samweb.countFiles(defname=args.defname)
        njobs=(old_div(num_files, files_per_job)) +1
        
        jobsub_opts += ['-N %d' %njobs]
        annie_sam_wrap_opts += ['--nevents %d' %files_per_job]
        
    elif njobs > 0:
        # Njobs given but not files/job, that's fine
        jobsub_opts += ['-N %d' %njobs]

    else:
        warn('Neither --njobs or --files_per_job were specified. Are you sure you want that? '\
             'I\'ll sleep for 5 seconds while you think about it')
        sleep(5)

    # Limit the number of jobs that a user tries to submit
    if njobs > 5000:
        print('''
        Error: cannot submit more than 5000 jobs in one cluster.
        Please break your submission into multiple batches of 5000 (or less) jobs,
        and after submitting the first batch, use --continue_project with the project
        that results from the first submission for the remaining batches.

        Please separate submissions by 5 minutes.
        ''', file=sys.stderr)
        sys.exit(1)

    # Jobsub options
    if args.exclude_site:
        for isite in args.exclude_site:
            jobsub_opts += [ "--append_condor_requirements='(TARGET.GLIDEIN_Site\\ isnt\\ \\\"%s\\\")'" % isite ]
            #jobsub_opts += [ "--append_condor_requirements='(TARGET.GLIDEIN_Site\ isnt\ \\\"%s\\\")'" % isite ]

    if args.disk:
        disk_opt="--disk=%sMB" % (args.disk)
        jobsub_opts += [ disk_opt ]

    if args.memory:
        mem_opt="--memory=%sMB" % (args.memory)
        jobsub_opts += [ mem_opt ]

    if args.cpu:
        cpu_opt="--cpu=%d" % (args.cpu)
        jobsub_opts += [ cpu_opt ]

    # autorelease options
    jobsub_opts += ["--lines '+FERMIHTC_AutoRelease=True'"]
    jobsub_opts += ["--lines '+FERMIHTC_GraceMemory=" + args.grace_memory + "'"]
    jobsub_opts += ["--lines '+FERMIHTC_GraceLifetime=" + args.grace_lifetime + "'"]

    #expected lifetime can be an in (number of secs) or
    # one of a few strings, this should test for either
    # possibility
    try:
        dummy=int(args.expected_lifetime)
        jobsub_opts += ["--expected-lifetime=%ss" % (args.expected_lifetime)]
    except:
        allowed_lifetimes=["short","medium","long"]
        if args.expected_lifetime not in allowed_lifetimes:
            fail("Invalid expected_lifetime %s" % args.expected_lifetime)
        else:
            jobsub_opts += ["--expected-lifetime=%s" % (args.expected_lifetime)]

    jobsub_opts += ["-G annie"]

    if args.test:
        jobsub_opts += ["--no-submit"]
        jobsub_opts += ["--debug"]

    if args.input_file is not None:
        for input_file in args.input_file:
            if not os.path.isfile(os.path.expandvars(input_file)):
                fail("Input file %s does not exist!" % input_file)

                if not os.path.expandvars(input_file).startswith("/pnfs/"):
                    fail("Input file %s must be in dCache /pnfs/annie/" % input_file)

                    jobsub_opts += ['-f dropbox://%s' % input_file]

    if args.copy_out_script is not None:
        if not os.path.isfile(os.path.expandvars(args.copy_out_script)):
            fail("Copyout script %s does not exist!" %args.copy_out_script)

            if not os.path.expandvars(args.copy_out_script).startswith("/pnfs/"):
                    fail("Copyout script %s must be in /pnfs/annie/" %args.copy_out_script)

                    jobsub_opts += ['-f dropbox://%s' % args.copy_out_script]

    if args.export:
        export_to_annie_sam_wrap += args.export

        
    ##########################################################
    # Start the project and setup additional SAM wrapper stuff
    #########################################################
    if not args.continue_project:
        ##start sam project
        project_name = user + "-" + args.jobname + "-" + timestamp
        if args.test_submission:
            project_name += "-testjobs"
        start_project = True
    else:
        project_name = args.continue_project
        start_project = False

    sam_station=os.getenv("SAM_STATION")
    if start_project and not args.test:
        print('starting %s' %project_name)
        start_proj_retval = samweb.startProject(project_name, defname=defname,
                            group='annie', station=sam_station)        

    os.putenv("SAM_PROJECT_NAME",project_name)

        
    annie_sam_wrap_opts += ['--dest %s' %args.dest]
    annie_sam_wrap_opts += ['--config %s' %args.config]
    annie_sam_wrap_opts += ['--input_file_config %s' %args.input_file_config]
    annie_sam_wrap_opts += ['--nevents %s' %args.nevents]
    if args.input_config_var:
        annie_sam_wrap_opts += ['--input_config_var %s' %args.input_config_var]
    if args.copy_out_script:
        annie_sam_wrap_opts += ['--copy_out_script %s' %args.copy_out_script]
    if args.tarball:
        annie_sam_wrap_opts += ['--tarball %s' %os.path.basename(args.tarball)]
    if not args.no_rename:
        annie_sam_wrap_opts += ['--rename_outputs']
    
    
    
    
    
    ############################
    # Actually launch the jobs #
    ############################

    jobsub_cmd = build_jobsub_cmd(jobsub_opts)

    if args.print_jobsub or args.test:
        print(jobsub_cmd)
        sys.stdout.flush()
        sys.stderr.flush()

#    os.system(jobsub_cmd)
