#!/bin/env python3

from __future__ import print_function
from __future__ import division
#from future import standard_library
#standard_library.install_aliases()
from builtins import str
#from past.utils import old_div
import os, sys, stat, pwd, re, glob
import argparse
import datetime
import samweb_client
import string
import tokenize
import io
import subprocess

# "Working" sites for all experiments from https://cdcvs.fnal.gov/redmine/projects/fife/wiki/Information_about_job_submission_to_OSG_sites
recommended_sites = [
    "BNL",       "Caltech",   "Clemson-Palmetto",        "Colorado",
    "Cornell",   "FermiGrid", "FNAL",                    "Michigan",
    "NotreDame", "Omaha",     "SLATE_US_NMSU_DISCOVERY", "SU-ITS",
    "UChicago",  "UConn-HPC", "UCSD",                    "Wisconsin"
    ]

# Discovered through testing
excluded_sites = [ 'Omaha', 'Swan', 'Wisconsin']

jobsub_opts = []

annie_sam_wrap_cmd =os.getenv('ANNIEGRIDUTILSDIR')+'/annie_sam_wrap.sh'
annie_sam_wrap_opts = []

export_to_annie_sam_wrap = ['GRID_USER', 'EXPERIMENT', 'SAM_EXPERIMENT', 'SAM_STATION', 'SAM_PROJECT_NAME', 'IFDH_BASE_URI']

# setup usage models to use and  sites we're going to use
usage_models = ['DEDICATED,OPPORTUNISTIC']

input_files = []
early_sources = []
early_scripts = []
sources = []
pre_scripts = []
post_scripts = []

timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
user=os.getenv("USER")

def remove_comments(src):
    '''
    This reads tokens using tokenize.generate_tokens and recombines them
    using tokenize.untokenize, and skipping comment/docstring tokens in between
    '''
    f = io.StringIO(src.encode().decode())
    class SkipException(Exception): pass
    processed_tokens = []
    last_token = None
    # go thru all the tokens and try to skip comments and docstrings
    for tok in tokenize.generate_tokens(f.readline):
        t_type, t_string, t_srow_scol, t_erow_ecol, t_line = tok

        try:
            if t_type == tokenize.COMMENT:
                raise SkipException()

            elif t_type == tokenize.STRING:

                if last_token is None or last_token[0] in [tokenize.INDENT]:
                    pass

        except SkipException:
            pass
        else:
            processed_tokens.append(tok)

        last_token = tok

    return tokenize.untokenize(processed_tokens)

def build_jobsub_cmd(jobsub_opts):
    jobsub_cmd = 'jobsub_submit'

    # Add exported environment variables
    jobsub_opts += ['-e ' + export for export in export_to_annie_sam_wrap]
        
    # Add tarball and SAM wrapper script with its options
    #jobsub_opts += ['--tar_file_name dropbox://' + args.tarball]
    jobsub_opts += ['-f dropbox://' + args.tarball]
    jobsub_opts += ['file://' + annie_sam_wrap_cmd]
    jobsub_opts += [' ' + ' '.join(annie_sam_wrap_opts)]

    jobsub_cmd += ' ' + ' '.join([opt for opt in jobsub_opts])

    return jobsub_cmd

    
#######################################################################################
if __name__=='__main__':
    prog=os.path.basename(sys.argv[0])

    while "-f" in sys.argv or "--file" in sys.argv:
        ### Allow args to be passed in as a plain text file.
        ### We make a preliminary parser get these arguments out for two reasons:
        ###    1)  Maintain standard -h, --help functionality
        ###    2)  Avoid necessity required arguments in initial parsing,
        ###        allow them to be missing, but find them in the file.
        preliminary_parser = argparse.ArgumentParser(prog=prog, description='Submit annie grid job')

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
                    print("%s was not found. Exiting." %filepath)
                    sys.exit(1)
                text = open(fullpath, 'r').read()
                text = remove_comments(text) # Strip out commented lines
                newargs = []
                for line in text.splitlines():
                    # Insert arguments into list in order
                    # where the -f appeared
                    newargs += line.split()
                sys.argv[index:index] = newargs

    
    parser = argparse.ArgumentParser(description="Submit ANNIE grid job", add_help=False)

    required_args = parser.add_argument_group('Required arguments', 'These arguments must be supplied')
    required_args.add_argument('--jobname',           required=True, help='Job name');
    required_args.add_argument('--dest',              required=True, help='Destination for outputs')
    required_args.add_argument('--config', '-c',      required=True, help='Tool chain config to use. The config should be a directory in ToolAnalysis/config/')
    required_args.add_argument('--input_file_config', required=True, help='File in the toolchain config directory that defines the input file that is run over. '\
                                                                          'eg. for LoadWCSim this would be \"LoadWCSimConfig\", '\
                                                                          'while for DataDecoder this would be \"my_files.txt\"')
    required_args.add_argument('--defname',           required=True, help='SAM dataset definition to run over')
    required_args.add_argument('--tarball',           required=True, help='If you want to use your local ToolAnalysis then pass in a tarball here.'\
                                                                          'Otherwise the base build from the singularity container will be used.'\
                                                                          'Note: you DO NOT have to move your tarball to your pnfs scratch area.')
    
    optional_args = parser.add_argument_group('Other optional arguments', 'These arguments are optional')
    optional_args.add_argument('--input_config_var',               help='Variable name in the input_file_config that defines what the input is. '\
                                                                        'eg. for LoadWCSim this would be required and set to \"InputFile\", '\
                                                                        'while for DataDecoder this argument is not required.')
    optional_args.add_argument('--no_job_dirs', action='store_true', help='By default directories will be created in DEST for each job number in order to prevent '\
                                                                          'overpopulating pnfs directories. Using this flag will turn off that feature.')
    optional_args.add_argument('--no_rename',   action='store_true', help='By default output file we be renamed by prepending the input file name for uniqueness. '\
                                                                          'Using this flag will turn off that feature')                                                                        
    optional_args.add_argument('--quick_copy',   action='store_true', help='By default output files are copied back at then end of all executions. '\
                                                                          'Using this flag will copy out file right after they are created.')                                                                    
    optional_args.add_argument('--copy_out_script',                  help='Use the supplied COPY_OUT_SCRIPT (located on pnfs). Otherwise all files will be copied as is to the DEST.')
    optional_args.add_argument('--input_file',  action='append',     help='Copy an extra file to the grid node. You can use this multiple times.')
    optional_args.add_argument('--export',      action='append',     help='Export environment variable to the grid. It must be already set in your current environment. '\
                                                                          'You can pass in multiple variables. ')
    optional_args.add_argument('--earlysource', action='append',     help='Source this script before doing anything else in the job. '\
                                                                          'You can use this multiple times. Syntax is colon separated arguments (ie. script:arg:arg...)')
    optional_args.add_argument('--earlyscript', action='append',     help='Execute this script before doing anything else in the job (other than any earlysources you\'ve requested. '\
                                                                          'You can use this multiple times. Syntax is colon separated arguments (ie. script:arg:arg...)')
    optional_args.add_argument('--source',      action='append',     help='Source this script after any specified "early" scripts or sources. '\
                                                                          'You can use this multiple times. Syntax is colon separated arguments (ie. script:arg:arg...)')
    optional_args.add_argument('--prescript',   action='append',     help='Execute this script before running the ToolChain. '\
                                                                          'You can use this multiple times. Syntax is colon separated arguments (ie. script:arg:arg...)')
    optional_args.add_argument('--postscript',  action='append',     help='Execute this script after running the ToolChain on all files but before copying them out. '\
                                                                          'You can use this multiple times. Syntax is colon separated arguments (ie. script:arg:arg...)')

    job_control_args = parser.add_argument_group('Job control args', 'Optional arguments for additional job control')
    job_control_args.add_argument('--njobs',             type=int, default=0,       help='Number of jobs to submit')
    job_control_args.add_argument('--maxConcurrent',     type=int, default=0,       help='Run a maximum of N jobs simultaneously')
    job_control_args.add_argument('--files_per_job',     type=int, default=0,       help='Number of files per job. If zero, calculate from number of jobs')
    job_control_args.add_argument('--nevents',           type=int, default=-1,      help='Number of events per file to process')
    job_control_args.add_argument('--disk',              type=int, default=10000,   help='Local disk space requirement for worker node in MB. (default 10000MB (10GB))')
    job_control_args.add_argument('--memory',            type=int, default=1900,    help='Local memory requirement for worker node in MB. (default 1900MB (1.9GB))')
    job_control_args.add_argument('--cpu',               type=int, default=1,       help='Request worker nodes that have at least NUMBER cpus')    
    job_control_args.add_argument('--expected_lifetime',           default="10800", help='Expected job lifetime (default is 10800s=3h). '\
                                                                                         'Valid values are an integer number of seconds or one of '\
                                                                                         '\"short\" (6h), \"medium\" (12h) or \"long\" (24h, jobsub default)')
    job_control_args.add_argument('--grace_memory',                default='1024',  help='Auto-releese jobs which become held due to insufficient memory '\
                                                                                         'and resubmit with this additional memory (in MB)')
    job_control_args.add_argument('--grace_lifetime',              default='10800', help='Auto-release jobs which become held due to insufficient lifetime '\
                                                                                         'and resubmit with this additional lifetime (in seconds)')

    job_control_args.add_argument('--continue_project',  metavar='PROJECT_NAME', default="",          help='Do not start a new samweb project, '\
                                                                                                           'instead continue the specified one.')
    job_control_args.add_argument('--site',                                      action='append',     help='Specify allowed offsite locations.  Omit to allow running at any offsite location')
    job_control_args.add_argument('--exclude_site',      metavar='SITE',         action='append',     help='Specify an offsite location to exclude.')
    job_control_args.add_argument('--all_sites',                                 action="store_true", help="Remove all specific site requirements.")
    job_control_args.add_argument('--onsite_only',                               action='store_true', help='Allow to run solely on onsite resources.')
    job_control_args.add_argument('--offsite_only',                              action='store_true', help='Allow to run solely on offsite resources.')
    job_control_args.add_argument('--grid_sl7',                                  action='store_true', help='Run in SL7 on the grid. By default, ANNIE submissions use the local AL9 environment.')

    debug_args = parser.add_argument_group('Debugging options', 'These are optional arguments that are useful for debugging or testing')
    debug_args.add_argument('--print_jobsub',    action='store_true', help='Print jobsub command')
    debug_args.add_argument('--test',            action='store_true', help='Do not actually do anything, just run tests and print jobsub cmd')
    debug_args.add_argument('--test_submission', action='store_true', help='Override other arguments given to submit a test to the grid.'\
                                                                           'It will run 1 job with 3 events and write the output to '\
                                                                           '/pnfs/nova/scratch/users/<user>/test_jobs/<date>_<time>')
    debug_args.add_argument("--kill_after", metavar="SEC", type=int, help='If job is still running after this many seconds, kill in such a way that a log will be returned')

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

    if args.onsite_only and args.offsite_only:
        fail("Cannot specify onsite_only and offsite_only")

    if not args.onsite_only :
        usage_models.append("OFFSITE")
        export_to_annie_sam_wrap.append("IS_OFFSITE=1")
    if args.offsite_only:
        usage_models = ["OFFSITE"]
        export_to_annie_sam_wrap.append("IS_OFFSITE=1")
        
    use_recommended_sites=False
    if not args.onsite_only and not args.site and not args.all_sites:
        use_recommended_sites=True


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
        print("  nevts", args.nevents, "-->", test_nevts)
        args.nevents = test_nevts
        print("  dest", args.dest, "-->", test_dest)
        args.dest = test_dest
        print("  expected_lifetime", args.expected_lifetime, "-->", test_expected_lifetime)
        args.expected_lifetime = test_expected_lifetime
        # print("  dynamic_lifetime", args.dynamic_lifetime, "-->", test_dynamic_lifetime)
        # args.dynamic_lifetime = test_dynamic_lifetime
        print("  files_per_job", args.files_per_job, "-->", test_files_per_job)
        args.files_per_job = test_files_per_job

        
    ##########################
    # Setup jobsub stuff
    ##########################
    njobs = args.njobs
    files_per_job = args.files_per_job
    samweb = samweb_client.SAMWebClient(experiment='annie')
        
    if files_per_job > 0 and njobs > 0:
        jobsub_opts += ['-N %d' %njobs]
        annie_sam_wrap_opts += ['--limit %d' %files_per_job]
            
    elif files_per_job > 0:
        # Files per job defined, but njobs not. Calculate on the fly
        num_files = samweb.countFiles(defname=args.defname)
        #njobs=(old_div(num_files, files_per_job)) +1
        njobs=(num_files//files_per_job) +1
        
        jobsub_opts += ['-N %d' %njobs]
        annie_sam_wrap_opts += ['--limit %d' %files_per_job]
        
    elif njobs > 0:
        # Njobs given but not files/job, that's fine
        jobsub_opts += ['-N %d' %njobs]

    else:
        warn('Neither --njobs or --files_per_job were specified. Are you sure you want that? '\
             'I\'ll sleep for 5 seconds while you think about it')
        sleep(5)

    # Limit the number of jobs that a user tries to submit
    if njobs > 5000 and not args.maxConcurrent:
        print('''
        Error: cannot submit more than 5000 jobs in one cluster.
        Please break your submission into multiple batches of 5000 (or less) jobs,
        and after submitting the first batch, use --continue_project with the project
        that results from the first submission for the remaining batches.

        Please separate submissions by 5 minutes.
        ''', file=sys.stderr)
        sys.exit(1)

    if args.maxConcurrent:
        jobsub_opts += ["--maxConcurrent=%d" %args.maxConcurrent]
        if args.maxConcurrent > 25000:
            print('''
            Error: cannot submit more than 25000 jobs to the grid so maxConcurrent shouldn't be higher than that.
            ''', file=sys.stderr)
            sys.exit(1)

    # Jobsub options
    resource_opt="--resource-provides=usage_model=" + ",".join( usage_models )
    jobsub_opts += [resource_opt]

    if use_recommended_sites or args.site and not args.all_sites:
        site_opt="--site="

        if use_recommended_sites:
            for isite in recommended_sites:
                site_opt += isite + ","
        if args.site:
            for isite in args.site:
                if isite not in recommended_sites:
                    warn("Site "+isite+" is not known to work. Your jobs may fail at that site. Sleeping for 5 seconds")
                    sleep(5)
                site_opt += isite + ","

        site_opt=site_opt[:-1]
        jobsub_opts += [ site_opt ]


    if args.exclude_site:
        for isite in args.exclude_site:            
            excluded_sites += [ isite ]
            
    for isite in excluded_sites:
        jobsub_opts += [ "--append_condor_requirements='(TARGET.GLIDEIN_Site\\ isnt\\ \\\"%s\\\")'" % isite ]

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

    if args.grid_sl7:        
        jobsub_opts += ["--singularity-image /cvmfs/singularity.opensciencegrid.org/fermilab/fnal-wn-sl7:latest"]
        
    # expected lifetime can be an in (number of secs) or
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

    if args.input_file:
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

    if args.earlysource:
        for script in args.earlysource:
            if ":" in script:
                script_path = script.split(":")[0]
            else:
                script_path = script

            if not os.path.isfile(os.path.expandvars(script)):
                fail("Input file %s does not exist!" % script)

            if not os.path.expandvars(script).startswith("/pnfs/"):
                fail("Input file %s must be in dCache /pnfs/annie/" % script)

            jobsub_opts += ['-f dropbox://%s' % script_path]
            annie_sam_wrap_opts += ['--earlysource %s' % script]


    if args.earlyscript:
        for script in args.earlyscript:
            if ":" in script:
                script_path = script.split(":")[0]
            else:
                script_path = script

            if not os.path.isfile(os.path.expandvars(script)):
                fail("Input file %s does not exist!" % script)

            if not os.path.expandvars(script).startswith("/pnfs/"):
                fail("Input file %s must be in dCache /pnfs/annie/" % script)

            jobsub_opts += ['-f dropbox://%s' % script_path]
            annie_sam_wrap_opts += ['--earlyscript %s' % script]
    
    if args.source:
        for script in args.source:
            if ":" in script:
                script_path = script.split(":")[0]
            else:
                script_path = script

            if not os.path.isfile(os.path.expandvars(script)):
                fail("Input file %s does not exist!" % script)

            if not os.path.expandvars(script).startswith("/pnfs/"):
                fail("Input file %s must be in dCache /pnfs/annie/" % script)

            jobsub_opts += ['-f dropbox://%s' % script_path]
            annie_sam_wrap_opts += ['--source %s' % script]

    if args.prescript:
        for script in args.prescript:
            if ":" in script:
                script_path = script.split(":")[0]
            else:
                script_path = script

            if not os.path.isfile(os.path.expandvars(script)):
                fail("Input file %s does not exist!" % script)

            if not os.path.expandvars(script).startswith("/pnfs/"):
                fail("Input file %s must be in dCache /pnfs/annie/" % script)

            jobsub_opts += ['-f dropbox://%s' % script_path]
            annie_sam_wrap_opts += ['--prescript %s' % script]


    if args.postscript:
        for script in args.postscript:
            if ":" in script:
                script_path = script.split(":")[0]
            else:
                script_path = script

            if not os.path.isfile(os.path.expandvars(script)):
                fail("Input file %s does not exist!" % script)

            if not os.path.expandvars(script).startswith("/pnfs/"):
                fail("Input file %s must be in dCache /pnfs/annie/" % script)

            jobsub_opts += ['-f dropbox://%s' % script_path]
            annie_sam_wrap_opts += ['--postscript %s' %script]

    if not os.path.expandvars(args.dest).startswith("/pnfs/"):
        fail("Destination directory %s must be in dCache /pnfs/annie/" % args.dest)
   
    export_to_annie_sam_wrap.append("DEST=%s" % args.dest)


        
    ##########################################################
    # Start the project and setup additional SAM wrapper stuff
    #########################################################
    if not args.continue_project:
        ##start sam project
        project_name = user + "_" + args.jobname + "_" + timestamp
        if args.test_submission:
            project_name += "_testjobs"
        start_project = True
    else:
        project_name = args.continue_project
        start_project = False

    sam_station=os.getenv("SAM_STATION")
    if start_project and not args.test:
        print('\nstarting %s\n' %project_name)
        start_proj_retval = samweb.startProject(project_name, defname=args.defname,
                            group='annie', station=sam_station)        

    os.putenv("SAM_PROJECT_NAME",project_name)

    annie_sam_wrap_opts += ['--tarball %s' %os.path.basename(args.tarball)]
    annie_sam_wrap_opts += ['--config %s' %args.config]
    annie_sam_wrap_opts += ['--input_file_config %s' %args.input_file_config]
    annie_sam_wrap_opts += ['--nevents "%s"' %args.nevents]
    if args.input_config_var:
        annie_sam_wrap_opts += ['--input_config_var %s' %args.input_config_var]
    if args.copy_out_script:
        annie_sam_wrap_opts += ['--copy_out_script %s' %os.path.basename(args.copy_out_script)]
    if not args.no_rename:
        annie_sam_wrap_opts += ['--rename_outputs']
    if not args.no_job_dirs:
        annie_sam_wrap_opts += ['--job_dirs']
    if args.quick_copy:
        annie_sam_wrap_opts += ['--quick_copy']
    if args.kill_after:
        annie_sam_wrap_opts += [ "--self_destruct_timer %d" % args.kill_after ]

    
    
    ############################
    # Actually launch the jobs #
    ############################

    jobsub_cmd = build_jobsub_cmd(jobsub_opts)

    if args.print_jobsub or args.test:
        print(jobsub_cmd)
        sys.stdout.flush()
        sys.stderr.flush()

    os.system(jobsub_cmd)

    files=glob.glob("./*.tbz2")
    for file in files:
        print(file)
        os.remove(file)
