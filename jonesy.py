import os

from dotenv import dotenv_values
from jonesy.jobs import Job


config = {
    **dotenv_values(".env.shared"),
    **dotenv_values(".env.secret"),
    **os.environ, 
}

if 'JOB' not in os.environ:
    print('No job specified, aborting')
else:
    Job(os.environ['JOB'], config).run()
