import os
from ctm_python_client.core.workflow import Workflow, WorkflowDefaults
from ctm_python_client.core.comm import Environment
from aapi import *
import boto3
import pandas as pd

aws_env = Workflow(Environment.create_onprem(host='dba-tlv-wo2bil.bmc.com',
                                             username='emuser',
                                             password='empass'),
                   WorkflowDefaults(run_as='emuser'))

run_as_dummy = True

pythonTransform = JobCommand(
    'InventoryTransforms',
    run_as="dbauser",
    command="/home/dbauser/bin/inventory_transforms/inventory-transforms.py",
    run_as_dummy=run_as_dummy
)

pythonTransform2 = JobCommand(
    'InventoryTransforms2',
    run_as="dbauser",
    command="/home/dbauser/bin/inventory_transforms/inventory-transforms.py",
    run_as_dummy=run_as_dummy
)

pythonTransform3 = JobCommand(
    'InventoryTransforms3',
    run_as="dbauser",
    command="/home/dbauser/bin/inventory_transforms/inventory-transforms.py",
    run_as_dummy=run_as_dummy
)

aws_env.chain([pythonTransform,pythonTransform2], inpath='ChainedJobs')
aws_env.chain([pythonTransform,pythonTransform3],inpath='ChainedJobs')


if aws_env.build():
    print('The workflow is valid!')

if aws_env.deploy():
    print('The workflow is deployed!')