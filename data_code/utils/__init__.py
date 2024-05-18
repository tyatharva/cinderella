#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from cdo import Cdo
import boto3
from botocore import UNSIGNED
from botocore.config import Config

cdo = Cdo()
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
datdir = 'data'

__all__ = ['cdo', 's3', 'datdir']