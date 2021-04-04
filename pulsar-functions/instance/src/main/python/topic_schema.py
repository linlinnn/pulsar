#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# -*- encoding: utf-8 -*-

"""topic_schema.py: Get schema
"""
import os
import inspect
import sys
import util
import schema
import importlib
from threading import Timer

import log

Log = log.Log

def get_schema(conf):
    schema_type = conf.schemaType
    schema_properties = conf.schemaProperties
    if schema_type == "BYTES":
        return schema.BytesSchema()
    elif schema_type == "STRING":
        return schema.StringSchema()
    elif schema_type == "JSON":
        return schema.JsonSchema()
    elif schema_type == "AVRO":
        return schema.AvroSchema()
    else:
        raise TypeError("Unsupported schema type" + schema_type)
        

def serde_schema(from_path, serde_class_name):
    serde_kclass = util.import_class(from_path, serde_class_name)
    return schema.SerDeSchema(serde_kclass())