#!/usr/bin/env python3
"""Five-layer validation for materialized OMOP CDM tables in Unity Catalog.

Auth is handled by Databricks runtime when invoked from Genie Code Agent.
--profile only applies for local development against ~/.databrickscfg.
"""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


@dataclass
class ColSpec:
    name: str
    sql_type: str
    nullable: bool
    pk: bool
    fk: str | None
    domain: str | None


