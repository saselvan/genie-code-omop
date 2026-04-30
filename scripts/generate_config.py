#!/usr/bin/env python3
"""Generate a stub OMOP transform YAML from a bronze table DESCRIBE (Databricks SQL).

Pure pass-through scaffolder: for every bronze column, emits
  {target: snake_case(col), expr: f"src.{col}"}

No domain heuristics. The agent rewrites column_mappings based on the OMOP
target columns, the resolution decision tree (MANDATORY rule 3 in SKILL.md),
and the canonical condition_occurrence example. Structural patterns
(resolution strategies, two-lookup rule, hash keys, domain_id) come from
the skill, not from this script.

Auth is handled by Databricks runtime when invoked from Genie Code Agent.
--profile only applies for local development against ~/.databrickscfg.
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


