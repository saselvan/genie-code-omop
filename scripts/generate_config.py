#!/usr/bin/env python3
"""Generate a stub OMOP transform YAML from a bronze table DESCRIBE (Databricks SQL).

Pure pass-through scaffolder: for every bronze column, emits
  {target: snake_case(col), expr: f"src.{col}"}

No domain heuristics. The agent rewrites column_mappings based on the OMOP
target columns, the resolution decision tree (MANDATORY rule 3 in SKILL.md),
and the canonical condition_occurrence example. Structural patterns
(resolution strategies, two-lookup rule, hash keys, domain_id) come from
the skill, not from this script.

Two invocation modes:
  - Default (explicit): --bronze-table FQN + --catalog + --bronze-schema.
    Use this when you don't have a discovery.yaml yet — i.e., the cold
    start path that every first-time user takes. No setup file required.
  - Fast path (lookup): --discovery-file <path> + --omop-table.
    Use this only when an up-to-date discovery.yaml already exists from
    a prior session. discovery.yaml is an OPTIONAL artifact the agent
    writes with user consent at the end of SKILL.md Step 4 — never a
    precondition to using the skill.

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


