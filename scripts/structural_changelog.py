#!/usr/bin/env python3
"""Field-level structural diff between two OMOP YAML configs.

Phase 3 Step 5 of omop-pipeline-builder v2.0. The Update sub-path's
prompt template needs a structured description of what changed
between the existing config and the regenerated config so the
reviewer-ratification step (Step 4 of SKILL.md) can scope its
fidelity checks to the actual deltas. The changelog is also
informational input to the response template — telling the engineer
"the regenerated config changes vocabulary_lookups[2].vocabulary_id
from X to Y" is much more reviewable than "we rewrote the whole
file."

Decision 9 (textual-vs-structural): this module is the structural
side. The skill never produces a textual line-diff because the
agent regenerates the WHOLE config — line-by-line comparison
against an LLM-rewritten file produces noise that overwhelms the
real changes. Structural diff via the Pydantic model levels the
playing field: two configs that are semantically identical but
formatted differently produce zero changes here.

Validation is the entry gate: both YAMLs must parse and validate
against the embedded ``OMOPConfig`` schema. A YAML that doesn't
validate cannot be diffed because we can't trust its semantic
shape; the agent's prompt template handles this case by surfacing
"the existing config doesn't validate against the current schema —
run Replace instead of Update" to the engineer.

Decision: hand-rolled diff via ``model_dump()`` rather than a
``deepdiff`` dependency. Trade-off: ~50 lines of code vs an
external package surface. The OMOP config schema is finite and the
recursion shape (dict | list | scalar) is well-bounded; rolling
our own keeps the dependency footprint minimal and gives clean
control over the output shape (dotted paths, bracket indices).

Auth boilerplate not applicable (pure Python; no SDK calls).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from validate_yaml_schema import OMOPConfig, validate_text

# Path-component separator. The Pydantic schema uses snake_case keys
# so "." is unambiguous (no key contains a literal dot). List indices
# use bracket notation appended to the parent key; nested lists nest
# bracket pairs (e.g., ``foo[0][1]``). Both conventions match the
# Phase 3 spec example: ``column_mappings[3].target``.
_PATH_DOT = "."


@dataclass
class FieldChange:
    """One semantic change between two OMOP configs.

    ``field_path`` uses dotted snake_case for dict keys and bracket
    notation for list indices, e.g.:

      - ``table_name``
      - ``sources[0].alias``
      - ``vocabulary_lookups[2].vocabulary_id``
      - ``expectations.fail[0].expr``

    ``change_type`` is one of:

      - ``"added"``: the path exists in NEW but not in OLD. ``old_value``
        is ``None`` for this case (the path was missing, not None-valued).
      - ``"removed"``: the path exists in OLD but not in NEW. ``new_value``
        is ``None``.
      - ``"modified"``: the path exists in both with different values.

    The values are the deepest comparable units — a whole new
    ``VocabularyLookup`` dict shows up as a single ``added`` change
    rather than as N ``added`` changes for each sub-field. This keeps
    the changelog at the "thing the engineer reviews" level rather
    than the "every leaf scalar" level.
    """

    field_path: str
    change_type: Literal["added", "removed", "modified"]
    old_value: Any
    new_value: Any


def compute_structural_changelog(
    old_yaml: str,
    new_yaml: str,
) -> list[FieldChange]:
    """Compute the field-level changelog between two OMOP YAML configs.

    Parses both inputs as :class:`OMOPConfig` (the same Pydantic
    schema the runtime config loader uses). Walks the dumped dict
    structures field-by-field, yielding one :class:`FieldChange` per
    semantic delta.

    Identical configs (modulo formatting and Pydantic default-fill)
    return an empty list.

    Args:
        old_yaml: YAML text of the existing config (the file on
            disk before the Update).
        new_yaml: YAML text of the regenerated config (the bytes
            the agent is about to write).

    Returns:
        Ordered list of :class:`FieldChange`. Order is stable and
        deterministic but driven by **insertion order**, not sort
        order:

          - For shared sub-trees, traversal follows OLD's
            ``model_dump()`` iteration order, which Pydantic emits
            in schema field declaration order — matching the
            engineer's reading order in the existing YAML.
          - Keys present only in OLD emit ``removed`` interleaved
            at the position they appear in OLD's iteration order.
          - Keys present only in NEW (i.e., brand-new fields) are
            appended as ``added`` at the END of each dict level,
            in NEW's iteration order. This collects new fields at
            the bottom of each level rather than scattering them
            among the shared walk.

        List elements are compared positionally; an out-of-position
        list change surfaces as either a ``modified`` (if positions
        overlap) plus ``added`` / ``removed`` (if lengths differ).

    Raises:
        ValueError: either YAML fails OMOPConfig validation. The
            error message names which side failed and includes the
            Pydantic validation summary so the agent's prompt
            template can surface a concrete error to the engineer.
    """
    old_dump = _parse_or_value_error(old_yaml, side="old").model_dump()
    new_dump = _parse_or_value_error(new_yaml, side="new").model_dump()

    changes: list[FieldChange] = []
    _diff(old_dump, new_dump, path="", changes=changes)
    return changes


def _parse_or_value_error(yaml_text: str, *, side: str) -> OMOPConfig:
    """Parse + validate a YAML string; map any failure to ValueError.

    The schema's :class:`pydantic.ValidationError` is the underlying
    failure type, but the writer's contract surface is
    :class:`ValueError` so callers don't need a Pydantic import to
    handle the negative path. We wrap with the schema-side message
    intact so the agent can pass it through to the engineer.
    """
    try:
        return validate_text(yaml_text)
    except Exception as e:
        # Catch broadly: ``yaml.YAMLError`` (malformed YAML),
        # ``pydantic.ValidationError`` (schema mismatch),
        # ``TypeError`` from None-input. All are "this YAML is not a
        # valid OMOP config" from the caller's perspective.
        raise ValueError(
            f"{side} YAML failed OMOPConfig validation: "
            f"{type(e).__name__}: {e}"
        ) from e


def _diff(
    old: Any,
    new: Any,
    *,
    path: str,
    changes: list[FieldChange],
) -> None:
    """Recursive structural diff for dict | list | scalar.

    Mutates ``changes`` rather than returning, because the natural
    traversal order matches the agent's reading order (depth-first,
    schema field order for shared sub-trees). Returning a list per
    recursion would force a flatten step that loses ordering.
    """
    if old == new:
        return

    # Type mismatch: treat as a single 'modified' rather than
    # recursing. E.g., scalar -> dict at the same path is one
    # semantic change, not "scalar removed plus dict added."
    if type(old) is not type(new):
        changes.append(
            FieldChange(
                field_path=path or "<root>",
                change_type="modified",
                old_value=old,
                new_value=new,
            )
        )
        return

    if isinstance(old, dict):
        _diff_dict(old, new, path=path, changes=changes)
        return

    if isinstance(old, list):
        _diff_list(old, new, path=path, changes=changes)
        return

    # Scalar mismatch (str, int, float, bool, None, etc.).
    changes.append(
        FieldChange(
            field_path=path or "<root>",
            change_type="modified",
            old_value=old,
            new_value=new,
        )
    )


def _diff_dict(
    old: dict,
    new: dict,
    *,
    path: str,
    changes: list[FieldChange],
) -> None:
    """Diff two dicts at the same path.

    Order rule: shared keys are walked in OLD's iteration order
    (which matches Pydantic model field order via ``model_dump``);
    keys present only in NEW are appended as ``added`` after the
    shared walk; keys present only in OLD are emitted as ``removed``
    interleaved with the shared walk at the position they appear in
    OLD's iteration order.

    The "shared walk" + "trailing adds" pattern keeps the changelog
    readable: an engineer reading top-to-bottom sees changes in
    schema order, with brand-new fields collected at the end of
    each level.
    """
    new_only_keys = [k for k in new if k not in old]

    for key in old:
        sub_path = _join_path(path, key)
        if key not in new:
            changes.append(
                FieldChange(
                    field_path=sub_path,
                    change_type="removed",
                    old_value=old[key],
                    new_value=None,
                )
            )
        else:
            _diff(old[key], new[key], path=sub_path, changes=changes)

    for key in new_only_keys:
        sub_path = _join_path(path, key)
        changes.append(
            FieldChange(
                field_path=sub_path,
                change_type="added",
                old_value=None,
                new_value=new[key],
            )
        )


def _diff_list(
    old: list,
    new: list,
    *,
    path: str,
    changes: list[FieldChange],
) -> None:
    """Diff two lists at the same path.

    Positional comparison. Shared indices recurse; trailing indices
    on the longer side surface as added (NEW longer) or removed
    (OLD longer). Reordering without value change surfaces as
    multiple ``modified`` entries — Phase 3 spec acknowledges this
    as a known limitation; future Phase 3.1 may add ordered-set
    semantics if real use produces noise.
    """
    shared = min(len(old), len(new))

    for i in range(shared):
        sub_path = f"{path}[{i}]"
        _diff(old[i], new[i], path=sub_path, changes=changes)

    if len(new) > shared:
        for i in range(shared, len(new)):
            changes.append(
                FieldChange(
                    field_path=f"{path}[{i}]",
                    change_type="added",
                    old_value=None,
                    new_value=new[i],
                )
            )
    elif len(old) > shared:
        for i in range(shared, len(old)):
            changes.append(
                FieldChange(
                    field_path=f"{path}[{i}]",
                    change_type="removed",
                    old_value=old[i],
                    new_value=None,
                )
            )


def _join_path(parent: str, key: str) -> str:
    """Compose a parent path + dict key into a dotted child path.

    Empty parent (root level) returns the key bare. Non-empty
    parent inserts the dot separator. Bracket notation for list
    indices is composed inline at the call site
    (``f"{path}[{i}]"``) because list indices have no separator.
    """
    if not parent:
        return key
    return f"{parent}{_PATH_DOT}{key}"
