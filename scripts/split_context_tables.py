#!/usr/bin/env python3
"""
split_context_tables.py — Split domain YAML context files into per-table files.

Converts:
  context/sources/prodview/tanks.yaml (12 tables, 311 lines)
Into:
  context/sources/prodview/domains/tanks.yaml    (domain header + table list + relationships)
  context/sources/prodview/tables/pvUnitTank.yaml
  context/sources/prodview/tables/pvUnitTankEntry.yaml
  context/sources/prodview/tables/pvUnitTankStrap.yaml
  ...

The original domain files are preserved as domain-level context (relationships,
workflow descriptions, header comments). Per-table files contain only the column
definitions for that specific table.

Usage:
    # Dry run (show what would be created)
    python scripts/split_context_tables.py --dry-run

    # Split all sources
    python scripts/split_context_tables.py

    # Split specific source
    python scripts/split_context_tables.py --source prodview

    # Split specific domain file
    python scripts/split_context_tables.py --file context/sources/prodview/tanks.yaml
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONTEXT_DIR = PROJECT_ROOT / "context" / "sources"


@dataclass
class TableEntry:
    """A single table parsed from a domain YAML file."""

    name: str
    header_line: str  # The full "pvTableName: Description +tags" line
    columns: list[str] = field(default_factory=list)
    domain: str = ""
    source: str = ""

    @property
    def filename(self) -> str:
        return f"{self.name}.yaml"

    def to_yaml(self) -> str:
        """Render as a standalone per-table YAML file."""
        lines = [
            f"# {self.source} / {self.domain} / {self.name}",
            f"# Source: context/sources/{self.source}/domains/{self.domain}.yaml",
            "#",
            self.header_line,
        ]
        lines.extend(self.columns)
        # Ensure trailing newline
        return "\n".join(lines).rstrip() + "\n"


@dataclass
class DomainFile:
    """A parsed domain YAML file."""

    path: Path
    source: str
    domain: str
    header_lines: list[str]  # Comments and metadata before first table
    tables: list[TableEntry] = field(default_factory=list)

    def to_domain_yaml(self) -> str:
        """Render as a domain-level file (header + table list, no columns)."""
        lines = list(self.header_lines)

        # Add table listing
        if self.tables:
            lines.append("")
            lines.append("# Tables in this domain (column details in tables/ directory):")
            for t in self.tables:
                # Extract the description part from the header line
                desc = t.header_line.split(":", 1)[1].strip() if ":" in t.header_line else ""
                # Truncate long descriptions
                if len(desc) > 80:
                    desc = desc[:77] + "..."
                lines.append(f"#   {t.name}: {desc}")

        return "\n".join(lines).rstrip() + "\n"


def parse_domain_file(filepath: Path, source: str) -> DomainFile:
    """Parse a domain YAML file into header + table entries."""
    content = filepath.read_text(encoding="utf-8")
    lines = content.split("\n")
    domain = filepath.stem  # e.g., "tanks" from "tanks.yaml"

    header_lines = []
    tables: list[TableEntry] = []
    current_table: TableEntry | None = None

    # Pattern for table header lines: starts with pv* or wv* (not indented)
    table_pattern = re.compile(r"^(pv\w+|wv\w+|WV\w+):\s*(.*)$")

    for line in lines:
        match = table_pattern.match(line)
        if match:
            # Save previous table
            if current_table:
                tables.append(current_table)

            table_name = match.group(1)
            current_table = TableEntry(
                name=table_name,
                header_line=line,
                domain=domain,
                source=source,
            )
        elif current_table is not None:
            # Column line (indented) or blank line within a table
            if line.startswith("  ") or line == "":
                current_table.columns.append(line)
            elif line.startswith("#"):
                # Comment between tables — could be section separator
                # If we have a current table and hit a comment, it might be
                # a separator before the next table group
                current_table.columns.append(line)
            else:
                # Non-indented, non-table, non-comment line
                current_table.columns.append(line)
        else:
            # Before any table — this is header
            header_lines.append(line)

    # Don't forget the last table
    if current_table:
        tables.append(current_table)

    # Clean up trailing blank lines from column lists
    for t in tables:
        while t.columns and t.columns[-1].strip() == "":
            t.columns.pop()

    return DomainFile(
        path=filepath,
        source=source,
        domain=domain,
        header_lines=header_lines,
        tables=tables,
    )


def find_domain_files(source: str | None = None) -> list[tuple[str, Path]]:
    """Find all domain YAML files to process."""
    results = []
    for source_dir in sorted(CONTEXT_DIR.iterdir()):
        if not source_dir.is_dir():
            continue
        if source and source_dir.name != source:
            continue
        for yaml_file in sorted(source_dir.glob("*.yaml")):
            if yaml_file.name.startswith("_"):
                continue  # Skip _index.yaml
            results.append((source_dir.name, yaml_file))
    return results


def split_domain_file(
    domain_file: DomainFile,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Split a domain file into per-table files.

    Returns (tables_created, bytes_written).
    """
    source_dir = CONTEXT_DIR / domain_file.source
    tables_dir = source_dir / "tables"
    domains_dir = source_dir / "domains"

    tables_created = 0
    bytes_written = 0

    if not dry_run:
        tables_dir.mkdir(exist_ok=True)
        domains_dir.mkdir(exist_ok=True)

    # Write per-table files
    for table in domain_file.tables:
        table_path = tables_dir / table.filename
        content = table.to_yaml()

        if dry_run:
            print(f"  CREATE {table_path.relative_to(PROJECT_ROOT)} ({len(content)} bytes)")
        else:
            table_path.write_text(content, encoding="utf-8")

        tables_created += 1
        bytes_written += len(content)

    # Write domain-level file (header + table listing, no columns)
    domain_path = domains_dir / f"{domain_file.domain}.yaml"
    domain_content = domain_file.to_domain_yaml()

    if dry_run:
        print(f"  CREATE {domain_path.relative_to(PROJECT_ROOT)} ({len(domain_content)} bytes)")
    else:
        domain_path.write_text(domain_content, encoding="utf-8")

    bytes_written += len(domain_content)

    return tables_created, bytes_written


def update_index(source: str, domain_files: list[DomainFile], dry_run: bool = False) -> None:
    """Update or create _index.yaml with the new structure reference."""
    index_path = CONTEXT_DIR / source / "_index.yaml"

    lines = [
        f"# {source} context index",
        f"# Structure: domains/ (relationships) + tables/ (column definitions)",
        "#",
        "# Domain files (load for cross-table context):",
    ]
    for df in domain_files:
        lines.append(f"#   domains/{df.domain}.yaml — {len(df.tables)} tables")

    lines.append("#")
    lines.append("# Per-table files (load for single-model work):")
    lines.append(f"#   tables/{{table_name}}.yaml")
    lines.append("#")

    total_tables = sum(len(df.tables) for df in domain_files)
    lines.append(f"# Total: {len(domain_files)} domains, {total_tables} tables")

    content = "\n".join(lines) + "\n"

    if dry_run:
        print(f"  UPDATE {index_path.relative_to(PROJECT_ROOT)}")
    else:
        index_path.write_text(content, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Split domain YAML context files into per-table files."
    )
    parser.add_argument(
        "--source",
        help="Only process this source (e.g., 'prodview', 'wellview')",
    )
    parser.add_argument(
        "--file",
        help="Only process this specific domain file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be created without writing files",
    )
    args = parser.parse_args()

    if args.file:
        filepath = Path(args.file)
        if not filepath.is_absolute():
            filepath = PROJECT_ROOT / filepath
        source = filepath.parent.name
        domain_files_to_process = [(source, filepath)]
    else:
        domain_files_to_process = find_domain_files(args.source)

    if not domain_files_to_process:
        print("No domain YAML files found to process.", file=sys.stderr)
        return 1

    total_tables = 0
    total_bytes = 0

    # Group by source for index updates
    by_source: dict[str, list[DomainFile]] = {}

    for source, filepath in domain_files_to_process:
        print(f"\n{'[DRY RUN] ' if args.dry_run else ''}Processing: {filepath.relative_to(PROJECT_ROOT)}")

        df = parse_domain_file(filepath, source)
        print(f"  Found {len(df.tables)} tables in {df.domain} domain")

        tables, bytes_written = split_domain_file(df, dry_run=args.dry_run)
        total_tables += tables
        total_bytes += bytes_written

        if source not in by_source:
            by_source[source] = []
        by_source[source].append(df)

    # Update index files
    for source, dfs in by_source.items():
        print(f"\n{'[DRY RUN] ' if args.dry_run else ''}Updating index: context/sources/{source}/_index.yaml")
        update_index(source, dfs, dry_run=args.dry_run)

    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}Done: {total_tables} table files, {total_bytes:,} bytes")

    if not args.dry_run:
        print("\nOriginal domain files preserved (not deleted).")
        print("New structure:")
        print("  context/sources/{source}/domains/  — domain relationships")
        print("  context/sources/{source}/tables/    — per-table column definitions")
        print("\nTo verify: find context/sources/*/tables/ -name '*.yaml' | wc -l")

    return 0


if __name__ == "__main__":
    sys.exit(main())
