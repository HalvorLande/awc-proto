from __future__ import annotations
import os
from pathlib import Path
from typing import List, Union
import re

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
global data_files_directory
global config_files_directory
data_files_directory = os.path.join(root_dir, "data", "files")
config_files_directory = os.path.join(root_dir, "config")

def get_file_path(file_name):
    return os.path.join(data_files_directory, file_name)

def _sanitize_file_name(file_name: str) -> str:
    """Return a version of ``file_name`` that is safe to use on all OSes."""

    # Replace characters that are invalid on Windows file systems.
    invalid_chars = r'[<>:"/\\|?*]'
    return re.sub(invalid_chars, "_", file_name)


def export_to_file(text, file_name):
#    print("root_dir: ", root_dir)
#    print("data_files_directory: ", data_files_directory)
    safe_file_name = _sanitize_file_name(str(file_name))
    os.makedirs(data_files_directory, exist_ok=True)
    file_path = os.path.join(data_files_directory, safe_file_name)
#    print("file_path: ", file_path)
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(text)

def read_config_file(file_name):
    try:
        # Check if the file exists
        file_path = os.path.join(config_files_directory, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file '{file_path}' does not exist.")

        # Open and read the file
        with open(file_path, "r") as file:
            file_content = file.read().strip() # Remove any trailing newline or spaces
            return file_content

    except Exception as e:
        print(f"Error reading file '{file_path}': {e}")
        return None

def read_file(file_name):
    try:
        # Check if the file exists
        file_path = os.path.join(data_files_directory, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file '{file_path}' does not exist.")

        # Try reading with UTF-8 first
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                return file.read().strip()  # Remove trailing newlines/spaces
        except UnicodeDecodeError:
            # Fallback to system default encoding
            with open(file_path, "r", encoding="latin-1") as file:
                return file.read().strip()

    except Exception as e:
        print(f"Error reading file '{file_path}': {e}")
        return None

def read_file_old(file_name):
    try:
        # Check if the file exists
        file_path = os.path.join(data_files_directory, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file '{file_path}' does not exist.")

        # Open and read the file
        with open(file_path, "r") as file:
            file_content = file.read().strip() # Remove any trailing newline or spaces
            return file_content

    except Exception as e:
        print(f"Error reading file '{file_path}': {e}")
        return None

"""Utility functions for working with generated prompt markdown files.

This module currently offers a single public helper:

- ``create_new_prompts`` — walk through ``data/files/new_files`` and, for every
  *generated_prompt 520 - YYYY-MM-DD.md* file, produce a matching
  *generated_prompt 526 - YYYY-MM-DD.md* file consisting of the shared
  ``header.md`` followed by the relevant slice of the source document.
"""


__all__ = [
    "create_new_prompts",
]


def create_new_prompts(
    source_dir: Union[str, Path] = Path("data/files/new_prompts"),
    *,
    old_prefix: str = "generated_prompt 520",
    new_prefix: str = "generated_prompt 526",
    start_marker: str = "It is now",
    end_marker: str = "## Your total assets are 100 million USD",
    encoding: str = "utf-8",
    verbose: bool = True,
) -> List[Path]:
    """Generate *526* prompt files from the existing *520* ones.

    The function performs the following steps:

    1. **Locate the working directory** (``source_dir``).
    2. **Read** *header.md* once.
    3. **Iterate** over Markdown files whose filename matches
       ``f"{old_prefix} - YYYY-MM-DD.md"``.
    4. For each source file:
       - Extract the section starting at ``start_marker`` **inclusive** and
         ending right **before** ``end_marker`` (exclusive).  If
         ``end_marker`` is not found, the slice continues to EOF.
       - Prepend the header to this slice.
       - Write the result to a new file named
         ``f"{new_prefix} - YYYY-MM-DD.md"`` in the same directory.

    Parameters
    ----------
    source_dir
        Directory that contains the *generated_prompt 520* files and
        ``header.md``.
    old_prefix, new_prefix
        Filename prefixes for source and target prompt files.
    start_marker, end_marker
        Delimiters used to isolate the desired section of each source file.
        ``start_marker`` is **inclusive** and ``end_marker`` is **exclusive**.
    encoding
        Text encoding used for *all* file reads/writes.
    verbose
        If *True*, prints a short progress line for every processed file.

    Returns
    -------
    list[Path]
        A list of :class:`pathlib.Path` objects pointing to the newly created
        files.
    """
    src_path = Path(source_dir).expanduser().resolve()
    if not src_path.is_dir():
        raise FileNotFoundError(f"Source directory {src_path!s} does not exist")

    header_path = src_path / "header.md"
    if not header_path.is_file():
        raise FileNotFoundError(
            f"Expected header file 'header.md' inside {src_path!s}, but it was not found."
        )

    header_content = header_path.read_text(encoding=encoding).rstrip()

    # Regex that captures the date component (YYYY-MM-DD) after the prefix and hyphen.
    date_re = re.compile(
        rf"^{re.escape(old_prefix)} - (\d{{4}}-\d{{2}}-\d{{2}})\.md$"
    )

    created_files: List[Path] = []

    for file_path in src_path.iterdir():
        if not file_path.is_file() or file_path.name == "header.md":
            continue

        match = date_re.match(file_path.name)
        if match is None:
            # Ignore unrelated files.
            continue

        date_str = match.group(1)
        if verbose:
            print(f"Processing {file_path.name} …", end=" ")

        text = file_path.read_text(encoding=encoding)

        try:
            start_idx = text.index(start_marker)
        except ValueError:
            if verbose:
                print("start marker not found — skipped.")
            continue

        try:
            end_idx = text.index(end_marker, start_idx)
        except ValueError:
            end_idx = len(text)  # End marker missing: take the rest of the file.

        extracted = text[start_idx:end_idx].rstrip()

        new_filename = f"{new_prefix} - {date_str}.md"
        new_path = src_path / new_filename

        # Assemble output: header + blank line + extracted slice + trailing newline.
        new_path.write_text(f"{header_content}\n\n{extracted}\n", encoding=encoding)
        created_files.append(new_path)

        if verbose:
            print(f"→ created {new_filename}")

    return created_files


if __name__ == "__main__":  # pragma: no cover
    # Allow ad‑hoc execution of the script for manual testing.
    create_new_prompts(verbose=True)
