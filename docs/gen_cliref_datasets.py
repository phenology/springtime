# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0
"""Generate the code reference pages and navigation."""

from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()

for path in sorted(Path("src/springtime/recipes/datasets").rglob("*.yaml")):
    module_path = path.relative_to("src/springtime/recipes/datasets").with_suffix("")
    doc_path = path.relative_to("src/springtime/recipes/datasets").with_suffix(".md")
    full_doc_path = Path("clireference", doc_path)

    parts = tuple(module_path.parts)
    nav[parts] = doc_path.as_posix()

    with open(path, 'r') as input_file:
        recipe = input_file.readlines()

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        fd.write("```yaml\n")
        fd.writelines(recipe)
        fd.write("\n```\n")

    mkdocs_gen_files.set_edit_path(full_doc_path, path)

with mkdocs_gen_files.open("clireference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
