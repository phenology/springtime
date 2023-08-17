# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0
"""Generate the code reference pages and navigation."""

from pathlib import Path
from textwrap import dedent

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()

for path in sorted(Path("src/springtime/recipes/datasets").rglob("*.yaml")):
    module_path = path.relative_to("src/springtime/recipes").with_suffix("")
    doc_path = path.relative_to("src/springtime/recipes").with_suffix(".md")
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


# Add header for cli ref
with mkdocs_gen_files.open("clireference/datasets/index.md", "w") as index_file:
    intro = [dedent("""
    Recipes to download a dataset. To prevent conflicts please comment out all
    datasets except one. Run by copying yaml text to file called `recipe.yaml`
    and run with `springtime recipe.yaml`
    """), ""]
    index_file.writelines(intro)

nav[("datasets",)] = "datasets/index.md"

# Add header files for subfolders
for level in ["insitu", "meteo", "satellite"]:
    name = f"datasets/{level}/index.md"
    nav[("datasets", level)] = name
    filename = "clireference/" + name
    with mkdocs_gen_files.open(filename, 'w') as f:
        f.writelines([f"# {level.capitalize()}", ""])

with mkdocs_gen_files.open("clireference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
