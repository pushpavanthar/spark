"""Renders the production dbt macro rollup_fact_sql outside dbt: argv = macro file, rollup json, columns json, source, filter."""
import json
import sys

import jinja2

macro, config, columns, source, source_filter = sys.argv[1:6]
env = jinja2.Environment(extensions=["jinja2.ext.do"])
template = env.from_string(open(macro).read() + "{{ rollup_fact_sql(c, cols, src, flt) }}")
print(template.render(c=json.load(open(config)), cols=json.loads(columns), src=source, flt=source_filter))
