#!/usr/bin/env python3
"""Print per-module surefire totals, every skipped test with its reason, and a grand total.

Usage: seal-surefire-summary.py <repo-root>
"""
import glob
import os
import sys
import xml.etree.ElementTree as ET

root = sys.argv[1] if len(sys.argv) > 1 else "."
reports = sorted(glob.glob(os.path.join(root, "**", "target", "surefire-reports", "TEST-*.xml"), recursive=True))
modules = {}
skipped = []
for path in reports:
    module = os.path.relpath(path, root).split(os.sep + "target" + os.sep)[0]
    tree = ET.parse(path).getroot()
    m = modules.setdefault(module, [0, 0, 0, 0])
    m[0] += int(tree.get("tests", 0))
    m[1] += int(tree.get("failures", 0))
    m[2] += int(tree.get("errors", 0))
    m[3] += int(tree.get("skipped", 0))
    for case in tree.iter("testcase"):
        sk = case.find("skipped")
        if sk is not None:
            reason = sk.get("message") or (sk.text or "").strip() or "@Ignore without message (upstream annotation)"
            skipped.append("%s#%s: %s" % (case.get("classname"), case.get("name"), reason))

tot = [0, 0, 0, 0]
for module, (t, f, e, s) in sorted(modules.items()):
    print("MODULE %-55s tests=%d passed=%d failures=%d errors=%d skipped=%d" % (module, t, t - f - e - s, f, e, s))
    tot = [tot[0] + t, tot[1] + f, tot[2] + e, tot[3] + s]
for line in skipped:
    print("SKIPPED " + line)
print("SUMMARY tests=%d passed=%d failures=%d errors=%d skipped=%d (from %d surefire reports)"
      % (tot[0], tot[0] - tot[1] - tot[2] - tot[3], tot[1], tot[2], tot[3], len(reports)))
if not reports:
    print("ERROR: no surefire reports found")
    sys.exit(1)
if tot[1] or tot[2]:
    sys.exit(1)
