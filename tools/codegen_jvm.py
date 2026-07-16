#!/usr/bin/env python3
"""Unified MEOS-surface code generator for the MobilityDB JVM bindings.

ONE generator, three engines, selected by ``--engine {spark|flink|kafka}``. Every
JVM binding (MobilitySpark, MobilityFlink, MobilityKafka) vendors this identical
file plus its ``codegen_spark_udfs.py`` sibling, so the generated surface can never
drift between engines — the North Star that MEOS is the single source of truth and
all bindings are GENERATED from it.

  * ``spark``        -> the MobilitySpark SQL-UDF surface. This path delegates
                        VERBATIM to the sibling ``codegen_spark_udfs.py``: the same
                        code runs, so the emitted files are byte-identical to what
                        that generator produces on its own.
  * ``flink|kafka``  -> the thin ``MeosOps*`` Java static-forwarder facades.
                        Covers the FULL jar surface (every
                        functions.GeneratedFunctions symbol), grouped by the
                        MEOS-API catalog object model: one class per object-model
                        class plus one per source header for the free functions,
                        plus the shared MeosOpsRuntime. flink and kafka differ ONLY
                        by the ``-Dmobility<engine>.meos.enabled`` toggle string.

Shared front-end (facade back-end only): load the catalog, list the jar symbols,
and derive each function's object-model class / role / header directly from the
catalog's ``objectModel``. The spark back-end owns its own catalog+jar front-end
(it needs jar arities the facade parse does not), so nothing about it changes.

Usage:
  codegen_jvm.py --engine spark --catalog meos-idl.json --jar JMEOS.jar --out DIR
  codegen_jvm.py --engine flink --catalog meos-idl.json --jar JMEOS.jar --out DIR
  codegen_jvm.py --engine kafka --catalog meos-idl.json --jar JMEOS.jar --out DIR
"""
import argparse
import importlib.util
import json
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


# ───────────────────────── shared front-end ─────────────────────────

def load_catalog(path):
    with open(path) as f:
        return json.load(f)


SIG_RE = re.compile(
    r'^\s*public\s+static\s+(?P<ret>[\w\.<>\[\]]+)\s+(?P<name>\w+)\((?P<args>[^)]*)\)')


def parse_jmeos_signatures(jar):
    """javap functions.GeneratedFunctions -> {name: {ret, arg_types}} (jar SoT).

    The jar is the ground truth of what the bundled JMEOS actually exposes, so the
    facade surface is exactly the jar surface."""
    out = subprocess.run(
        ['javap', '-cp', str(jar), 'functions.GeneratedFunctions'],
        check=True, capture_output=True, text=True).stdout
    jmeos = {}
    for line in out.splitlines():
        m = SIG_RE.match(line.rstrip(';'))
        if m:
            raw = m.group('args').strip()
            jmeos[m.group('name')] = {
                'ret': m.group('ret'),
                'arg_types': [a.strip() for a in raw.split(',')] if raw else [],
            }
    return jmeos


_SEQ_RE = re.compile(r'\bTSequence\b')


def seq_typed(canonical):
    """Sequence-typed return: materializes a whole TSequence / *SeqSet, so the
    function is inherently non-streamable (drives the sequence-only guard). Purely
    catalog-derived from returnType.canonical — no name heuristics."""
    s = canonical or ''
    return bool(_SEQ_RE.search(s)) or 'SeqSet' in s


def object_model_index(cat):
    """Catalog-derived class / role / header / sequence index for the facades.

    Returns (fn_class, fn_role, fn_header, fn_seq):
      fn_class[name]  -> the object-model class name (e.g. 'TGeo', 'FloatSpan')
                         or None if the function is a free/plumbing function.
      fn_role[name]   -> the object-model role (constructor/accessor/... ) or None.
      fn_header[name] -> the source header the catalog attributes the function to.
      fn_seq[name]    -> True iff its return type is sequence-typed.

    class/role come from objectModel.classes[*].methods[*] (each function appears
    under exactly ONE class, agreeing with objectModel.functionToClass). The header
    is the per-function 'file' field. The spark back-end does not use this — it
    groups by doxygen @ingroup."""
    om = cat['objectModel']
    fn_class, fn_role, fn_header, fn_seq = {}, {}, {}, {}
    for cls_name, cls in om['classes'].items():
        for m in cls.get('methods', []):
            fn = m['function']
            fn_class.setdefault(fn, cls_name)
            fn_role.setdefault(fn, m.get('role'))
    for f in cat['functions']:
        n = f['name']
        fn_header[n] = f['file']
        fn_seq[n] = seq_typed(f['returnType'].get('canonical',
                                                   f['returnType'].get('c', '')))
    return fn_class, fn_role, fn_header, fn_seq


# ───────────────────────── spark back-end ─────────────────────────

def run_spark(args):
    """Delegate to the sibling codegen_spark_udfs.py so output is byte-identical.

    The reference generator owns the whole catalog+jar front-end and the SQL-UDF
    emit; running its own code (rather than a re-implementation) is what makes the
    output provably identical to today's. Every binding vendors codegen_spark_udfs.py
    next to this file, so the import target is always the sibling."""
    spark_path = Path(__file__).resolve().parent / 'codegen_spark_udfs.py'
    spec = importlib.util.spec_from_file_location('codegen_spark_udfs', spark_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    argv = ['codegen_spark_udfs',
            '--catalog', args.catalog,
            '--out', args.out,
            '--jar', args.jar]
    if args.report:
        argv.append('--report')
    saved = sys.argv
    try:
        sys.argv = argv
        mod.main()
    finally:
        sys.argv = saved


# ───────────────────────── facade back-end ─────────────────────────

def short_type(t):
    if t.startswith('java.lang.'):
        return t[len('java.lang.'):]
    return t.split('.')[-1] if '.' in t else t


def header_to_class(h):
    """Free-function class name from the source header, split on '_' AND '.' so a
    dotted internal header (e.g. postgres_ext_defs.in.h) yields a valid class."""
    base = h.replace('.h', '').replace('meos_', '').replace('meos', 'core')
    if base in ('', 'core'):
        return 'MeosOpsFreeCore'
    return 'MeosOpsFree' + ''.join(p.capitalize()
                                   for p in re.split(r'[_.]', base) if p)


def collect_imports(rows):
    imports = {'functions.GeneratedFunctions'}
    for r in rows:
        for t in [r['java_ret']] + [a[0] for a in r['java_params']]:
            if '.' in t and not t.startswith('java.lang.'):
                imports.add(t.replace('[]', ''))
    return sorted(i for i in imports if '.' in i)


def emit_method(r, prop):
    fname = r['name']
    args = ', '.join(f'{short_type(t)} {n}' for (t, n) in r['java_params'])
    call_args = ', '.join(n for (t, n) in r['java_params'])
    ret = short_type(r['java_ret'])
    L = ['    /**',
         f'     * MEOS {{@code {fname}}}.']
    if r.get('oo_class'):
        L.append(f'     * <p>Object-model class: {{@code {r["oo_class"]}}}'
                 + (f', role {{@code {r["role"]}}}' if r.get('role') else '')
                 + '.</p>')
    elif r.get('role'):
        L.append(f'     * <p>Object-model role: {{@code {r["role"]}}}.</p>')
    if r['seq']:
        L.append('     * <p>Sequence-only: builds a whole sequence — '
                 'not supported in a streaming context.</p>')
    L.append('     */')
    if r['seq']:
        # sequence-only guard
        L += [f'    public static {ret} {fname}({args}) {{',
              '        throw new UnsupportedOperationException(',
              f'            "{fname} is sequence-only — not supported in a '
              'streaming context");',
              '    }', '']
    else:
        # runtime MEOS_AVAILABLE guard
        ret_stmt = '' if ret == 'void' else 'return '
        L += [f'    public static {ret} {fname}({args}) {{',
              '        if (!MEOS_AVAILABLE) {',
              '            throw new UnsupportedOperationException(',
              f'                "{fname} requires libmeos — set -D{prop}=true");',
              '        }',
              f'        {ret_stmt}GeneratedFunctions.{fname}({call_args});',
              '    }', '']
    return L


def emit_class(cls, rows, pkg, prop, banner):
    seq_cnt = sum(1 for r in rows if r['seq'])
    L = [f'package {pkg};', '',
         '/* AUTO-GENERATED by tools/codegen_jvm.py — do not edit by hand.',
         f' * {banner}',
         f' * Methods emitted: {len(rows)} (full jar surface'
         + (f'; {seq_cnt} sequence-only)' if seq_cnt else ')'),
         ' * Source: the bundled JMEOS functions.GeneratedFunctions surface,',
         ' *         grouped by the MEOS-API catalog object model.',
         ' */', '']
    L += [f'import {i};' for i in collect_imports(rows)]
    L += ['',
          f'public final class {cls} {{', '',
          '    public static final boolean MEOS_AVAILABLE = '
          'MeosOpsRuntime.MEOS_AVAILABLE;',
          '',
          f'    private {cls}() {{ /* utility */ }}', '']
    for r in sorted(rows, key=lambda r: r['name']):
        L += emit_method(r, prop)
    L.append('}')
    return '\n'.join(L) + '\n'


def runtime_src(pkg, prop):
    """Shared MEOS_AVAILABLE probe."""
    return f'''package {pkg};

import functions.GeneratedFunctions;

/* AUTO-GENERATED by tools/codegen_jvm.py — do not edit by hand.
 * Shared runtime helper: owns the single MEOS_AVAILABLE static-init across all
 * generated MeosOps* facades, so libmeos is probed exactly once per JVM. */
final class MeosOpsRuntime {{

    static final boolean MEOS_AVAILABLE;

    static {{
        boolean enabled = Boolean.parseBoolean(
                System.getProperty("{prop}", "true"));
        boolean ok = false;
        if (enabled) {{
            try {{
                GeneratedFunctions.meos_initialize();
                ok = true;
            }} catch (Throwable t) {{
                ok = false;
            }}
        }}
        MEOS_AVAILABLE = ok;
    }}

    private MeosOpsRuntime() {{ /* utility */ }}
}}
'''


def run_facades(args):
    prop = f'mobility{args.engine}.meos.enabled'
    out_dir = Path(args.out) / 'src/main/java' / args.package.replace('.', '/')
    out_dir.mkdir(parents=True, exist_ok=True)

    cat = load_catalog(args.catalog)
    jmeos = parse_jmeos_signatures(args.jar)
    fn_class, fn_role, fn_header, fn_seq = object_model_index(cat)

    # FULL surface: one row per jar symbol (the jar is the ground truth of what the
    # bundled JMEOS actually exposes). A jar symbol absent from the catalog has no
    # class/header, so it falls back to the core free class and the return-type
    # sequence check.
    rows = []
    for name, sig in jmeos.items():
        rows.append({
            'name': name,
            'java_ret': sig['ret'],
            'java_params': [(t, f'arg{i}') for i, t in enumerate(sig['arg_types'])],
            'oo_class': fn_class.get(name),
            'role': fn_role.get(name),
            'header': fn_header.get(name, 'meos.h'),
            'seq': fn_seq.get(name, seq_typed(sig['ret'])),
        })

    # Regenerate from scratch: drop stale MeosOps*.java, keep hand-written caller
    # logic in the same package (e.g. MeosSetSetJoin.java).
    for f in out_dir.glob('MeosOps*.java'):
        f.unlink()
    (out_dir / 'MeosOpsRuntime.java').write_text(runtime_src(args.package, prop))

    # Group by the FINAL class name, not the raw grouping key: several distinct
    # headers collapse to the same free class (e.g. cbuffer.h and meos_cbuffer.h
    # both -> MeosOpsFreeCbuffer), so key on the class name and MERGE their rows —
    # else the second write_text would clobber the first and silently drop methods.
    by_class = defaultdict(list)      # class name -> rows
    class_headers = defaultdict(set)  # free class name -> contributing headers
    for r in rows:
        if r['oo_class']:
            by_class[f'MeosOps{r["oo_class"]}'].append(r)
        else:
            cls = header_to_class(r['header'])
            by_class[cls].append(r)
            class_headers[cls].add(r['header'])

    for cls, crows in sorted(by_class.items()):
        if cls in class_headers:
            banner = 'Free functions from ' + ', '.join(sorted(class_headers[cls]))
        else:
            banner = 'MEOS object-model class: ' + cls[len('MeosOps'):]
        (out_dir / f'{cls}.java').write_text(
            emit_class(cls, crows, args.package, prop, banner))

    n_seq = sum(1 for r in rows if r['seq'])
    n_oo = sum(1 for c in by_class if c not in class_headers)
    n_free = len(class_headers)
    print(f'{args.engine}: emitted {1 + len(by_class)} facade classes into {out_dir} '
          f'({n_oo} object-model + {n_free} free + MeosOpsRuntime), '
          f'{len(rows)} methods ({n_seq} sequence-only guarded)')


# ───────────────────────── entry point ─────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--engine', required=True, choices=['spark', 'flink', 'kafka'])
    ap.add_argument('--catalog', required=True, help='MEOS-API meos-idl.json')
    ap.add_argument('--jar', required=True,
                    help='JMEOS jar with functions.GeneratedFunctions')
    ap.add_argument('--out', required=True, help='output directory')
    ap.add_argument('--package', default='org.mobilitydb.meos',
                    help='facade package (flink/kafka only)')
    ap.add_argument('--report', action='store_true', help='spark only')
    args = ap.parse_args()

    if args.engine == 'spark':
        run_spark(args)
    else:
        run_facades(args)


if __name__ == '__main__':
    main()
