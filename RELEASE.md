# CommandGraph Release Process

This project uses a manually maintained engine version. The source of truth is
`__version__` in `cgr_src/common.py`; the shipped `cgr.py` artifact is generated
from that source.

Template versions declared inside files under `repo/` are separate. They describe
template compatibility and are not tied to the engine version.

## Version Policy

Use semantic versioning for the engine version:

| Change type | Increment | Examples |
|---|---:|---|
| Bug fix or internal-only fix | Patch | Fix resume behavior, installer bug, docs typo that does not change behavior |
| Backward-compatible feature | Minor | New CLI command, new `.cgr` syntax that preserves existing files, new output field |
| Breaking change | Major | Incompatible syntax change, state format change without migration, removed CLI behavior |

If a change spans multiple categories, use the highest applicable increment.

## Required Steps

1. Decide the next version from the policy above.
2. Edit `__version__` in `cgr_src/common.py`.
3. Rebuild the shipped artifact:

   ```bash
   python3 cgr_dev.py apply build.cgr --no-resume
   ```

4. Confirm both development and shipped entrypoints report the same version:

   ```bash
   python3 cgr_dev.py version
   python3 cgr.py version
   ```

5. Run the core verification suite:

   ```bash
   python3 -c "import py_compile; py_compile.compile('cgr.py', doraise=True)"
   python3 -m pytest test_commandgraph.py -x -q
   ```

6. Validate the root feature-exercise graphs when the change touches parsing,
   resolver behavior, execution, state, apply output, or templates:

   ```bash
   python3 cgr.py validate nginx_setup.cg
   python3 cgr.py validate nginx_setup.cgr
   python3 cgr.py validate webserver.cg --repo ./repo
   python3 cgr.py validate webserver.cgr --repo ./repo
   python3 cgr.py validate parallel_test.cgr
   python3 cgr.py validate multinode_test.cgr
   python3 cgr.py validate multinode_test.cg
   python3 cgr.py validate system_audit.cgr --repo ./repo
   python3 cgr.py validate api_integration.cgr
   ```

7. Commit the source changes and regenerated `cgr.py` together.
8. Tag releases as `vX.Y.Z` after the versioned commit is ready.

## Installer Behavior

`install.sh` does not fetch remote release metadata. When it says it will install
or upgrade to the latest CommandGraph version, it means the version reported by
this checkout's local `cgr.py`.
