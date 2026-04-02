# CommandGraph Quickstart

Get from zero to running in 5 minutes.

## 1. Install

CommandGraph is a single Python file with zero dependencies. Python 3.9+ required.

```bash
# Copy the engine
cp cgr.py /usr/local/bin/cgr
chmod +x /usr/local/bin/cgr

# Or just use it directly
alias cgr="python3 /path/to/cgr.py"
```

## 2. Hello World

Create `hello.cgr`:

```
--- Hello CommandGraph ---

target "local" local:

  [say hello]:
    run $ echo "Hello from CommandGraph!"
```

Run it:

```bash
cgr apply hello.cgr
```

You'll see `✓ ok  say_hello` — your first graph executed.

## 3. Add a dependency

Create `setup.cgr`:

```
--- My first dependency ---

target "local" local:

  [create directory]:
    skip if $ test -d /tmp/myapp
    run $ mkdir -p /tmp/myapp

  [write config]:
    first [create directory]
    skip if $ test -f /tmp/myapp/config.txt
    run $ echo "port=8080" > /tmp/myapp/config.txt
```

Key concepts:
- `first [create directory]` means "run create directory before this step"
- `skip if` makes steps idempotent — if the check passes, the step is skipped

```bash
cgr plan setup.cgr    # See the execution order
cgr apply setup.cgr   # Run it
cgr apply setup.cgr   # Run again — both steps skip (already done)
```

## 4. Use variables

```
--- Variables ---
set app_port = "3000"
set app_dir = "/tmp/myapp"

target "local" local:

  [create app dir]:
    skip if $ test -d ${app_dir}
    run $ mkdir -p ${app_dir}

  [write config]:
    first [create app dir]
    run $ echo "port=${app_port}" > ${app_dir}/config.txt
```

Override from the command line:

```bash
cgr apply setup.cgr --set app_port=9090
```

## 5. Launch the Web IDE

```bash
cgr serve setup.cgr
```

This opens a browser with a three-panel IDE: editor, live DAG visualization, and execution log. Edit your graph, see the dependency tree update in real time, and click Apply to execute.

For remote/headless environments:

```bash
cgr serve setup.cgr --host 0.0.0.0 --no-open
```

## What's next?

| Goal | Read |
|------|------|
| Guided walkthrough (9 lessons, ~1 hour) | [TUTORIAL.md](TUTORIAL.md) |
| Real-world recipes and patterns | [COOKBOOK.md](COOKBOOK.md) |
| Complete syntax reference | [MANUAL.md](MANUAL.md) |
| Formal language spec (for code generators) | [COMMANDGRAPH_SPEC.md](COMMANDGRAPH_SPEC.md) |
