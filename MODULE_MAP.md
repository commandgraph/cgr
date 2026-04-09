# Module Map - Where To Look

| I want to... | Read this module | Also need context from |
|---|---|---|
| Change `.cg` brace syntax | `cgr_src/parser_cg.py` | `cgr_src/lexer.py`, `cgr_src/ast_nodes.py` |
| Change `.cgr` indent syntax | `cgr_src/parser_cgr.py` | `cgr_src/ast_nodes.py` |
| Add a new AST node type | `cgr_src/ast_nodes.py` | both parsers |
| Change dependency resolution | `cgr_src/resolver.py` | `cgr_src/ast_nodes.py` |
| Change how commands execute | `cgr_src/executor.py` | `cgr_src/resolver.py` |
| Change state/resume behavior | `cgr_src/state.py` | `cgr_src/executor.py`, `cgr_src/resolver.py` |
| Add or change a CLI command | `cgr_src/commands.py`, `cgr_src/cli.py` | `cgr_src/resolver.py` |
| Change DOT output | `cgr_src/dot.py` | `cgr_src/resolver.py` |
| Change the HTML visualization | `cgr_src/visualize.py` | `cgr_src/resolver.py`, `cgr_src/state.py` |
| Change the IDE or `cgr serve` | `cgr_src/serve.py` | `cgr_src/visualize.py`, `cgr_src/executor.py` |
| Change template repo loading | `cgr_src/repo.py` | `cgr_src/ast_nodes.py`, both parsers |
| Change shared utilities or output helpers | `cgr_src/common.py` | depends on the caller |
