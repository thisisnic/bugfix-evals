## Example Scripts

### Running a basic eval

How to run a basic eval (e.g. `01_inspect_hello_world.py`)

1. Set up venv and install inspect_ai and anthropic

```
uv venv
source .venv/bin/activate
uv pip install inspect_ai
uv pip install anthropic
```

2.  Run eval, sppecifying the model. You should have `ANTHROPIC_API_KEY` env var set up already.

```
inspect eval 01_inspect_hello_world.py --model anthropic/claude-sonnet-4-20250514
```

3. View results in Inspect viewer

```
inspect view
```

### Running a basic agentic eval

1. Install inspect_swe 

```
source .venv/bin/activate
uv pip install inspect_swe
```

2. Run example eval

```
inspect eval example_scripts/02_agentic_eval.py --model anthropic/claude-sonnet-4-20250514
```

3. View results in Inspect viewer

```
inspect view
```