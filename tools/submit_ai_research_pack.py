from __future__ import annotations

import argparse
import json
import os
import zipfile
from pathlib import Path
from typing import Any


def _read_prompt(zip_path: Path, prompt_file: str) -> str:
    with zipfile.ZipFile(zip_path, "r") as zf:
        try:
            return zf.read(prompt_file).decode("utf-8", errors="replace")
        except KeyError:
            return (
                "Analyze the uploaded AI research pack. Start with README.md, "
                "PROMPT_FOR_CHATGPT.md, manifest.json, data/*.csv, and notes/*.md."
            )


def _json_default(value: Any) -> Any:
    try:
        return value.model_dump()
    except Exception:
        pass
    try:
        return value.__dict__
    except Exception:
        return str(value)


def submit(args: argparse.Namespace) -> int:
    zip_path = Path(args.zip).expanduser().resolve()
    if not zip_path.exists():
        raise FileNotFoundError(zip_path)
    if not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is not set.")

    try:
        from openai import OpenAI
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "The OpenAI Python package is missing. Install it with: "
            ".\\.venv\\Scripts\\python.exe -m pip install openai"
        ) from exc

    output_dir = Path(args.output_dir) if args.output_dir else zip_path.with_suffix("")
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    prompt = _read_prompt(zip_path, str(args.prompt_file))
    extra = str(args.extra_prompt or "").strip()
    if extra:
        prompt = prompt.rstrip() + "\n\nAdditional user instruction:\n" + extra + "\n"

    client = OpenAI()
    with zip_path.open("rb") as handle:
        uploaded = client.files.create(file=handle, purpose="assistants")

    response = client.responses.create(
        model=str(args.model),
        tools=[
            {
                "type": "code_interpreter",
                "container": {
                    "type": "auto",
                    "memory_limit": str(args.memory_limit),
                    "file_ids": [uploaded.id],
                },
            }
        ],
        input=prompt,
    )

    raw_path = output_dir / "openai_response_raw.json"
    text_path = output_dir / "openai_response_text.md"
    raw_path.write_text(json.dumps(response, default=_json_default, ensure_ascii=False, indent=2), encoding="utf-8")
    text = getattr(response, "output_text", None) or ""
    text_path.write_text(str(text), encoding="utf-8")

    print(f"uploaded_file_id={uploaded.id}")
    print(f"raw_response={raw_path}")
    print(f"output_text={text_path}")
    print(str(text)[:4000])
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Submit AI research pack ZIP to OpenAI Code Interpreter.")
    parser.add_argument("--zip", required=True, help="Path to ai research pack ZIP.")
    parser.add_argument("--model", default=str(os.environ.get("OPENAI_MODEL", "gpt-5.5") or "gpt-5.5"))
    parser.add_argument(
        "--memory-limit",
        default=str(os.environ.get("OPENAI_MEMORY_LIMIT", "4g") or "4g"),
        choices=["1g", "4g", "16g", "64g"],
    )
    parser.add_argument("--prompt-file", default="PROMPT_FOR_CHATGPT.md")
    parser.add_argument("--extra-prompt", default="")
    parser.add_argument("--output-dir", default="")
    return submit(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
