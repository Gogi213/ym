import pathlib


ROOT = pathlib.Path(__file__).resolve().parents[1]
SOURCE_DIR = ROOT / "appsscript-src"
OUTPUT_PATH = ROOT / "Code.js"


def build_bundle() -> str:
    parts = []
    for path in sorted(SOURCE_DIR.glob("*.js")):
        parts.append(path.read_text(encoding="utf-8-sig").rstrip())
    return "\n\n".join(parts) + "\n"


def main() -> None:
    OUTPUT_PATH.write_text(build_bundle(), encoding="utf-8")


if __name__ == "__main__":
    main()
