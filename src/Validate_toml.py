import re
try:
    # Standard library in Python 3.11+
    import tomllib
except ImportError:
    # Fallback for older Python versions, install with: pip install tomli
    try:
        import tomli as tomllib
    except ImportError:
        print("❌ Error: 'tomli' is not installed. Please run 'pip install tomli' to proceed.")
        exit()

files = [
    'config.toml',
    'secrets.toml',
    'historical_collection_logs.toml',
    'historical_personal_bests.toml'
]

for file_path in files:
    print(f"Validating {file_path}...")
    try:
        with open(file_path, 'rb') as f:
            tomllib.load(f)
        print("✅ TOML file is valid!")

    except FileNotFoundError:
        print(f"❌ Error: File not found at '{file_path}'")

    except tomllib.TOMLDecodeError as e:
        print(f"❌ TOML error: {e}")

        # FIX: Parse line/col from the error message string for compatibility.
        match = re.search(r"\(at line (\d+), column (\d+)\)", str(e))
        
        if match:
            line, col = int(match.group(1)), int(match.group(2))
            print(f"   → Occurred at Line {line}, Column {col}")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    error_line = lines[line - 1].rstrip('\n')
                    print(f"   | {error_line}")
                    print(f"   | {' ' * (col - 1)}^")
            except (IOError, IndexError) as file_error:
                print(f"   (Could not display the specific line due to: {file_error})")
        else:
            print("   (Could not determine the exact error location from the message.)")

    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

    finally:
        print("-" * 40)

print("Validation complete!")