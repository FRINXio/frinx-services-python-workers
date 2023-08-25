import subprocess
import re

REV_PATTERN = r'rev\s*=\s*"([^"]+)"'
PYPROJECT_PATH = "pyproject.toml"


def _get_branch() -> str:
    try:
        output = subprocess.check_output(['git', 'branch', '--show-current'], stderr=subprocess.STDOUT, text=True)
        current_branch = output.strip()

        print("Current Git Branch:", current_branch)
        return current_branch
    except subprocess.CalledProcessError:
        print("Error: Not a Git repository or no current branch found.")
    except FileNotFoundError:
        print("Error: Git command not found. Please ensure Git is installed.")


def _replace_placeholder(current_branch: str) -> None:
    with open(PYPROJECT_PATH, 'r') as file:
        file_contents = file.read()

    updated_contents = re.sub(REV_PATTERN, f'rev = "{current_branch}"', file_contents)

    with open(PYPROJECT_PATH, 'w') as file:
        file.write(updated_contents)

    print(f"Frinx worker packages revision replaced with '{current_branch}' in '{PYPROJECT_PATH}'")


def main():
    _replace_placeholder(_get_branch())


if __name__ == '__main__':
    main()
