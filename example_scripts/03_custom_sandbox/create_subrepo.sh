#!/bin/bash
# Creates the test git repo used by this example.
set -e
mkdir -p subrepo && cd subrepo
git init && git checkout -b main
git config user.email "test@example.com"
git config user.name "Test"

cat > main.py << 'PYEOF'
def main():
    print("Hello, World!")

if __name__ == "__main__":
    main()
PYEOF
git add main.py && git commit -m "first"

sed -i 's/Hello, World!/This is the second message/' main.py
git add main.py && git commit -m "second"

sed -i 's/This is the second message/This is the third message/' main.py
git add main.py && git commit -m "third"

echo "Subrepo created. Update task.py with these hashes:"
git log --oneline
