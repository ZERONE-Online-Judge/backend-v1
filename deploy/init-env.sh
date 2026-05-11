#!/bin/sh
set -eu

base_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
env_dir="$base_dir/env"

for name in backend db minio; do
  target="$env_dir/$name.env"
  example="$env_dir/$name.env.example"
  if [ ! -f "$target" ]; then
    cp "$example" "$target"
    echo "created $target"
  else
    echo "exists  $target"
  fi
done

echo "edit backend/deploy/env/*.env before production use"
