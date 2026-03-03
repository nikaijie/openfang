#!/usr/bin/env bash
set -euo pipefail

KEY_ARG="${1:-}"
KEY_VALUE="${KEY_ARG:-${GEMINI_API_KEY:-}}"

if [[ -z "${KEY_VALUE}" ]]; then
  echo "Error: missing Gemini API key." >&2
  echo "Usage: bash desk/scripts/setup_gemini_persistent_env.sh '<YOUR_GEMINI_API_KEY>'" >&2
  echo "Or export GEMINI_API_KEY first, then run this script." >&2
  exit 1
fi

SECRETS_DIR="${HOME}/.openfang/secrets"
KEY_FILE="${SECRETS_DIR}/gemini_api_key"
LOADER_FILE="${SECRETS_DIR}/load_gemini_env.sh"
ZSHRC_FILE="${HOME}/.zshrc"

mkdir -p "${SECRETS_DIR}"
chmod 700 "${SECRETS_DIR}"

printf '%s\n' "${KEY_VALUE}" > "${KEY_FILE}"
chmod 600 "${KEY_FILE}"

cat > "${LOADER_FILE}" <<'EOF'
#!/usr/bin/env bash
if [[ -f "$HOME/.openfang/secrets/gemini_api_key" ]]; then
  export GEMINI_API_KEY="$(<"$HOME/.openfang/secrets/gemini_api_key")"
fi
EOF
chmod 700 "${LOADER_FILE}"

SNIPPET_START="# >>> openfang gemini env >>>"
SNIPPET_END="# <<< openfang gemini env <<<"
SNIPPET_CONTENT="${SNIPPET_START}
if [[ -f \"\$HOME/.openfang/secrets/load_gemini_env.sh\" ]]; then
  source \"\$HOME/.openfang/secrets/load_gemini_env.sh\"
fi
${SNIPPET_END}"

touch "${ZSHRC_FILE}"
if ! grep -Fq "${SNIPPET_START}" "${ZSHRC_FILE}"; then
  {
    echo
    echo "${SNIPPET_CONTENT}"
  } >> "${ZSHRC_FILE}"
fi

echo "Gemini key saved to: ${KEY_FILE}"
echo "Persistent loader written to: ${LOADER_FILE}"
echo "zsh startup updated: ${ZSHRC_FILE}"
echo "Run this to apply in current shell:"
echo "source ${ZSHRC_FILE}"
